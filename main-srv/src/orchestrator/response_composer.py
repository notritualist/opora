"""
main-srv/src/orchestrator/response_composer.py

Композер генерации финального ответа пользователю (user_answer_generation).

Логика пайплайна:
1. Загрузка сообщения пользователя и ID активного диалога.
2. Интеграция с Preprocessing Pipeline:
   - Чтение retrieval_log_id из сообщения.
   - Загрузка raw_content из memory.retrieval_logs.
   - Инъекция блока "<Контекст из базы знаний>" в системный промпт 
     (после тега </Правила>, чтобы не ломать префикс-кэш).
3. Сборка контекста:
   - Формирование истории диалога (последние N сообщений).
   - Расчет токенов (n_ctx - max_tokens - 10% safety margin).
   - Обрезка истории (удаление самых старых сообщений) при переполнении контекста.
4. Вызов LLM и сохранение результатов:
   - Генерация через ModelService (с поддержкой reasoning_content).
   - Сохранение метрик (ветвление на internal/external провайдеры).
   - Запись ответа в dialogs.row_messages с расчетом answer_latency.
"""

__version__ = "1.2.0"
__description__ = "Module for generating the final response to the user"


import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone
from typing import Optional, Dict, Any

# Локальные импорты проекта
from db_manager.db_manager import load_postgres_config
from model_service.model_service import ModelService
from services.tokens_counter import count_tokens_qwen
from services.service_metrics import (
    mark_task_running,
    create_orchestrator_step,
    complete_step_success,
    complete_step_error,
    complete_task_success,
    complete_task_error,
    save_llm_metrics,
    save_reasoning,
    set_step_reasoning_id,
    save_llm_artifacts,
    save_llm_external_metrics
)
from version import __version__ as agent_version
from services.datetime_context import build_time_block

logger = logging.getLogger(__name__)


# =============================================================================
# === КОНСТАНТЫ (единый источник для max_tokens) ==============================
# =============================================================================
# Математика лимитов:
# - n_ctx сервера = 262144 токенов (из model_config.yaml)
# - DEFAULT_MAX_TOKENS = 65536 (лимит на генерацию: ответ + рассуждение вместе)
# - Доступно под контекст = n_ctx - max_tokens
# - ROUGH_CONTEXT_LIMIT_TOKENS = 90% от доступного (запас 10% на ошибку округления и overhead)
# - HISTORY_MESSAGE_LIMIT - Лимит сообщений истории для контекста
# =============================================================================
DEFAULT_MAX_TOKENS: int = 65536
CONTEXT_SAFETY_MARGIN_PERCENT: float = 0.9  # 10% запас
HISTORY_MESSAGE_LIMIT: int = 15


# =============================================================================
# === ВЫБОР ПРОМПТА (ручное переключение) =====================================
# =============================================================================
# False → agent_core_identity        (internal, локальная llama)
# True  → agent_core_identity_external (external, DashScope API)
# Меняй руками и перезапускай сервис.
# =============================================================================
USE_EXTERNAL_PROMPT: bool = True

PROMPT_NAME_INTERNAL: str = 'agent_core_identity'
PROMPT_NAME_EXTERNAL: str = 'agent_core_identity_external'

METRIC_SOURCE_INTERNAL: str = 'internal'
METRIC_SOURCE_EXTERNAL: str = 'external'

def _build_history_context(db_config: dict, session_id: str, current_message_id: str) -> tuple[list[dict], list[str]]: 
    """
    Собирает историю сообщений сессии для контекста.
    Берёт последние N сообщений в хронологическом порядке.
    Возвращает кортеж: (список сообщений, список их ID).
    """
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, actor_type, row_text
                FROM dialogs.row_messages
                WHERE session_id = %s
                  AND id != %s
                ORDER BY timestamp DESC
                LIMIT %s
            """, (session_id, current_message_id, HISTORY_MESSAGE_LIMIT))

            rows = cur.fetchall()
            # Разворачиваем в хронологическом порядке (от старых к новым
            rows.reverse()

            messages = []
            message_ids = []
            for row in rows:
                # В нашей схеме агент имеет тип 'system'
                role = "assistant" if row["actor_type"] == "system" else "user"
                messages.append({"role": role, "content": row["row_text"]})
                message_ids.append(str(row["id"]))  # ← сохраняем UUID как строку

            return messages, message_ids


def _load_retrieved_knowledge(db_config: dict, retrieval_log_id: Optional[str]) -> Optional[str]:
    """
    Загружает raw_content из memory.retrieval_logs по UUID лога.
    Graceful degradation: если лог отсутствует или пустой — возвращает None.
    
    Args:
        db_config: параметры подключения к PostgreSQL
        retrieval_log_id: UUID записи в memory.retrieval_logs
    
    Returns:
        str | None: raw_content или None
    """
    if not retrieval_log_id:
        return None
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT raw_content, nodes_count, total_tokens, strategy, error_message
                    FROM memory.retrieval_logs
                    WHERE id = %s
                """, (retrieval_log_id,))
                row = cur.fetchone()
                if not row:
                    logger.warning("Retrieval log %s not found", str(retrieval_log_id)[:8])
                    return None
                raw_content, nodes_count, total_tokens, strategy, error_message = row
                if error_message:
                    logger.warning("Retrieval log %s has error: %s", str(retrieval_log_id)[:8], error_message[:200])
                    return None
                if not raw_content or not raw_content.strip():
                    logger.debug("Retrieval log %s has empty raw_content", str(retrieval_log_id)[:8])
                    return None
                logger.info(
                    "Knowledge loaded from retrieval log %s: nodes=%d, tokens=%d, strategy=%s",
                    str(retrieval_log_id)[:8], nodes_count, total_tokens, strategy
                )
                return raw_content
    except Exception as exc:
        logger.warning("Failed to load retrieval log %s: %s", str(retrieval_log_id)[:8] if retrieval_log_id else "None", exc)
        return None


def _inject_knowledge_into_system_prompt(
    system_prompt: str,
    knowledge_content: str
) -> str:
    """
    Инъецирует блок знаний в конец системного промпта.
    
    Блок добавляется после закрывающего тега </Правила> (если он есть),
    либо в самый конец промпта. Это не ломает структуру исходного промпта
    и позволяет LLM чётко отделить правила поведения от фактического контекста.
    
    Args:
        system_prompt: исходный текст системного промпта
        knowledge_content: raw_content из memory.retrieval_logs
    
    Returns:
        str: модифицированный системный промпт с блоком знаний
    """
    knowledge_block = (
        "\n\n<Контекст из базы знаний>\n"
        "Ниже приведены проверенные факты из моей базы знаний, релевантные вопросу собеседника.\n"
        "Используй их как ПРИОРИТЕТНЫЙ источник информации для ответа.\n\n"
        f"{knowledge_content}\n\n"
        "ПРАВИЛА РАБОТЫ С КОНТЕКСТОМ:\n"
        "- Если факт из контекста прямо отвечает на вопрос — опирайся на него.\n"
        "- Если факты противоречат вопросу собеседника — вежливо укажи на расхождение, ссылаясь на контекст.\n"
        "- Если в контексте НЕТ информации для ответа — прямо сообщи: «В моей базе знаний нет данных по этому вопросу».\n"
        "- Не дополняй ответ выдуманными фактами, если их нет в контексте.\n"
        "</Контекст из базы знаний>\n"
    )
    
    # Вставляем блок после закрывающего тега </Правила>, если он есть
    if "</Правила>" in system_prompt:
        return system_prompt.replace("</Правила>", f"</Правила>{knowledge_block}")
    
    # Fallback: добавляем в самый конец
    return system_prompt + knowledge_block


def compose_final_response(task_id: str, input_data: Dict[str, Any]) -> None:
    """
    Генерирует финальный ответ пользователю.
    Логика:
    1. Читает сообщение пользователя и получает ID активного диалога.
    2. Загружает системный промпт и параметры генерации из orchestrator.prompts.
    3. Строит историю диалога (последние N сообщений).
    4. Проверяет лимиты токенов и обрезает историю при необходимости.
    5. Вызывает ModelService.generate().
    6. Сохраняет метрики, артефакты, рассуждения и сам ответ в БД.
    7. Завершает задачу оркестратора.
    """
    
    db_config = load_postgres_config()
    mark_task_running(task_id)
    message_id = input_data.get("message_id")
    
    if not message_id:
        error_msg = f"Missing message_id in task {task_id} input_data"
        logger.error(error_msg)
        complete_task_error(task_id, error_module="response_composer", error_message=error_msg)
        return

    # === 1. Загрузка исходного сообщения пользователя ===
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, row_text, session_id, actor_id, timestamp, retrieval_log_id
                FROM dialogs.row_messages
                WHERE id = %s
            """, (message_id,))
            msg = cur.fetchone()
            if not msg:
                error_msg = f"Message {message_id} not found in dialogs.row_messages"
                logger.error(error_msg)
                complete_task_error(task_id, error_module="response_composer", error_message=error_msg)
                return
            user_content = msg["row_text"]
            session_id = msg["session_id"]
            user_actor_id = msg["actor_id"]
            user_msg_timestamp = msg["timestamp"]
            retrieval_log_id = msg.get("retrieval_log_id")
            logger.debug(
                f"Loaded user message {message_id[:8]} from session {session_id[:8]}, "
                f"retrieval_log={str(retrieval_log_id)[:8] if retrieval_log_id else 'None'}"
            )

    # === 1.1 Получаем ID активного диалога ===
    from dialog_services.dialogue_manager import ensure_active_dialogue

    dialogue_id = ensure_active_dialogue(
        db_config=db_config,
        session_id=session_id,
        actor_id=user_actor_id,
        agent_version=agent_version
    )
    logger.debug(f"Active dialogue ID: {dialogue_id[:8]}")

    # === 1.2 Выбор промпта по константе ===
    prompt_name = PROMPT_NAME_EXTERNAL if USE_EXTERNAL_PROMPT else PROMPT_NAME_INTERNAL
    metric_source = METRIC_SOURCE_EXTERNAL if USE_EXTERNAL_PROMPT else METRIC_SOURCE_INTERNAL
    logger.info("Using prompt '%s' (metric_source=%s, USE_EXTERNAL_PROMPT=%s)",
                prompt_name, metric_source, USE_EXTERNAL_PROMPT)

    # === 2. Загрузка промпта и параметров генерации ===
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, text, params
                FROM orchestrator.prompts
                WHERE name = %s
                AND status IN ('testing'::prompt_status, 'active'::prompt_status)
                ORDER BY created_at DESC
                LIMIT 1
            """, (prompt_name,))
            prompt = cur.fetchone()
            if not prompt:
                error_msg = f"Prompt '{prompt_name}' not found in database"
                logger.error(error_msg)
                complete_task_error(task_id, error_module="response_composer", error_message=error_msg)
                return
            prompt_id = prompt["id"]
            raw_system_prompt = prompt["text"]
            model_params = prompt["params"] or {}
    
    # === 3. Системный промпт с интеграцией знаний из графа ===
    # Инъецируем время в конец. Тег </Правила> остается на месте, 
    # поэтому префикс-кэш статической части промпта не ломается.
    system_prompt = raw_system_prompt + build_time_block("response")

    # Загружаем знания, собранные preprocessing pipeline
    knowledge_content = _load_retrieved_knowledge(db_config, retrieval_log_id)
    knowledge_injected = False
    if knowledge_content:
        system_prompt = _inject_knowledge_into_system_prompt(system_prompt, knowledge_content)
        knowledge_injected = True
        logger.info(
            "Knowledge block injected into system prompt (%d chars)",
            len(knowledge_content)
        )
    else:
        logger.debug("No knowledge available — using base system prompt only")
    
    # === 4. Формирование контекста (только история) ===
    history_messages, history_message_ids = _build_history_context(db_config, session_id, message_id)

    # Формируем messages для LLM API
    messages = [
        {"role": "system", "content": system_prompt},
        *history_messages,
        {"role": "user", "content": user_content}
    ]

    # === 4.1 Расчёт лимитов токенов ===
    model = ModelService()

    model_name = model_params.get("model_name")
    if not model_name:
        error_msg = "Missing 'model_name' in prompt params"
        logger.error(error_msg)
        complete_task_error(task_id, error_module="response_composer", error_message=error_msg)
        return

    # Получаем n_ctx через роутер
    model_info = model.get_model_info(model_name)
    n_ctx = model_info.get("n_ctx", 32768)

    # Получаем max_tokens из промпта или используем дефолт
    max_tokens = model_params.get("max_tokens") or DEFAULT_MAX_TOKENS

    # Максимально допустимое количество токенов под контекст (с запасом)
    available_for_context = int((n_ctx - max_tokens) * CONTEXT_SAFETY_MARGIN_PERCENT)

    # Считаем токены
    system_tokens = count_tokens_qwen(system_prompt)
    history_tokens = sum(count_tokens_qwen(m["content"]) for m in history_messages)
    user_tokens = count_tokens_qwen(user_content)
    total_input_tokens = system_tokens + history_tokens + user_tokens

    # Проверка переполнения
    if total_input_tokens > available_for_context:
        logger.warning(
            "Input exceeds context limit: %d tokens (available: %d, n_ctx=%d, max_tokens=%d)",
            total_input_tokens, available_for_context, n_ctx, max_tokens
        )
        # Обрезаем историю до тех пор, пока не уложимся в лимит
        while history_messages and total_input_tokens > available_for_context:
            removed = history_messages.pop(0)  # Удаляем самое старое сообщение
            removed_tokens = count_tokens_qwen(removed["content"])
            total_input_tokens -= removed_tokens
        
        # Пересобираем messages
        messages = [
            {"role": "system", "content": system_prompt},
            *history_messages,
            {"role": "user", "content": user_content}
        ]
        logger.info("Context truncated: %d messages left, %d tokens", len(history_messages), total_input_tokens)

    # Обновляем total_input_tokens для шага
    total_input_tokens = (
        count_tokens_qwen(system_prompt) +
        sum(count_tokens_qwen(m["content"]) for m in history_messages) +
        count_tokens_qwen(user_content)
    )

    # === 5. Создание шага оркестратора ===
    step_input = {
        "message_id": message_id,
        "prompt_id": prompt_id,
        "token_count": total_input_tokens,
        "history_messages_count": len(history_messages),
        "history_message_ids": history_message_ids,
        "retrieval_log_id": str(retrieval_log_id) if retrieval_log_id else None,
        "knowledge_injected": knowledge_injected,
    }
    step_id = create_orchestrator_step(
        task_id=task_id,
        step_number=1,
        step_type_name="user_answer_generation",
        input_data=step_input
    )

    # === 6. Вызов модели ===
    logger.debug(f"Calling ModelService.generate with {len(messages)} messages")

    # Извлекаем model_name из параметров промпта
    model_name = model_params.get("model_name")
    if not model_name:
        error_msg = "Missing 'model_name' in prompt params"
        logger.error(error_msg)
        complete_step_error(step_id, error_module="response_composer", error_message=error_msg)
        complete_task_error(task_id, error_module="response_composer", error_message=error_msg)
        return

    # Фильтруем остальные параметры (без model_name)
    safe_params = {
        k: v for k, v in model_params.items()
        if k in [
            "temperature", "top_p", "top_k", "min_p", "max_tokens",
            "presence_penalty", "repetition_penalty", "stop", "chat_template_kwargs"
        ]
    }
    
    model = ModelService()

    try:
        result = model.generate(
            messages=messages,
            model_name=model_name,  # ← ЯВНО ПЕРЕДАЁМ
            **safe_params
        )
    except Exception as e:
        logger.exception(f"ModelService generation failed: {e}")
        complete_step_error(step_id, error_module="ModelService", error_message=str(e))
        complete_task_error(task_id, error_module="response_composer", error_message=str(e))
        return

    if not result.get("success"):
        error_msg = result.get("error", "Unknown model generation error")
        logger.error(f"Model returned failure: {error_msg}")
        complete_step_error(step_id, error_module="response_composer", error_message=error_msg)
        complete_task_error(task_id, error_module="response_composer", error_message=error_msg)
        return

    # === 7. Обработка ответа модели ===
    if not result.get("success"):
        error = result.get("error", "Unknown model generation error")
        logger.error(f"Model generation failed: {error}")
        complete_step_error(step_id, error_module="ModelService", error_message=error)
        complete_task_error(task_id, error_module="response_composer", error_message=error)
        return
    
    # === 8. Извлечение ответа и рассуждения ===
    # llama-server с Qwen3.5 возвращает reasoning_content как отдельное поле
    clean_response: str = result.get("response", "") or result.get("content", "")
    reasoning_text: Optional[str] = result.get("reasoning_content") or result.get("reasoning")

    if not clean_response:
        clean_response = "[Empty response]"
        logger.warning("Generated response is empty")

    # === 9. Сохранение рассуждений (если есть) ===
    reasoning_id = None
    if reasoning_text and reasoning_text.strip():
        # Рассуждение штампуется тем же PHS-срезом, что и ответ
        reasoning_id = save_reasoning(
            orchestrator_step_id=step_id,
            content=reasoning_text.strip(),
            content_type="messages"
        )
        if reasoning_id:
            set_step_reasoning_id(step_id, reasoning_id)
            logger.debug(f"Reasoning saved: {reasoning_id[:8]}")

    # === 10. Запись метрик LLM (ветвление по metric_source) ===
    metrics = result.get("metrics", {})
    timings = metrics.get("timings", {})
    usage = metrics.get("usage", {})
    provider_name = model.get_model_info(model_name).get("provider", "local_llama")

    llm_metric_id = None
    llm_metric_external_id = None

    if metric_source == METRIC_SOURCE_EXTERNAL:
        llm_metric_external_id = save_llm_external_metrics(
            orchestrator_step_id=step_id,
            prompt_id=prompt_id,
            provider=provider_name,
            model=metrics.get("model", model_name),
            param=model_params,
            prompt_tokens=usage.get("prompt_tokens", 0),
            completion_tokens=usage.get("completion_tokens", 0),
            total_tokens=usage.get("total_tokens", 0),
            host_nctx=metrics.get("host_nctx", 0),
            prompt_ms=timings.get("prompt_ms", 0.0),
            prompt_per_token_ms=timings.get("prompt_per_token_ms", 0.0),
            prompt_per_second=timings.get("prompt_per_second", 0.0),
            predicted_per_second=timings.get("predicted_per_second", 0.0),
            resp_time=timings.get("predicted_ms", 0.0) / 1000,
            net_latency=0.0,
            full_time=0.0,
            error_status=False
        )
    else:
        llm_metric_id = save_llm_metrics(
            orchestrator_step_id=step_id,
            prompt_id=prompt_id,
            host="main-srv",
            model=metrics.get("model", "unknown"),
            param=model_params,
            cache_n=timings.get("cache_n", 0),
            prompt_tokens=usage.get("prompt_tokens", 0),
            completion_tokens=usage.get("completion_tokens", 0),
            total_tokens=usage.get("total_tokens", 0),
            host_nctx=metrics.get("host_nctx", 0),
            prompt_ms=timings.get("prompt_ms", 0.0),
            prompt_per_token_ms=timings.get("prompt_per_token_ms", 0.0),
            prompt_per_second=timings.get("prompt_per_second", 0.0),
            predicted_per_second=timings.get("predicted_per_second", 0.0),
            resp_time=timings.get("predicted_ms", 0.0) / 1000,
            net_latency=0.0,
            full_time=0.0,
            error_status=False
        )

    # === 10.1 Сохранение артефактов (полный промпт + ответ + параметры) ===
    save_llm_artifacts(
        llm_metric_id=llm_metric_id,
        llm_metric_external_id=llm_metric_external_id,
        metric_source=metric_source,
        orchestrator_step_id=step_id,
        messages=messages,
        raw_response=clean_response,
        final_params=safe_params
    )

    # === 11. Расчёт задержки ответа (answer_latency) ===
    answer_timestamp = datetime.now(timezone.utc)
    if user_msg_timestamp.tzinfo is None:
        user_msg_timestamp = user_msg_timestamp.replace(tzinfo=timezone.utc)
    answer_latency = (answer_timestamp - user_msg_timestamp).total_seconds()

    # === 12. Сохранение ответа в dialogs.row_messages ===
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM users.actors WHERE type = 'system'::actor_type LIMIT 1")
                sys_actor = cur.fetchone()
                if not sys_actor:
                    raise RuntimeError("System actor not found in users.actors")
                system_actor_id = sys_actor[0]

                cur.execute("""
                    INSERT INTO dialogs.row_messages (
                        parent_message_id,
                        actor_id,
                        actor_type,
                        responded_by_actor_id,
                        session_id,
                        dialogue_id,
                        row_text,
                        answer_latency,
                        orchestrator_step_id,
                        agent_version,
                        timestamp
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    message_id,
                    system_actor_id,
                    "system",
                    user_actor_id,
                    session_id,
                    dialogue_id,
                    clean_response,
                    answer_latency,
                    step_id,
                    agent_version,
                    answer_timestamp
                ))
                response_id = str(cur.fetchone()[0])
                conn.commit()
                logger.info(f"Agent response saved: {response_id[:8]}, latency={answer_latency:.2f}s")

    except Exception as e:
        logger.error(f"Failed to save response to DB: {e}", exc_info=True)
        complete_step_error(step_id, error_module="response_composer", error_message=str(e))
        complete_task_error(task_id, error_module="response_composer", error_message=str(e))
        return

    # === 13. Завершение шага и задачи ===
    step_output = {
        "response_message_id": response_id,
        "llm_metric_id": llm_metric_id,
        "llm_metric_external_id": llm_metric_external_id,
        "metric_source": metric_source,
        "reasoning_id": reasoning_id
    }

    complete_step_success(
        step_id,
        output_data=step_output,
        llm_metric_id=llm_metric_id,                # ← для internal (local_llama)
        llm_metric_external_id=llm_metric_external_id,  # ← для external (dashscope)
        metric_source=metric_source,                # ← 'internal' или 'external'
    )

    complete_task_success(task_id, output_data=step_output)
    logger.info(f"Task {task_id[:8]} and step {step_id[:8]} completed successfully")