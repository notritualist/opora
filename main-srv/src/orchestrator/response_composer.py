"""
main-srv/src/orchestrator/response_composer.py

Модуль генерации финального ответа пользователю.
Логика:
1. Получение сообщения пользователя из БД.
2. Получение системного промпта и параметров генерации из orchestrator.prompts.
3. Сборка истории диалога (последние N сообщений согласно HISTORY_MESSAGE_LIMIT).
4. Расчёт лимитов токенов и обрезка истории при переполнении контекста.
5. Вызов ModelService.generate().
6. Сохранение полных артефактов в metrics.llm_artifacts.
7. Сохранение ответа в dialogs.row_messages.
8. Завершение задачи и шага оркестратора.
"""

__version__ = "1.1.0"
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
)
from version import __version__ as agent_version

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
                SELECT id, row_text, session_id, actor_id, timestamp
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

            logger.debug(f"Loaded user message {message_id[:8]} from session {session_id[:8]}")

    # === 1.1 Получаем ID активного диалога ===
    from dialog_services.dialogue_manager import ensure_active_dialogue

    dialogue_id = ensure_active_dialogue(
        db_config=db_config,
        session_id=session_id,
        actor_id=user_actor_id,
        agent_version=agent_version
    )
    logger.debug(f"Active dialogue ID: {dialogue_id[:8]}")

    # === 2. Загрузка промпта и параметров генерации ===
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Ищем актуальный системный промпт (testing или active)
            cur.execute("""
                SELECT id, text, params
                FROM orchestrator.prompts
                WHERE name = 'agent_core_identity'
                  AND status IN ('testing'::prompt_status, 'active'::prompt_status)
                ORDER BY created_at DESC
                LIMIT 1
            """)
            prompt = cur.fetchone()
            if not prompt:
                error_msg = "Prompt 'agent_core_identity' not found in database"
                logger.error(error_msg)
                complete_task_error(task_id, error_module="response_composer", error_message=error_msg)
                return

            prompt_id = prompt["id"]
            raw_system_prompt = prompt["text"]
            model_params = prompt["params"] or {}
    
    # === 3. Системный промпт (в V001 без плейсхолдеров) ===
    system_prompt = raw_system_prompt
    
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

    # === 10. Запись метрик LLM ===
    metrics = result.get("metrics", {})
    timings = metrics.get("timings", {})
    usage = metrics.get("usage", {})

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

    # === 13. Привязка метрик к шагу оркестратора ===
    # llm_metric_id хранится в orchestrator.orchestrator_steps, а не в сообщениях
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE orchestrator.orchestrator_steps SET llm_metric_id = %s WHERE id = %s", (llm_metric_id, step_id))
                conn.commit()
    except Exception as e:
        logger.warning(f"Failed to link llm_metric_id to step: {e}")

    # === 14. Завершение шага и задачи ===
    step_output = {
        "response_message_id": response_id,
        "llm_metric_id": llm_metric_id,
        "reasoning_id": reasoning_id
    }
    complete_step_success(step_id, output_data=step_output)
    complete_task_success(task_id, output_data=step_output)
    logger.info(f"Task {task_id[:8]} and step {step_id[:8]} completed successfully")