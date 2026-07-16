"""
/main-srv/src/preprocessing/routing_composer.py

Шаг question_routing: быстрый LLM-предразбор вопроса для определения доменов.

Логика работы:
1. Загрузка справочника: читает активные домены из memory.knowledge_domains (code, name, description).
2. Формирование промпта: передает в LLM список доменов с описаниями для лучшего понимания контекста.
3. Вызов LLM: использует промпт 'question_domain_router' из orchestrator.prompts.
4. Парсинг и Детерминированная валидация (Защита от галлюцинаций):
   - Парсит JSON-ответ модели.
   - Жестко фильтрует список доменов: оставляет только те code, что физически есть в БД.
   - Нормализует confidence в диапазон [0.0, 1.0].
   - Принудительно обнуляет topics (архитектурное решение: роутинг только по доменам).
5. Сохранение: обновляет dialogs.row_messages.routing_context и логирует метрики LLM.

Результат:
    Словарь с валидированным списком доменов (domains), уровнем уверенности (confidence) 
    и служебными полями для трассировки.
"""
version = "1.1.0"
description = "LLM-based question routing с пост-валидацией входимости тем в домены"

import json
import time
import logging
import psycopg2
from typing import Dict, Any, List
from psycopg2.extras import RealDictCursor, Json 

from db_manager.db_manager import load_postgres_config
from services.service_metrics import (
    create_orchestrator_step,
    complete_step_success,
    complete_step_error,
    save_llm_metrics,
    save_llm_artifacts,
    save_reasoning,
)
from model_service.model_service import ModelService

logger = logging.getLogger(__name__)


# =============================================================================
# === ЗАГРУЗКА СПРАВОЧНИКОВ ===================================================
# =============================================================================

def _load_active_domains(cur) -> List[Dict[str, str]]:
    """Загружает активные домены: code | name | description."""
    cur.execute("""
        SELECT code, name, description
        FROM memory.knowledge_domains
        WHERE is_active = TRUE
        ORDER BY code
    """)
    return [dict(row) for row in cur.fetchall()]


# =============================================================================
# === ФОРМАТИРОВАНИЕ ДЛЯ ПРОМПТА ==============================================
# =============================================================================

def _format_domains_for_prompt(domains: List[Dict]) -> str:
    """
    Формат: code | название | описание
    Совпадает с тем, что ожидает промпт question_domain_router.
    """
    if not domains:
        return "(нет активных доменов)"
    lines = []
    for d in domains:
        desc = (d.get("description") or "").strip()
        lines.append(f"- {d['code']} | {d['name']} | {desc}")
    return "\n".join(lines)


# =============================================================================
# === ВАЛИДАЦИЯ ОТВЕТА LLM ====================================================
# =============================================================================

def _validate_routing_response(
    raw: Dict[str, Any],
    valid_domain_codes: List[str]  # <-- Убрали valid_topic_ids
) -> Dict[str, Any]:
    """
    Валидирует и очищает ответ LLM.
    - Оставляет только те code, что существуют в справочниках доменов.
    - Нормализует confidence в диапазон [0.0, 1.0].
    - Принудительно обнуляет topics, так как роутинг теперь только по доменам.
    """
    domains = raw.get("domains", [])
    confidence = raw.get("confidence", 0.5)

    if not isinstance(domains, list):
        domains = []

    # Оставляем только существующие домены
    domains = [d for d in domains if d in valid_domain_codes]

    try:
        confidence = float(confidence)
        confidence = max(0.0, min(1.0, confidence))
    except (TypeError, ValueError):
        confidence = 0.5

    return {
        "domains": domains,
        "topics": [],  # <-- Жестко задаем пустой список, LLM их больше не присылает
        "confidence": confidence,
    }

# =============================================================================
# === ЛОКАЛЬНАЯ ОБЁРТКА ВЫЗОВА LLM С ПРОМПТОМ ИЗ БД ==========================
# =============================================================================

def _call_llm_with_prompt(
    prompt_name: str,
    variables: Dict[str, Any],
    orchestrator_step_id: str,
) -> Dict[str, Any]:
    """
    Локальная обёртка вызова LLM с промптом из БД.
    
    Логика:
    1. Загружает промпт (text + params) из orchestrator.prompts по name+version.
    - Приоритет 1: status='active' (последний по created_at)
    - Приоритет 2: status='testing' (fallback)
    - Если ни одного нет → RuntimeError
    2. Подставляет переменные в текст промпта (простая замена {{key}}).
    3. Формирует messages = [{"role": "user", "content": rendered_text}].
    4. Вызывает ModelService().generate() с параметрами из prompt.params.
    5. Сохраняет метрики в metrics.llm_internal через save_llm_metrics.
    6. Сохраняет артефакты в metrics.llm_artifacts через save_llm_artifacts.
    7. Сохраняет reasoning (если есть) через save_reasoning.
    
    Args:
        prompt_name: Имя промпта в orchestrator.prompts
        prompt_version: Версия промпта (SemVer)
        variables: Словарь переменных для подстановки в текст
        orchestrator_step_id: UUID шага оркестратора для трассировки
    
    Returns:
        dict: {
            "raw_response": str,          # сырой ответ модели (content)
            "reasoning_content": str,      # блок /think (если был)
            "llm_metric_id": str,          # UUID записи в metrics.llm_internal
            "messages": list,              # финальный массив messages
            "final_params": dict,          # параметры генерации
        }
    """
    db_config = load_postgres_config()
    start_time = time.time()
    
    # === 1. Загружаем АКТИВНЫЙ промпт из БД (версия берётся оттуда!) ===
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, version, text, params, destination_id
                FROM orchestrator.prompts
                WHERE name = %s
                  AND status IN ('active'::public.prompt_status, 'testing'::public.prompt_status)
                ORDER BY 
                    CASE status 
                        WHEN 'active'::public.prompt_status THEN 1 
                        ELSE 2 
                    END,
                    created_at DESC
                LIMIT 1
            """, (prompt_name,))
            prompt_row = cur.fetchone()
            if not prompt_row:
                raise RuntimeError(
                    f"No active/testing prompt '{prompt_name}' found in orchestrator.prompts"
                )
            prompt_id = str(prompt_row["id"])
            prompt_version = prompt_row["version"]  # ← БЕРЁМ ИЗ БД
            prompt_text = prompt_row["text"]
            prompt_params = prompt_row["params"] or {}
    
    logger.debug(
        "Using prompt '%s' v%s (id=%s) for step %s",
        prompt_name, prompt_version, prompt_id[:8], orchestrator_step_id[:8]
    )
    
    # === 2. Подстановка переменных в текст промпта ===
    rendered_text = prompt_text
    for key, value in variables.items():
        rendered_text = rendered_text.replace("{{" + key + "}}", str(value))
    
    messages = [{"role": "user", "content": rendered_text}]
    
    # === 3. Извлекаем параметры генерации ===
    model_name = prompt_params.get("model_name", "Qwen3.5-9B-Q4_K_M.gguf")
    temperature = float(prompt_params.get("temperature", 0.7))
    top_p = float(prompt_params.get("top_p", 0.9))
    top_k = int(prompt_params.get("top_k", 40))
    min_p = float(prompt_params.get("min_p", 0.0))
    max_tokens = int(prompt_params.get("max_tokens", 4096))
    presence_penalty = float(prompt_params.get("presence_penalty", 0.0))
    stop = prompt_params.get("stop", [])
    chat_kwargs = prompt_params.get("chat_template_kwargs", {})
    
    # === 4. Вызов LLM через ModelService ===
    try:
        service = ModelService()
        response = service.generate(
            messages=messages,
            model_name=model_name,
            temperature=temperature,
            top_p=top_p,
            top_k=top_k,
            min_p=min_p,
            max_tokens=max_tokens,
            presence_penalty=presence_penalty,
            stop=stop,
            chat_template_kwargs=chat_kwargs,
        )
    except Exception as exc:
        logger.error("ModelService.generate failed: %s", exc, exc_info=True)
        raise
    
    full_time = time.time() - start_time
    
    # === 5. Извлекаем поля ответа провайдера ===
    success = response.get("success", False)
    raw_response = response.get("response", "")
    reasoning_content = response.get("reasoning_content", "") or ""
    metrics = response.get("metrics", {}) or {}
    error_msg = response.get("error")
    
    if not success and error_msg:
        raise RuntimeError(f"LLM generation failed: {error_msg}")
    
    # === 6. Сохраняем метрики в metrics.llm_internal ===
    # Локальный провайдер возвращает вложенную структуру: metrics.usage + metrics.timings
    metrics = response.get("metrics", {}) or {}
    usage = metrics.get("usage", {})
    timings = metrics.get("timings", {})

    llm_metric_id = save_llm_metrics(
        orchestrator_step_id=orchestrator_step_id,
        prompt_id=prompt_id,
        host=metrics.get("host", "main-srv"),
        model=model_name,
        param=prompt_params,
        cache_n=int(timings.get("cache_n", 0)),
        prompt_tokens=int(usage.get("prompt_tokens", 0)),
        completion_tokens=int(usage.get("completion_tokens", 0)),
        total_tokens=int(usage.get("total_tokens", 0)),
        host_nctx=int(metrics.get("host_nctx", 0)),
        prompt_ms=float(timings.get("prompt_ms", 0.0)),
        prompt_per_token_ms=float(timings.get("prompt_per_token_ms", 0.0)),
        prompt_per_second=float(timings.get("prompt_per_second", 0.0)),
        predicted_per_second=float(timings.get("predicted_per_second", 0.0)),
        resp_time=float(timings.get("predicted_ms", full_time * 1000)) / 1000,
        net_latency=float(timings.get("net_latency", 0.0)),
        full_time=full_time,
        error_status=not success,
        error_message=error_msg,
    )
    
    # === 7. Сохраняем reasoning (если есть) ===
    if reasoning_content.strip():
        save_reasoning(
            orchestrator_step_id=orchestrator_step_id,
            content=reasoning_content,
            content_type="messages",
        )
    
    # === 8. Сохраняем артефакты ===
    save_llm_artifacts(
        llm_metric_id=llm_metric_id,
        orchestrator_step_id=orchestrator_step_id,
        messages=messages,
        raw_response=raw_response,
        final_params=prompt_params,
    )
    
    return {
        "raw_response": raw_response,
        "reasoning_content": reasoning_content,
        "llm_metric_id": llm_metric_id,
        "messages": messages,
        "final_params": prompt_params,
        "prompt_version": prompt_version,  # ← фактическая версия из БД
    }

# =============================================================================
# === ГЛАВНАЯ ФУНКЦИЯ ШАГА ====================================================
# =============================================================================

def compose_question_routing(
    task_id: str,
    message_id: str,
    step_type_name: str,
    prompt_name: str,
) -> Dict[str, Any]:
    """
    Выполняет шаг question_routing.
    
    Поток:
    1. Загрузка справочников + карты входимости тем
    2. Вызов LLM с полным промптом (с описаниями)
    3. Парсинг JSON-ответа
    4. Валидация (только существование в справочниках)
    5. Детерминированная фильтрация тем по входимости в домены
    6. Сохранение результата в row_messages.routing_context
    
    Args:
        task_id: UUID задачи question_preprocessing
        message_id: UUID сообщения пользователя
        
    Returns:
        dict: {
            "domains": [...], "confidence": float,
            "raw_response": str,
            "llm_metric_id": str,
            "prompt_name": str, "prompt_version": str
        }
    """
    db_config = load_postgres_config()
    
    # === 1. Создаём шаг оркестратора ===
    step_id = create_orchestrator_step(
        task_id=task_id,
        step_number=1,
        step_type_name=step_type_name,
        input_data={"message_id": message_id}
    )

    conn = None
    try:
        # === 2. Загружаем данные ===
        conn = psycopg2.connect(**db_config)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Текст вопроса
            cur.execute(
                "SELECT row_text FROM dialogs.row_messages WHERE id = %s",
                (message_id,)
            )
            msg_row = cur.fetchone()
            if not msg_row:
                raise RuntimeError(f"Message {message_id} not found")
            question_text = msg_row["row_text"]

            # Справочники
            domains = _load_active_domains(cur)
            valid_domain_codes = [d["code"] for d in domains]
            
        # === 3. Вызов LLM ===
        variables = {
            "question": question_text,
            "domains_list": _format_domains_for_prompt(domains)
        }

        llm_response = _call_llm_with_prompt(
            prompt_name=prompt_name,
            variables=variables,
            orchestrator_step_id=step_id,
        )

        raw_response = llm_response.get("raw_response", "")
        llm_metric_id = llm_response.get("llm_metric_id")
        prompt_version = llm_response.get("prompt_version", "unknown")

        # === 4. Парсинг JSON ===
        try:
            cleaned = raw_response.strip()
            if cleaned.startswith("```"):
                cleaned = cleaned.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
            parsed = json.loads(cleaned)
        except json.JSONDecodeError as e:
            logger.warning("Routing JSON parse failed: %s. Raw: %s", e, raw_response[:300])
            parsed = {"domains": [], "confidence": 0.0}


        # === 5. Валидация ===
        validated = _validate_routing_response(parsed, valid_domain_codes)

        # === 6. Фильтрация тем больше не нужна, обнуляем ===
        dropped_topics = []
        validated["topics"] = [] 

         # === 7. Формируем итоговый routing_context ===
        routing_context = {
            **validated,
            "dropped_topics": dropped_topics,
            "raw_response": raw_response,
            "llm_metric_id": llm_metric_id,
            "prompt_name": prompt_name,
            "prompt_version": prompt_version,
        }
        

        # === 8. Обновляем row_messages.routing_context ===
        with psycopg2.connect(**db_config) as conn2:
            with conn2.cursor() as cur2:
                cur2.execute("""
                    UPDATE dialogs.row_messages
                    SET routing_context = %s
                    WHERE id = %s
                """, (Json(routing_context), message_id))
            conn2.commit()

        # === 9. Завершаем шаг успешно ===
        complete_step_success(
            step_id=step_id,
            output_data=routing_context,
            llm_metric_id=llm_metric_id,
        )

        logger.info(
            "Routing completed: domains=%s, confidence=%.2f",
            validated["domains"],
            validated["confidence"],
        )

        return routing_context

    except Exception as exc:
        logger.exception("Routing step failed: %s", exc)
        complete_step_error(
            step_id=step_id,
            error_module="preprocessing.routing_composer",
            error_message=str(exc),
        )
        raise
    finally:
        if conn:
            conn.close()