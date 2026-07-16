"""
/main-srv/src/preprocessing/decomposition_composer.py
Шаг query_decomposition: LLM-разбиение сложного запроса на независимые подвопросы.

Логика работы:
1. Вызывается условно из pipeline.py — только если длина запроса в токенах 
   превышает MAX_QUERY_TOKENS_FOR_SINGLE_VECTOR (150 токенов).
2. Загружает активный промпт 'query_decomposer' из БД.
3. Передает текст вопроса в LLM, ожидая на выходе JSON-массив строк.
4. Парсит и валидирует ответ (фильтрует пустые строки, ограничивает до 5 подвопросов).
5. Если LLM вернула 0 или 1 подвопрос, декомпозиция считается неприменимой (single-vector mode).
6. Сохраняет метрики LLM, артефакты и результат декомпозиции в БД 
   (в dialogs.row_messages.routing_context для трассировки).

Результат:
    Словарь с массивом строк-подвопросов (sub_queries) или None, 
    если декомпозиция не нужна или произошла ошибка.
    Эти подвопросы затем векторизуются отдельно в retrieval_composer.
"""
version = "1.1.0"
description = "LLM query decomposition into sub-queries"

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


PROMPT_NAME = "query_decomposer"


def _load_active_prompt(cur, prompt_name: str) -> Dict[str, Any]:
    """Загружает активный (или testing) промпт из БД."""
    cur.execute("""
        SELECT id, version, text, params
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
    row = cur.fetchone()
    if not row:
        raise RuntimeError(f"No active/testing prompt '{prompt_name}' found")
    return dict(row)


def _call_llm(
    prompt_row: Dict[str, Any],
    variables: Dict[str, Any],
    orchestrator_step_id: str,
) -> Dict[str, Any]:
    """
    Вызов LLM с промптом из БД.
    Возвращает {raw_response, reasoning_content, llm_metric_id, messages, final_params, prompt_version}
    """
    db_config = load_postgres_config()
    start_time = time.time()

    prompt_id = str(prompt_row["id"])
    prompt_version = prompt_row["version"]
    prompt_text = prompt_row["text"]
    prompt_params = prompt_row["params"] or {}

    # Подстановка переменных
    rendered_text = prompt_text
    for key, value in variables.items():
        rendered_text = rendered_text.replace("{{" + key + "}}", str(value))
    messages = [{"role": "user", "content": rendered_text}]

    # Параметры генерации
    model_name = prompt_params.get("model_name", "Qwen3.5-9B-Q4_K_M.gguf")
    temperature = float(prompt_params.get("temperature", 0.7))
    top_p = float(prompt_params.get("top_p", 0.9))
    top_k = int(prompt_params.get("top_k", 40))
    min_p = float(prompt_params.get("min_p", 0.0))
    max_tokens = int(prompt_params.get("max_tokens", 4096))
    presence_penalty = float(prompt_params.get("presence_penalty", 0.0))
    stop = prompt_params.get("stop", [])
    chat_kwargs = prompt_params.get("chat_template_kwargs", {})

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

    full_time = time.time() - start_time

    success = response.get("success", False)
    raw_response = response.get("response", "")
    reasoning_content = response.get("reasoning_content", "") or ""
    metrics = response.get("metrics", {}) or {}
    error_msg = response.get("error")

    if not success and error_msg:
        raise RuntimeError(f"LLM generation failed: {error_msg}")

    # Метрики
    llm_metric_id = save_llm_metrics(
        orchestrator_step_id=orchestrator_step_id,
        prompt_id=prompt_id,
        host=metrics.get("host", "local"),
        model=model_name,
        param=prompt_params,
        cache_n=int(metrics.get("cache_n", 0)),
        prompt_tokens=int(metrics.get("prompt_tokens", 0)),
        completion_tokens=int(metrics.get("completion_tokens", 0)),
        total_tokens=int(metrics.get("total_tokens", 0)),
        host_nctx=int(metrics.get("host_nctx", 0)),
        prompt_ms=float(metrics.get("prompt_ms", 0.0)),
        prompt_per_token_ms=float(metrics.get("prompt_per_token_ms", 0.0)),
        prompt_per_second=float(metrics.get("prompt_per_second", 0.0)),
        predicted_per_second=float(metrics.get("predicted_per_second", 0.0)),
        resp_time=float(metrics.get("resp_time", full_time)),
        net_latency=float(metrics.get("net_latency", 0.0)),
        full_time=full_time,
        error_status=not success,
        error_message=error_msg,
    )

    if reasoning_content.strip():
        save_reasoning(
            orchestrator_step_id=orchestrator_step_id,
            content=reasoning_content,
            content_type="messages",
        )

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
        "prompt_version": prompt_version,
    }


def _parse_sub_queries(raw_response: str) -> List[str]:
    """
    Парсит JSON-массив подвопросов из ответа LLM.
    Возвращает список строк или пустой список при ошибке парсинга.
    """
    try:
        cleaned = raw_response.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
        parsed = json.loads(cleaned)
        if isinstance(parsed, list):
            # Только непустые строки
            result = [str(q).strip() for q in parsed if isinstance(q, str) and q.strip()]
            # Ограничение: максимум 5 подвопросов
            return result[:5]
    except (json.JSONDecodeError, TypeError, ValueError) as e:
        logger.warning("Sub-queries JSON parse failed: %s. Raw: %s", e, raw_response[:200])
    return []


def compose_query_decomposition(
    task_id: str,
    message_id: str,
    question_text: str,
    step_type_name: str = "query_decomposition",
) -> Dict[str, Any]:
    """
    Выполняет шаг декомпозиции запроса.
    
    Args:
        task_id: UUID задачи question_preprocessing
        message_id: UUID сообщения
        question_text: Исходный текст вопроса (уже известен из pipeline)
        step_type_name: Имя типа шага оркестратора
    
    Returns:
        dict: {
            "sub_queries": [str] | None,  # None если не применимо
            "applied": bool,
            "llm_metric_id": str | None,
            "prompt_version": str | None,
            "raw_response": str | None,
        }
    """
    db_config = load_postgres_config()

    step_id = create_orchestrator_step(
        task_id=task_id,
        step_number=2,  # между routing (1) и retrieval (3)
        step_type_name=step_type_name,
        input_data={"message_id": message_id, "question_length": len(question_text)}
    )

    try:
        # Загрузка промпта
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                prompt_row = _load_active_prompt(cur, PROMPT_NAME)

        # Вызов LLM
        llm_response = _call_llm(
            prompt_row=prompt_row,
            variables={"question": question_text},
            orchestrator_step_id=step_id,
        )

        # Парсинг
        sub_queries = _parse_sub_queries(llm_response["raw_response"])

        # Если декомпозиция вернула 0-1 элементов — считаем её неприменимой
        if len(sub_queries) <= 1:
            logger.info(
                "Decomposition returned %d sub-queries — treating as single query",
                len(sub_queries)
            )
            sub_queries = None
            applied = False
        else:
            applied = True
            logger.info("Decomposition applied: %d sub-queries", len(sub_queries))

        # Сохраняем результат в row_messages для трассировки
        decomposition_result = {
            "sub_queries": sub_queries,
            "applied": applied,
            "llm_metric_id": llm_response["llm_metric_id"],
            "prompt_version": llm_response["prompt_version"],
            "raw_response": llm_response["raw_response"],
        }

        with psycopg2.connect(**db_config) as conn2:
            with conn2.cursor() as cur2:
                cur2.execute("""
                    UPDATE dialogs.row_messages
                    SET routing_context = COALESCE(routing_context, '{}'::jsonb)
                        || %s::jsonb
                    WHERE id = %s
                """, (Json({"decomposition": decomposition_result}), message_id))
            conn2.commit()

        complete_step_success(
            step_id=step_id,
            output_data=decomposition_result,
            llm_metric_id=llm_response["llm_metric_id"],
        )

        return decomposition_result

    except Exception as exc:
        logger.exception("Decomposition step failed: %s", exc)
        complete_step_error(
            step_id=step_id,
            error_module="preprocessing.decomposition_composer",
            error_message=str(exc),
        )
        # При ошибке декомпозиции — возвращаем None (не крашим пайплайн)
        return {
            "sub_queries": None,
            "applied": False,
            "llm_metric_id": None,
            "prompt_version": None,
            "raw_response": None,
            "error": str(exc),
        }