"""
main-srv/src/preprocessing/pipeline.py
Оркестратор этапа преданализа (question_preprocessing).

Логика работы (последовательность шагов):
1. question_routing (routing_composer) 
   → LLM-предразбор вопроса для определения целевых доменов базы знаний.
   → Fallback: при падении шага, retrieval выполняется без фильтрации доменов.
   
2. query_decomposition (decomposition_composer) — УСЛОВНЫЙ ШАГ
   → Подсчет токенов в вопросе. Если > MAX_QUERY_TOKENS_FOR_SINGLE_VECTOR (150), 
     запускается LLM-декомпозиция на подвопросы.
   → Fallback: при ошибке подсчета или декомпозиции используется исходный запрос.
   
3. knowledge_retrieval (retrieval_composer)
   → Векторный поиск, расширение по графу, ранжирование и сборка контекста.
   → Fallback: при падении шага задача все равно считается выполненной, 
     но генерация ответа пойдет с пустым retrieved_context.

Архитектура сохранения результатов:
- dialogs.row_messages.routing_context (JSONB — результаты роутинга и декомпозиции)
- dialogs.row_messages.retrieved_context (JSONB — ID узлов и ребер контекста)
- memory.retrieval_logs (полная трассировка поиска)
"""
version = "1.1.0"
description = "Preprocessing pipeline orchestrator"

import logging
from services.service_metrics import complete_task_success, complete_task_error
from services.tokens_counter import count_tokens_qwen

logger = logging.getLogger(__name__)


# Порог токенов, выше которого запускается декомпозиция запроса.
# Запросы короче этого порога ищутся одним вектором (быстро).
# Запросы длиннее — разбиваются LLM на подвопросы для повышения качества.
MAX_QUERY_TOKENS_FOR_SINGLE_VECTOR: int = 150


def run_question_preprocessing(task_id: str, input_data: dict) -> None:
    """
    Запускает оба шага преданализа последовательно.
    Если шаг 1 (routing) падает — шаг 2 (retrieval) выполняется в режиме fallback.
    Если оба падают — задача завершается с ошибкой.
    """
    from preprocessing.routing_composer import compose_question_routing
    from preprocessing.retrieval_composer import compose_knowledge_retrieval

    message_id = input_data.get("message_id")
    if not message_id:
        raise ValueError("input_data must contain 'message_id'")

    logger.info("Preprocessing started for message %s", message_id[:8])

    # === Шаг 1: Роутинг ===
    try:
        routing_result = compose_question_routing(
            task_id=task_id,
            message_id=message_id,
            step_type_name="question_routing",
            prompt_name="question_domain_router",
        )
        logger.info(
            "Routing completed: domains=%s, topics=%d",
            routing_result.get("domains", []),
            len(routing_result.get("topics", []))
        )
    except Exception as exc:
        logger.warning("Routing failed, will use fallback retrieval: %s", exc)
        routing_result = {"domains": [], "topics": [], "fallback": True, "error": str(exc)}

    # === Шаг 2 (условный): Декомпозиция запроса ===
    sub_queries = None
    try:
        # Загружаем текст вопроса для подсчёта токенов
        from db_manager.db_manager import load_postgres_config as _load_pg
        import psycopg2 as _psycopg2
        _db_cfg = _load_pg()
        with _psycopg2.connect(**_db_cfg) as _conn:
            with _conn.cursor() as _cur:
                _cur.execute(
                    "SELECT row_text FROM dialogs.row_messages WHERE id = %s",
                    (message_id,)
                )
                _row = _cur.fetchone()
                question_text = _row[0] if _row else ""

        token_count = count_tokens_qwen(question_text)
        logger.info(
            "Query length: %d tokens (threshold: %d)",
            token_count, MAX_QUERY_TOKENS_FOR_SINGLE_VECTOR
        )

        if token_count > MAX_QUERY_TOKENS_FOR_SINGLE_VECTOR:
            from preprocessing.decomposition_composer import compose_query_decomposition
            decomp_result = compose_query_decomposition(
                task_id=task_id,
                message_id=message_id,
                question_text=question_text,
            )
            sub_queries = decomp_result.get("sub_queries")
        else:
            logger.debug("Query short enough — skipping decomposition")
    except Exception as exc:
        logger.warning("Decomposition check/execution failed, proceeding with single vector: %s", exc)
        sub_queries = None
    
    # === Шаг 3: Выборка знаний ===
    try:
        retrieval_result = compose_knowledge_retrieval(
            task_id=task_id,
            message_id=message_id,
            routing_context=routing_result,
            sub_queries=sub_queries,
        )
        logger.info(
            "Retrieval completed: nodes=%d, tokens=%d, trimmed=%s",
            retrieval_result.get("nodes_count", 0),
            retrieval_result.get("total_tokens", 0),
            retrieval_result.get("trimmed", False)
        )
    except Exception as exc:
        logger.error("Retrieval failed: %s", exc, exc_info=True)
        # Задача preprocessing всё равно считается выполненной —
        # просто без контекста. Генерация ответа пойдёт с пустым retrieved_context.
        retrieval_result = {"error": str(exc), "nodes_count": 0, "total_tokens": 0}

    # === Завершение задачи ===
    complete_task_success(
        task_id=task_id,
        output_data={
            "routing": routing_result,
            "retrieval": retrieval_result,
        }
    )