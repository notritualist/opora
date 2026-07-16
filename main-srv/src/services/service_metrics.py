"""
main-srv/src/services/service_metrics.py

Центральный сервис телеметрии, метрик и управления состоянием оркестратора.
Выступает единой точкой входа для всех записей в таблицы orchestrator.* и metrics.*.

Основные возможности:
1. Управление жизненным циклом задач и шагов:
   - mark_task_running / complete_task_success / complete_task_error.
   - create_orchestrator_step / complete_step_success / complete_step_error.
   - Автоматический расчет латентности (run_latency, total_latency) через SQL EXTRACT(EPOCH).
2. Сохранение метрик LLM (с ветвлением по провайдерам):
   - save_llm_metrics: для внутренних провайдеров (local_llama) → metrics.llm_internal.
   - save_llm_external_metrics: для внешних API (DashScope) → metrics.llm_external.
   - save_emb_metrics: для сервера эмбеддингов → metrics.emb_internal.
3. Запись артефактов и рассуждений (Chain-of-Thought):
   - save_llm_artifacts: сохранение полных промптов, ответов и параметров в JSONB.
   - save_reasoning: сохранение текста рассуждений модели в orchestrator.reasonings.
4. Связывание сущностей:
   - Привязка метрик и рассуждений к конкретным шагам оркестратора (set_step_*).
5. Восстановление после сбоев (Crash Recovery):
   - close_dangling_orchestrator_records: при старте системы переводит все "зависшие" 
     задачи и шаги (pending/running) в статус failed, предотвращая блокировку очереди.

Архитектурные принципы:
- Все функции принимают ID и данные, выполняют атомарные SQL-запросы и возвращают ID или None.
- Поддержка JSONB-полей через psycopg2.extras.Json.
- Гибкая система привязки метрик к шагам: complete_step_* принимают опциональные 
  llm_metric_id, llm_metric_external_id и metric_source для корректного маппинга.
"""

__version__ = "1.2.0"
__description__ = "Utility module for updating statuses and saving metrics"

import logging
import psycopg2
import json
from psycopg2.extras import Json
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
from db_manager.db_manager import load_postgres_config
# Единая версия проекта — как в main.py
from version import __version__ as agent_version

# Логгер модуля
logger = logging.getLogger(__name__)

# =============================================================================
# === УПРАВЛЕНИЕ СТАТУСАМИ ЗАДАЧ И ШАГОВ ===
# =============================================================================
def mark_task_running(task_id: str) -> None:
    """
    Помечает задачу как выполняющуюся (status='running').
    
    Вызывается оркестратором перед запуском обработчика в потоке.
    
    Args:
        task_id (str): UUID задачи из orchestrator.orchestrator_tasks
        
    Returns:
        None
    """
    db_config: dict = load_postgres_config()
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE orchestrator.orchestrator_tasks
                SET status = 'running'::task_status,
                    started_at = NOW()
                WHERE id = %s
            """, (task_id,))
            conn.commit()
    logger.debug("ЗTask %s is marked as running", task_id[:8])


def complete_task_success(task_id: str, output_data: Optional[Dict[str, Any]] = None) -> None:
    """
    Завершает задачу успешно (status='completed').
    
    Args:
        task_id (str): UUID задачи
        output_data (dict, optional): Результаты выполнения задачи в формате JSON
    """
    db_config: dict = load_postgres_config()
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE orchestrator.orchestrator_tasks
                SET 
                    status = 'completed'::task_status,
                    completed_at = NOW(),
                    output_data = %s,
                    run_latency = EXTRACT(EPOCH FROM (NOW() - started_at)),
                    total_latency = EXTRACT(EPOCH FROM (NOW() - created_at))
                WHERE id = %s
            """, (Json(output_data) if output_data else None, task_id))
            conn.commit()
    logger.info("Task %s completed successfully", task_id[:8])


def complete_task_error(
        task_id: str,
        error_module: str,
        error_message: str
    ) -> None:
        """
        Завершает задачу с ошибкой (status='failed').
        
        Args:
            task_id (str): UUID задачи
            error_module (str): Имя модуля, где произошла ошибка (для трассировки)
            error_message (str): Текст ошибки
            
        Returns:
            None
        """
        db_config: dict = load_postgres_config()
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE orchestrator.orchestrator_tasks
                    SET status = 'failed'::task_status,
                        completed_at = NOW(),
                        error_module = %s,
                        error_message = %s,
                        error_timestamp = NOW(),
                        total_latency = EXTRACT(EPOCH FROM (NOW() - created_at)),
                        run_latency = EXTRACT(EPOCH FROM (NOW() - started_at))
                    WHERE id = %s
                """, (error_module, error_message, task_id))
                conn.commit()
        logger.warning("Task %s completed with error: %s", task_id[:8], error_message)


def create_orchestrator_step(
    task_id: str,
    step_number: int,
    step_type_name: str,
    input_data: Optional[Dict[str, Any]] = None
) -> str:
    """
    Создаёт новый шаг оркестратора для задачи.
    
    Args:
        task_id (str): UUID родительской задачи
        step_number (int): Порядковый номер шага в задаче (начинается с 1)
        step_type_name (str): Имя типа шага из orchestrator.ste p_types.step_name
        input_data (dict, optional): Входные данные шага в формате JSON
        
    Returns:
        str: UUID созданного шага
    """
    db_config: dict = load_postgres_config()
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            # Получаем ID типа шага
            cur.execute("""
                SELECT id FROM orchestrator.step_types 
                WHERE step_name = %s
            """, (step_type_name,))
            row = cur.fetchone()
            if not row:
                raise RuntimeError(f"Step type '{step_type_name}' not found in orchestrator.step_types")
            step_type_id = row[0]
            
            # Создаём шаг
            cur.execute("""
                INSERT INTO orchestrator.orchestrator_steps (
                    task_id,
                    step_number,
                    step_type_id,
                    status,
                    input_data,
                    agent_version,
                    created_at
                ) VALUES (
                    %s, %s, %s, 'pending'::task_status, %s, %s, NOW()
                )
                RETURNING id
            """, (
                task_id,
                step_number,
                step_type_id,
                Json(input_data) if input_data else None,
                agent_version
            ))
            step_id = str(cur.fetchone()[0])
            conn.commit()
            
    logger.debug("Step %s created for task %s (type: %s)", step_id[:8], task_id[:8], step_type_name)
    return step_id


def complete_step_success(
    step_id: str,
    output_data: Optional[Dict[str, Any]] = None,
    llm_metric_id: Optional[str] = None,
    llm_metric_external_id: Optional[str] = None,   # ← НОВОЕ
    metric_source: Optional[str] = None,            # ← НОВОЕ
    emb_metric_id: Optional[str] = None
) -> None:
    """Завершает шаг оркестратора со статусом 'completed'."""
    db_config = load_postgres_config()
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE orchestrator.orchestrator_steps
                    SET status = 'completed'::task_status,
                        output_data = %s::jsonb,
                        completed_at = NOW(),
                        latency = EXTRACT(EPOCH FROM (NOW() - created_at))
                    WHERE id = %s
                """, (json.dumps(output_data or {}), step_id))

                update_fields = []
                params = []
                if llm_metric_id:
                    update_fields.append("llm_metric_id = %s::uuid")
                    params.append(llm_metric_id)
                if llm_metric_external_id:
                    update_fields.append("llm_metric_external_id = %s::uuid")
                    params.append(llm_metric_external_id)
                if metric_source:
                    update_fields.append("metric_source = %s::metric_source")
                    params.append(metric_source)
                if emb_metric_id:
                    update_fields.append("emb_metric_id = %s::uuid")
                    params.append(emb_metric_id)

                if update_fields:
                    params.append(step_id)
                    cur.execute(f"""
                        UPDATE orchestrator.orchestrator_steps
                        SET {', '.join(update_fields)}
                        WHERE id = %s
                    """, params)
                conn.commit()
        logger.debug("Step %s completed successfully", step_id[:8])
    except Exception as e:
        logger.error("Failed to complete step %s: %s", step_id[:8], e, exc_info=True)


def complete_step_error(
    step_id: str,
    error_module: str,
    error_message: str,
    llm_metric_id: Optional[str] = None,
    llm_metric_external_id: Optional[str] = None,   # ← НОВОЕ
    metric_source: Optional[str] = None,            # ← НОВОЕ
    emb_metric_id: Optional[str] = None
) -> None:
    """Завершает шаг оркестратора со статусом 'failed'."""
    db_config = load_postgres_config()
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE orchestrator.orchestrator_steps
                    SET status = 'failed'::task_status,
                        error_module = %s,
                        error_message = %s,
                        error_timestamp = NOW(),
                        completed_at = NOW(),
                        latency = EXTRACT(EPOCH FROM (NOW() - created_at))
                    WHERE id = %s
                """, (error_module, error_message, step_id))

                update_fields = []
                params = []
                if llm_metric_id:
                    update_fields.append("llm_metric_id = %s::uuid")
                    params.append(llm_metric_id)
                if llm_metric_external_id:
                    update_fields.append("llm_metric_external_id = %s::uuid")
                    params.append(llm_metric_external_id)
                if metric_source:
                    update_fields.append("metric_source = %s::metric_source")
                    params.append(metric_source)
                if emb_metric_id:
                    update_fields.append("emb_metric_id = %s::uuid")
                    params.append(emb_metric_id)

                if update_fields:
                    params.append(step_id)
                    cur.execute(f"""
                        UPDATE orchestrator.orchestrator_steps
                        SET {', '.join(update_fields)}
                        WHERE id = %s
                    """, params)
                conn.commit()
        logger.debug("Step %s marked as failed", step_id[:8])
    except Exception as e:
        logger.error("Failed to complete step error %s: %s", step_id[:8], e, exc_info=True)
        
# =============================================================================
# === СОХРАНЕНИЕ МЕТРИК И РАССУЖДЕНИЙ ===
# =============================================================================
def save_llm_metrics(
    orchestrator_step_id: str,
    prompt_id: str,
    host: str,
    model: str,
    param: Dict[str, Any],
    cache_n: int,
    prompt_tokens: int,
    completion_tokens: int,
    total_tokens: int,
    host_nctx: int,
    prompt_ms: float,
    prompt_per_token_ms: float,
    prompt_per_second: float,
    predicted_per_second: float,
    resp_time: float,
    net_latency: float,
    full_time: float,
    error_status: bool = False,
    error_message: Optional[str] = None
) -> str:
    """
    Сохраняет метрики LLM-запроса в metrics.llm_internal.
    
    Args:
        orchestrator_step_id (str): UUID шага оркестратора, инициировавшего запрос
        prompt_id (str): UUID использованного промпта
        host (str): Имя хоста, где выполнялся запрос
        model (str): Название модели
        param (dict): Параметры генерации (temperature, top_p и т.д.)
        cache_n (int): Количество токенов, взятых из кэша
        prompt_tokens (int): Токены во входном промпте
        completion_tokens (int): Токены в сгенерированном ответе
        total_tokens (int): Общее количество обработанных токенов
        host_nctx (int): Размер контекста (n_ctx) на хосте
        prompt_ms (float): Время обработки промпта в мс
        prompt_per_token_ms (float): Среднее время на токен промпта
        prompt_per_second (float): Скорость обработки промпта (токенов/сек)
        predicted_per_second (float): Скорость генерации ответа (токенов/сек)
        resp_time (float): Общее время генерации ответа в секундах
        net_latency (float): Сетевая задержка в секундах
        full_time (float): Полное время выполнения запроса в секундах
        error_status (bool): Флаг ошибки (по умолчанию False)
        error_message (str, optional): Текст ошибки, если была
        
    Returns:
        str: UUID записи метрики
    """
    db_config: dict = load_postgres_config()
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO metrics.llm_internal (
                    orchestrator_step_id,
                    prompt_id,
                    host,
                    model,
                    param,
                    cache_n,
                    prompt_tokens,
                    completion_tokens,
                    total_tokens,
                    host_nctx,
                    prompt_ms,
                    prompt_per_token_ms,
                    prompt_per_second,
                    predicted_per_second,
                    resp_time,
                    net_latency,
                    full_time,
                    error_status,
                    error_message,
                    error_time,
                    agent_version,
                    timestamp
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                )
                RETURNING id
            """, (
                orchestrator_step_id,
                prompt_id,
                host,
                model,
                Json(param),
                cache_n,
                prompt_tokens,
                completion_tokens,
                total_tokens,
                host_nctx,
                prompt_ms,
                prompt_per_token_ms,
                prompt_per_second,
                predicted_per_second,
                resp_time,
                net_latency,
                full_time,
                error_status,
                error_message,
                datetime.now(timezone.utc) if error_status else None,
                agent_version
            ))
            metric_id = str(cur.fetchone()[0])
            conn.commit()
            
    logger.debug("LLM metrics saved: %s (step: %s)", metric_id[:8], orchestrator_step_id[:8])
    return metric_id


def save_reasoning(
    orchestrator_step_id: str,
    content: str,
    content_type: str
) -> Optional[str]:
    """
    Сохраняет рассуждение (Chain of Thought) в orchestrator.reasonings.
    
    Args:
        orchestrator_step_id (str): UUID шага, в рамках которого сгенерировано рассуждение
        content (str): Текст рассуждения
        content_type (str): Тип рассуждения из ENUM re asoning_content_type
        
    Returns:
        str | None: UUID записи рассуждения или None, если не сохранено
    """
    if not content or not content.strip():
        return None
        
    db_config: dict = load_postgres_config()
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO orchestrator.reasonings (
                    orchestrator_step_id,
                    reasoning_content,
                    reasoning_content_type,
                    agent_version,
                    timestamp
                ) VALUES (
                    %s, %s, %s, %s, NOW()
                )
                RETURNING id
            """, (
                orchestrator_step_id,
                content,
                content_type,
                agent_version
            ))
            reasoning_id = str(cur.fetchone()[0])
            conn.commit()
            
    logger.debug("Reasoning saved: %s (step: %s)", reasoning_id[:8], orchestrator_step_id[:8])
    return reasoning_id


def set_step_llm_metric_id(step_id: str, llm_metric_id: str) -> None:
    """
    Привязывает запись метрики LLM к шагу оркестратора.
    
    Args:
        step_id (str): UUID шага в orchestrator.orchestrator_steps
        llm_metric_id (str): UUID метрики в metrics.llm_internal
    """
    db_config: dict = load_postgres_config()
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE orchestrator.orchestrator_steps
                SET llm_metric_id = %s
                WHERE id = %s
            """, (llm_metric_id, step_id))
            conn.commit()
    logger.debug("Linked llm_metric_id %s to step %s", llm_metric_id[:8], step_id[:8])


def set_step_reasoning_id(step_id: str, reasoning_id: str) -> None:
    """
    Привязывает запись рассуждения к шагу оркестратора.
    
    Примечание: в текущей схеме рассуждение уже ссылается на шаг через 
    orchestrator.reasonings.orchestrator_step_id. Эта функция может использоваться
    для дополнительной индексации или кэширования, если потребуется в будущем.
    
    В текущей реализации — заглушка для совместимости с интерфейсом.
    
    Args:
        step_id (str): UUID шага
        reasoning_id (str): UUID рассуждения
    """
    # В текущей схеме V001 связь идёт "снизу вверх" (reasoning → step),
    # поэтому обратная ссылка не требуется. Функция оставлена для будущего расширения.
    logger.debug("Reasoning %s already linked to step %s via FK", reasoning_id[:8], step_id[:8])
    pass


def save_llm_artifacts(
    llm_metric_id: Optional[str],          # ← был str, стал Optional
    orchestrator_step_id: Optional[str],
    messages: List[Dict[str, str]],
    raw_response: str,
    final_params: Dict[str, Any],
    llm_metric_external_id: Optional[str] = None,   # ← НОВОЕ
    metric_source: str = "internal"                 # ← НОВОЕ
) -> str:
    """Сохраняет артефакты. llm_metric_id или llm_metric_external_id — в зависимости от metric_source."""
    db_config = load_postgres_config()
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO metrics.llm_artifacts (
                    llm_metric_id, llm_metric_external_id, metric_source,
                    orchestrator_step_id, messages_json, raw_response, final_params, agent_version
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                llm_metric_id, llm_metric_external_id, metric_source,
                orchestrator_step_id,
                Json(messages), raw_response, Json(final_params), agent_version
            ))
            artifact_id = str(cur.fetchone()[0])
            conn.commit()
    logger.debug("LLM artifacts saved: %s (source=%s)", artifact_id[:8], metric_source)
    return artifact_id


def save_emb_metrics(
    orchestrator_step_id: str,
    host: str,
    model: str,
    param: Dict[str, Any],
    vector_dimension: int,
    prompt_tokens: int,
    received_at: Optional[datetime],
    sent_at: Optional[datetime],
    full_time: float,
    error_status: bool,
    error_message: Optional[str] = None,
    agent_version: str = "unknown"
) -> str:
    """
    Сохраняет метрики эмбеддинга в metrics.emb_internal.
    
    Args:
        orchestrator_step_id: UUID шага оркестратора
        host: Хост и порт emb-srv (например, "192.168.100.3:8000")
        model: Название модели эмбеддингов
        param: Параметры векторизации (JSONB)
        vector_dimension: Размерность полученного вектора
        prompt_tokens: Количество токенов в тексте
        received_at: Время получения запроса
        sent_at: Время отправки ответа
        full_time: Общее время генерации (сек)
        error_status: Флаг ошибки
        error_message: Текст ошибки (если была)
        agent_version: Глобальная версия агента
        
    Returns:
        str: UUID записи метрики
    """
    db_config: dict = load_postgres_config()
    
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO metrics.emb_internal (
                    orchestrator_step_id, host, model, param,
                    vector_dimension, prompt_tokens,
                    received_at, sent_at, full_time,
                    error_status, error_message, error_time,
                    agent_version, timestamp
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                )
                RETURNING id
            """, (
                orchestrator_step_id, host, model, Json(param),
                vector_dimension, prompt_tokens,
                received_at, sent_at, full_time,
                error_status, error_message,
                datetime.now(timezone.utc) if error_status else None,
                agent_version
            ))
            metric_id = str(cur.fetchone()[0])
            conn.commit()
            
    logger.debug("Emb metrics saved: %s (step: %s)", metric_id[:8], orchestrator_step_id[:8])
    return metric_id


def save_llm_external_metrics(
    orchestrator_step_id: str,
    prompt_id: str,
    provider: str,
    model: str,
    param: Dict[str, Any],
    prompt_tokens: int,
    completion_tokens: int,
    total_tokens: int,
    host_nctx: int,
    prompt_ms: float = 0.0,
    prompt_per_token_ms: float = 0.0,
    prompt_per_second: float = 0.0,
    predicted_per_second: float = 0.0,
    resp_time: float = 0.0,
    net_latency: float = 0.0,
    full_time: float = 0.0,
    error_status: bool = False,
    error_message: Optional[str] = None
) -> str:
    """Сохраняет метрики ВНЕШНЕГО LLM API в metrics.llm_external."""
    db_config: dict = load_postgres_config()
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO metrics.llm_external (
                    orchestrator_step_id, prompt_id, provider, model, param,
                    prompt_tokens, completion_tokens, total_tokens, host_nctx,
                    prompt_ms, prompt_per_token_ms, prompt_per_second,
                    predicted_per_second, resp_time, net_latency, full_time,
                    error_status, error_message, error_time,
                    agent_version, timestamp
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                )
                RETURNING id
            """, (
                orchestrator_step_id, prompt_id, provider, model, Json(param),
                prompt_tokens, completion_tokens, total_tokens, host_nctx,
                prompt_ms, prompt_per_token_ms, prompt_per_second,
                predicted_per_second, resp_time, net_latency, full_time,
                error_status, error_message,
                datetime.now(timezone.utc) if error_status else None,
                agent_version
            ))
            metric_id = str(cur.fetchone()[0])
            conn.commit()
    logger.debug("External LLM metrics saved: %s (provider=%s)", metric_id[:8], provider)
    return metric_id


def close_dangling_orchestrator_records(db_config: dict) -> tuple:
    """
    Закрывает зависшие задачи и шаги оркестратора при перезапуске системы.
    Вызывается из main.py при старте агента.
    Аналог close_dangling_sessions() и close_dangling_verification_sessions().
    
    Args:
        db_config: параметры подключения к PostgreSQL
        
    Returns:
        tuple: (tasks_closed: int, steps_closed: int)
    """
    tasks_closed = 0
    steps_closed = 0
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                # Закрываем зависшие задачи (pending/running → failed)
                cur.execute("""
                    UPDATE orchestrator.orchestrator_tasks
                    SET status = 'failed'::task_status,
                        completed_at = NOW(),
                        error_module = 'main_startup',
                        error_message = 'System restart: task interrupted',
                        error_timestamp = NOW(),
                        run_latency = EXTRACT(EPOCH FROM (NOW() - started_at)),
                        total_latency = EXTRACT(EPOCH FROM (NOW() - created_at))
                    WHERE status IN ('pending'::task_status, 'running'::task_status)
                """)
                tasks_closed = cur.rowcount
                
                # Закрываем зависшие шаги (pending/running → failed)
                cur.execute("""
                    UPDATE orchestrator.orchestrator_steps
                    SET status = 'failed'::task_status,
                        completed_at = NOW(),
                        error_module = 'main_startup',
                        error_message = 'System restart: step interrupted',
                        error_timestamp = NOW(),
                        latency = EXTRACT(EPOCH FROM (NOW() - created_at))
                    WHERE status IN ('pending'::task_status, 'running'::task_status)
                """)
                steps_closed = cur.rowcount
                
                conn.commit()
                
                if tasks_closed > 0 or steps_closed > 0:
                    logger.warning(
                        "Closed dangling orchestrator records on startup: "
                        "%d task(s), %d step(s)",
                        tasks_closed, steps_closed
                    )
                return (tasks_closed, steps_closed)
    except Exception as e:
        logger.error(
            "Error closing dangling orchestrator records: %s", e, exc_info=True
        )
        return (0, 0)