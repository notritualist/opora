"""
main-srv/src/orchestrator/orchestrator.py

Главный цикл оркестратора задач AGI.
Возможности:
- Одиночный фоновый поток с безопасным получением задач (FOR UPDATE SKIP LOCKED).
- Контроль параллелизма: одна задача генерации за раз через флаг _composer_busy.
- Обработчики задач: user_answer_generation.
- Отказоустойчивость: ошибки логируются, цикл продолжается.
- Интеграция с жизненным циклом: проверка таймаутов неактивности и диалогов на каждом пульсе.
"""

__version__ = "1.1.0"
__description__ = "AGI Agent Task Orchestrator"

import threading
import time
import logging
import psycopg2
from typing import Dict, Callable
from psycopg2.extras import RealDictCursor

# Локальные импорты
from db_manager.db_manager import load_postgres_config
from services.service_metrics import mark_task_running, complete_task_error
from services.lifecycle_manager import LifecycleManager
from dialog_services.dialogue_manager import check_dialogue_timeouts

logger = logging.getLogger(__name__)

# =============================================================================
# НАСТРОЙКИ ОРКЕСТРАТОРА
# =============================================================================

# Флаг работы основного цикла
_running: bool = False

# =============================================================================
# ФЛАГ ЗАНЯТОСТИ ДЛЯ КОНТРОЛЯ ПАРАЛЛЕЛИЗМА
# =============================================================================

# Разрешаем только одну одновременную генерацию ответа (чтобы не перегружать LLM)
_composer_busy: bool = False
_composer_lock: threading.Lock = threading.Lock()

def _cleanup_dangling_records(db_config: dict):
    """
    Очищает зависшие записи при старте оркестратора.
    Выполняется один раз при запуске.
    Обрабатывает:
    - задачи со статусом pending/running → failed,
    - шаги со статусом pending/running → failed
    """
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            # Сброс зависших задач
            cur.execute("""
                UPDATE orchestrator.orchestrator_tasks
                SET 
                    status = 'failed'::task_status,
                    completed_at = NOW(),
                    error_module = 'orchestrator_startup',
                    error_message = 'System restart: task interrupted',
                    error_timestamp = NOW(),
                    run_latency = EXTRACT(EPOCH FROM (NOW() - started_at)),
                    total_latency = EXTRACT(EPOCH FROM (NOW() - created_at))
                WHERE status IN ('pending', 'running')
            """)
            tasks_count = cur.rowcount

            # Сброс зависших шагов
            cur.execute("""
                UPDATE orchestrator.orchestrator_steps
                SET 
                    status = 'failed'::task_status,
                    completed_at = NOW(),
                    error_module = 'orchestrator_startup',
                    error_message = 'System restart: step interrupted',
                    error_timestamp = NOW(),
                    latency = EXTRACT(EPOCH FROM (NOW() - created_at))
                WHERE status IN ('pending', 'running')
            """)
            steps_count = cur.rowcount

            conn.commit()

            if tasks_count > 0:
                logger.warning("Cleared %d dangling tasks on startup", tasks_count)
            if steps_count > 0:
                logger.warning("Cleared %d dangling steps on startup", steps_count)

def _get_pending_task(db_config: dict, task_type_name: str):
    """
    Извлекает следующую ожидающую задачу указанного типа из БД.
    Пропускает задачи, у которых parent_task_id не завершён.
    Использует FOR UPDATE SKIP LOCKED для защиты от дублирования при многопоточности.
    
    Args:
        db_config (dict): параметры подключения к PostgreSQL
        task_type_name (str): имя типа задачи (например, 'user_answer_generation')
        
    Returns:
        dict | None: словарь с полями 'id' и 'input_data', или None
    """
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT t.id, t.input_data, t.parent_task_id
                FROM orchestrator.orchestrator_tasks t
                JOIN orchestrator.task_types tt ON t.task_type_id = tt.id
                WHERE t.status = 'pending'::task_status
                  AND tt.type_name = %s
                ORDER BY t.priority DESC, t.created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            """, (task_type_name,))
            task = cur.fetchone()
            
            if not task:
                return None

            # === ПРОВЕРКА ЗАВИСИМОСТИ ===
            parent_id = task.get('parent_task_id')
            if parent_id:
                cur.execute("""
                    SELECT status FROM orchestrator.orchestrator_tasks WHERE id = %s
                """, (parent_id,))
                parent_row = cur.fetchone()
                
                # Если родитель не завершён — пропускаем задачу в этом пульсе
                if not parent_row or parent_row['status'] != 'completed':
                    logger.debug(
                        f"Task {task['id'][:8]} skipped: parent {parent_id[:8]} "
                        f"status={parent_row['status'] if parent_row else 'missing'}"
                    )
                    # Снимаем блокировку, возвращая задачу в очередь
                    conn.rollback()
                    return None
            
            return task

def _handle_answer_generation(task_id: str, input_data: dict) -> None:
    """
    Обработчик задачи генерации финального ответа пользователю.
    Запускается в отдельном потоке.
    
    Логика:
    1. Импортирует compose_final_response внутри функции (защита от циклических импортов).
    2. Вызывает композер.
    3. При ошибке — завершает задачу как failed.
    4. Всегда сбрасывает флаг занятости в finally.
    """
    global _composer_busy
    try:
        from orchestrator.response_composer import compose_final_response
        compose_final_response(task_id=task_id, input_data=input_data)
    except Exception as exc:
        logger.exception("Error in response_composer (task_id=%s): %s", task_id[:8], exc)
        complete_task_error(
            task_id=task_id,
            error_module="response_composer",
            error_message=str(exc)
        )
    finally:
        with _composer_lock:
            _composer_busy = False

def _get_task_type_name(db_config: dict, task_id: str) -> str:
    """Возвращает type_name задачи по её ID."""
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tt.type_name
                FROM orchestrator.orchestrator_tasks t
                JOIN orchestrator.task_types tt ON t.task_type_id = tt.id
                WHERE t.id = %s
            """, (task_id,))
            row = cur.fetchone()
            return row[0] if row else "unknown"
 
def load_pulse_seconds(db_config: dict) -> int:
    """Загружает orchestrator_pulse_seconds из state.settings."""
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT value_float 
                    FROM state.settings 
                    WHERE param_name = 'orchestrator_pulse_seconds'
                """)
                row = cur.fetchone()
                return int(row[0]) if row and row[0] is not None else 1
    except Exception:
        logger.warning("Failed to load orchestrator_pulse_seconds from DB, using default=1")
        return 1

def _orchestrator_loop():
    """
    Основной цикл оркестратора.
    На каждом пульсе:
    1. Проверяет таймаут бездействия (lifecycle).
    2. Проверяет таймауты диалогов.
    3. Если не занят — извлекает задачу генерации ответа.
    4. Запускает обработчик в отдельном потоке.
    
    Задачи с parent_task_id пропускаются, если родитель не завершён.
    """
    global _composer_busy, _running

    db_config = load_postgres_config()
    pulse_seconds = load_pulse_seconds(db_config)
    # Инициализация lifecycle manager
    lifecycle_mgr = LifecycleManager(db_config)

    logger.info("Orchestrator started. Pulse interval: %d second(s)", pulse_seconds)

    while _running:
        try:
            # === ПРОВЕРКА ТАЙМАУТОВ ===
            lifecycle_mgr.check_inactivity()
            check_dialogue_timeouts(db_config)
            
            if not _composer_busy:
                # === ИЗВЛЕЧЕНИЕ ЗАДАЧИ ===
                task = _get_pending_task(db_config, "user_answer_generation")
                
                if task:
                    task_id = task["id"]
                    input_data = task["input_data"]
                    task_type = _get_task_type_name(db_config, task_id)
                    
                    mark_task_running(task_id)
                    
                    with _composer_lock:
                        _composer_busy = True
                    
                    # Маппинг типов → обработчиков
                    handlers: Dict[str, Callable] = {
                        "user_answer_generation": _handle_answer_generation,
                    }
                    
                    target = handlers.get(task_type)
                    if not target:
                        complete_task_error(task_id, "orchestrator", f"Unknown task type: {task_type}")
                        with _composer_lock:
                            _composer_busy = False
                        continue
                    
                    threading.Thread(
                        target=target,
                        args=(task_id, input_data),
                        daemon=True,
                        name=f"Orch-{task_type[:10]}-{task_id[:8]}"
                    ).start()
                    
                    logger.debug("Launched task %s: %s", task_type, task_id[:8])
            
            time.sleep(pulse_seconds)

        except Exception as exc:
            logger.exception("Critical error in orchestrator loop: %s", exc)
            time.sleep(pulse_seconds)

def start_orchestrator() -> threading.Thread | None:
    """
    Запускает оркестратор в фоновом потоке.
    Выполняет очистку зависших записей перед стартом.
    Защищён от повторного запуска.
    
    Returns:
        threading.Thread | None: ссылка на поток или None, если уже запущен
    """
    global _running
    if _running:
        logger.warning("Orchestrator is already running")
        return None

    db_config = load_postgres_config()
    _cleanup_dangling_records(db_config)

    _running = True
    thread = threading.Thread(target=_orchestrator_loop, daemon=True, name="Orchestrator")
    thread.start()

    logger.info("Orchestrator background thread started")
    return thread

def stop_orchestrator():
    """
    Корректно останавливает оркестратор.
    Устанавливает флаг _running = False, после чего цикл завершится.
    """
    global _running
    _running = False
    logger.info("Orchestrator stopped")