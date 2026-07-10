"""
main-srv/src/orchestrator/orchestrator.py

Главный цикл оркестратора задач AGI.
Возможности:
- Одиночный фоновый поток с безопасным получением задач (FOR UPDATE SKIP LOCKED).
- Контроль параллелизма: одна задача за раз через флаг _composer_busy.
- Проверка зависимостей: задачи с parent_task_id ждут завершения родителя.
- Интеграция с жизненным циклом: проверка таймаутов неактивности и диалогов.
- Автоматическое планирование фоновых задач в режиме sleep (memory_extraction, verification_proposal).

Обработчики задач:
- user_answer_generation → response_composer.py
- memory_extraction → memory_composer.py
- verification_proposal → verification_composer.py (инициация верификации через NOTIFY)
- hypothesis_refinement → verification_composer.py (LLM-уточнение)
- graph_update → graph_composer.py
- graph_route_and_create → graph_route_composer.py
- graph_merge_resolve → graph_merge_composer.py
- graph_relation_linker → graph_linker_composer.py
- graph_summarize → graph_summarize_composer.py

Архитектура:
- Оркестратор работает как daemon-поток, запускаемый из main.py.
- Задачи создаются через orchestrator_entry.
- Обработчики — тонкие обёртки, делегирующие работу в композеры:
  * user_answer_generation → response_composer.py
  * memory_extraction → memory_composer.py
- Метрики обновляются через service_metrics.
"""

__version__ = "1.4.0"
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

# Защитный интервал после запуска оркестратора (секунды)
# В течение этого времени фоновые задачи (memory_extraction, verification_proposal)
# не планируются, чтобы избежать реакции на временные переходы lifecycle
# при старте системы (например, после краша).
ORCHESTRATOR_STARTUP_GRACE_PERIOD = 60

# =============================================================================
# ФЛАГ ЗАНЯТОСТИ ДЛЯ КОНТРОЛЯ ПАРАЛЛЕЛИЗМА
# =============================================================================

# Разрешаем только одну одновременную генерацию ответа (чтобы не перегружать LLM)
_composer_busy: bool = False
_composer_lock: threading.Lock = threading.Lock()


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


def _handle_memory_extraction(task_id: str, input_data: dict) -> None:
    """
    Обработчик задачи извлечения гипотез в долговременную память.
    Запускается в отдельном потоке.
    Логика:
    1. Импортирует compose_memory_extraction внутри функции (защита от циклических импортов).
    2. Вызывает композер.
    3. При ошибке — завершает задачу как failed.
    4. Всегда сбрасывает флаг занятости в finally.
    """
    global _composer_busy
    try:
        from memory_service.memory_composer import compose_memory_extraction
        compose_memory_extraction(task_id=task_id, input_data=input_data)
    except Exception as exc:
        logger.exception(
            "Error in memory_composer (task_id=%s): %s", task_id[:8], exc
        )
        complete_task_error(
            task_id=task_id,
            error_module="memory_composer",
            error_message=str(exc)
        )
    finally:
        with _composer_lock:
            _composer_busy = False


def _handle_verification_proposal(task_id: str, input_data: dict) -> None:
    """Обработчик задачи инициации верификации. Делегирует в verification_composer."""
    global _composer_busy
    try:
        from memory_service.verification_composer import compose_verification_proposal
        compose_verification_proposal(task_id=task_id, input_data=input_data)
    except Exception as exc:
        logger.exception("Error in verification_proposal (task_id=%s): %s", task_id[:8], exc)
        complete_task_error(task_id=task_id, error_module="verification_composer", error_message=str(exc))
    finally:
        with _composer_lock:
            _composer_busy = False


def _handle_hypothesis_refinement(task_id: str, input_data: dict) -> None:
    """Обработчик задачи LLM-уточнения гипотезы."""
    global _composer_busy
    try:
        from memory_service.verification_composer import compose_hypothesis_refinement
        compose_hypothesis_refinement(task_id=task_id, input_data=input_data)
    except Exception as exc:
        logger.exception("Error in hypothesis_refinement (task_id=%s): %s", task_id[:8], exc)
        complete_task_error(task_id=task_id, error_module="verification_composer", error_message=str(exc))
    finally:
        with _composer_lock:
            _composer_busy = False


def _handle_topic_classification(task_id: str, input_data: dict) -> None:
    global _composer_busy
    try:
        from memory_service.topic_composer import compose_topic_classification
        compose_topic_classification(task_id=task_id, input_data=input_data)
    except Exception as exc:
        logger.exception("Error in topic_classification (task=%s): %s", task_id[:8], exc)
        complete_task_error(task_id=task_id, error_module="topic_composer", error_message=str(exc))
    finally:
        with _composer_lock:
            _composer_busy = False


def _handle_graph_route(task_id: str, input_data: dict) -> None:
    global _composer_busy
    try:
        from memory_service.graph_route_composer import compose_graph_route_and_create
        compose_graph_route_and_create(task_id, input_data)
    except Exception as exc:
        logger.exception("Error in graph_route (task_id=%s): %s", task_id[:8], exc)
        complete_task_error(task_id, "graph_route_composer", str(exc))
    finally:
        with _composer_lock: _composer_busy = False


def _handle_graph_merge(task_id: str, input_data: dict) -> None:
    global _composer_busy
    try:
        from memory_service.graph_merge_composer import compose_graph_merge_resolve
        compose_graph_merge_resolve(task_id, input_data)
    except Exception as exc:
        logger.exception("Error in graph_merge (task_id=%s): %s", task_id[:8], exc)
        complete_task_error(task_id, "graph_merge_composer", str(exc))
    finally:
        with _composer_lock: _composer_busy = False


def _handle_graph_linker(task_id: str, input_data: dict) -> None:
    global _composer_busy
    try:
        from memory_service.graph_linker_composer import compose_graph_relation_linker
        compose_graph_relation_linker(task_id, input_data)
    except Exception as exc:
        logger.exception("Error in graph_linker (task_id=%s): %s", task_id[:8], exc)
        complete_task_error(task_id, "graph_linker_composer", str(exc))
    finally:
        with _composer_lock: _composer_busy = False


def _handle_graph_summarize(task_id: str, input_data: dict) -> None:
    global _composer_busy
    try:
        from memory_service.graph_summarize_composer import compose_graph_summarize
        compose_graph_summarize(task_id, input_data)
    except Exception as exc:
        logger.exception("Error in graph_summarize (task_id=%s): %s", task_id[:8], exc)
        complete_task_error(task_id, "graph_summarize_composer", str(exc))
    finally:
        with _composer_lock: _composer_busy = False


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
    
def _has_active_task_of_type(cur, type_name: str) -> bool:
    """
    Строгая проверка: существует ли задача указанного типа в статусах pending/running.
    Используется для предотвращения дублирования фоновых задач.
    """
    cur.execute("""
        SELECT 1 FROM orchestrator.orchestrator_tasks t
        JOIN orchestrator.task_types tt ON t.task_type_id = tt.id
        WHERE tt.type_name = %s 
          AND t.status IN ('pending'::task_status, 'running'::task_status)
        LIMIT 1
    """, (type_name,))
    return cur.fetchone() is not None
  

def _orchestrator_loop(lifecycle_mgr: LifecycleManager):
    """
    Основной цикл оркестратора.
    На каждом пульсе:
    1. Проверяет таймаут бездействия (lifecycle).
    2. Проверяет таймауты диалогов.
    3. Если не занят — извлекает задачу генерации ответа.
    4. Запускает обработчик в отдельном потоке.
    
    Задачи с parent_task_id пропускаются, если родитель не завершён.
    
    В течение первых ORCHESTRATOR_STARTUP_GRACE_PERIOD секунд после запуска
    фоновые задачи (memory_extraction, verification_proposal, graph_update)
    не планируются, чтобы избежать реакции на временные переходы lifecycle
    при старте системы (например, после краша).
    """
    global _composer_busy
    db_config = load_postgres_config()
    pulse_seconds = load_pulse_seconds(db_config)
   
    _orchestrator_start_time = time.time()
    logger.info("Orchestrator started. Pulse interval: %d second(s), start_epoch=%.0f", 
                pulse_seconds, _orchestrator_start_time)

    while _running:
        try:
            # === ПРОВЕРКА ТАЙМАУТОВ ===
            lifecycle_mgr.check_inactivity()
            check_dialogue_timeouts(db_config)

            # === ПОЛУЧЕНИЕ ТЕКУЩЕГО СОСТОЯНИЯ ЖИЗНЕННОГО ЦИКЛА ===
            current_state = None
            try:
                current_state = lifecycle_mgr._get_global_lifecycle()
            except Exception as e:
                logger.warning("Failed to get global lifecycle state: %s", e)

            # === ПЛАНИРОВАНИЕ ФОНОВЫХ ЗАДАЧ В РЕЖИМЕ SLEEP ===
            if current_state and current_state.get('state_type') == 'sleep':
                if (time.time() - _orchestrator_start_time) < ORCHESTRATOR_STARTUP_GRACE_PERIOD:
                    time.sleep(pulse_seconds)
                    continue
                if _composer_busy:
                    time.sleep(pulse_seconds)
                    continue

                try:
                    with psycopg2.connect(**db_config) as conn:
                        with conn.cursor() as cur:
                            # === 1. memory_extraction (если есть необработанные сообщения) ===
                            if not _has_active_task_of_type(cur, 'memory_extraction'):
                                cur.execute("""
                                    SELECT 1 FROM dialogs.row_messages rm
                                    JOIN dialogs.dialogues d ON rm.dialogue_id = d.id
                                    WHERE d.status = 'completed'
                                    AND NOT EXISTS (SELECT 1 FROM memory.message_analyses ma WHERE ma.message_id = rm.id)
                                    LIMIT 1
                                """)
                                if cur.fetchone():
                                    from orchestrator.orchestrator_entry import schedule_memory_extraction
                                    schedule_memory_extraction(priority=0.3)
                                    logger.info("Scheduled memory_extraction: unprocessed messages found")

                            # === 2. topic_classification (если есть draft без topic_id) ===
                            if not _has_active_task_of_type(cur, 'topic_classification'):
                                cur.execute("""
                                    SELECT 1 FROM memory.hypotheses
                                    WHERE status = 'draft'::memory.hypothesis_status AND topic_id IS NULL
                                    LIMIT 1
                                """)
                                if cur.fetchone():
                                    from orchestrator.orchestrator_entry import schedule_topic_classification
                                    schedule_topic_classification(priority=0.4)
                                    logger.info("Scheduled topic_classification: unclassified drafts found")

                            # === 3. verification_proposal (если есть needs_clarification и нет активной сессии) ===
                            if not _has_active_task_of_type(cur, 'verification_proposal'):
                                cur.execute("""
                                    SELECT 1 FROM memory.verification_sessions
                                    WHERE status = 'active'::memory.verification_session_status
                                    OR (status = 'deferred'::memory.verification_session_status AND deferred_until > NOW())
                                    LIMIT 1
                                """)
                                if not cur.fetchone():
                                    cur.execute("""
                                        SELECT 1 FROM memory.hypotheses
                                        WHERE status = 'needs_clarification'::memory.hypothesis_status
                                        LIMIT 1
                                    """)
                                    if cur.fetchone():
                                        from orchestrator.orchestrator_entry import schedule_verification_proposal
                                        schedule_verification_proposal(priority=0.2)
                                        logger.info("Scheduled verification_proposal: needs_clarification found")

                            # === 4. GRAPH PIPELINE — ТОЛЬКО ПЕРВЫЙ ШАГ ===
                            # Планировщик создаёт ТОЛЬКО graph_route_and_create.
                            # Остальные шаги (merge → linker → summarize) создаются
                            # автоматически предыдущими композерами через parent_task_id.
                            if not _has_active_task_of_type(cur, 'graph_route_and_create'):
                                # Проверяем: есть ли вообще работа для пайплайна?
                                cur.execute("""
                                    SELECT 1 FROM memory.hypotheses
                                    WHERE status = 'confirmed'::memory.hypothesis_status
                                    AND graph_merge_status = 'none'
                                    AND topic_id IS NOT NULL
                                    LIMIT 1
                                """)
                                if cur.fetchone():
                                    # Дополнительная защита: не запускаем, если уже есть
                                    # задачи следующих этапов в работе (пайплайн ещё не закончился)
                                    cur.execute("""
                                        SELECT 1 FROM orchestrator.orchestrator_tasks t
                                        JOIN orchestrator.task_types tt ON t.task_type_id = tt.id
                                        WHERE tt.type_name IN ('graph_merge_resolve', 'graph_relation_linker', 'graph_summarize')
                                        AND t.status IN ('pending'::task_status, 'running'::task_status)
                                        LIMIT 1
                                    """)
                                    if not cur.fetchone():
                                        from orchestrator.orchestrator_entry import schedule_graph_route_and_create
                                        # dialogue_id=None — композер сам выберет все confirmed гипотезы
                                        schedule_graph_route_and_create(dialogue_id=None, priority=0.4)
                                        logger.info("Scheduled graph_route_and_create: pipeline started")

                except Exception as e:
                    logger.warning("Background scheduling failed: %s", e)
                            
            # === ДИСПЕТЧЕРИЗАЦИЯ ЗАДАЧ ===
            if not _composer_busy:
                task = _get_pending_task(db_config, "user_answer_generation")
                if not task: task = _get_pending_task(db_config, "hypothesis_refinement")
                if not task: task = _get_pending_task(db_config, "memory_extraction")
                if not task: task = _get_pending_task(db_config, "topic_classification")
                if not task: task = _get_pending_task(db_config, "verification_proposal")
                if not task: task = _get_pending_task(db_config, "graph_route_and_create")
                if not task: task = _get_pending_task(db_config, "graph_merge_resolve")
                if not task: task = _get_pending_task(db_config, "graph_relation_linker")
                if not task: task = _get_pending_task(db_config, "graph_summarize")
                
                if task:
                    task_id = task["id"]
                    input_data = task["input_data"]
                    task_type = _get_task_type_name(db_config, task_id)
                    
                    mark_task_running(task_id)
                    
                    with _composer_lock:
                        _composer_busy = True
                    
                    handlers: Dict[str, Callable] = {
                        "user_answer_generation": _handle_answer_generation,
                        "memory_extraction": _handle_memory_extraction,
                        "topic_classification": _handle_topic_classification,
                        "verification_proposal": _handle_verification_proposal,
                        "hypothesis_refinement": _handle_hypothesis_refinement,
                        "graph_route_and_create": _handle_graph_route,
                        "graph_merge_resolve": _handle_graph_merge,
                        "graph_relation_linker": _handle_graph_linker,
                        "graph_summarize": _handle_graph_summarize,
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


def start_orchestrator(lifecycle_mgr: LifecycleManager) -> threading.Thread | None:
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

    _running = True
    thread = threading.Thread(
        target=_orchestrator_loop,
        args=(lifecycle_mgr,),
        daemon=True,
        name="Orchestrator"
    )
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