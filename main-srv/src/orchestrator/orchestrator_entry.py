"""
main-srv/src/orchestrator/orchestrator_entry.py

Единая точка входа (Facade) для создания задач оркестратора.
Централизует логику INSERT в orchestrator.orchestrator_tasks и валидацию типов задач.

Основные возможности:
1. Конвейер обработки сообщений (on_user_message):
   - Создает связку задач: question_preprocessing (родитель) → user_answer_generation (дочерняя).
   - Гарантирует строгую последовательность через parent_task_id.
   - Фиксирует активность пользователя в LifecycleManager.
2. Планировщики фоновых задач (schedule_*):
   - Предоставляет типизированные обёртки для memory_extraction, verification_proposal, 
     hypothesis_refinement, graph_route_and_create, graph_merge_resolve, entity_clustering и др.
   - Управляет приоритетами задач (например, ответ пользователю = 0.8, граф = 0.2-0.4).
3. Интеграция:
   - Все модули (UI, сессии, лайфцикл) используют этот файл вместо прямых SQL-запросов.
   - Автоматическая подстановка текущей agent_version из version.py.
"""

__version__ = "1.5.0"
__description__ = "Entry point for orchestrator"

import logging
import psycopg2
from typing import Optional, Dict, Any
from psycopg2.extras import RealDictCursor, Json
from services.lifecycle_manager  import LifecycleManager
from db_manager.db_manager import load_postgres_config
from version import __version__ as agent_version

logger = logging.getLogger(__name__)


def on_user_message(message_id: str) -> tuple[str, str]:
    """
    Создаёт пару связанных задач: преданализ → генерация ответа.
    answer_generation имеет parent_task_id = preprocessing_task_id,
    что гарантирует выполнение строго после завершения преданализа.
    
    Returns:
        tuple: (preprocessing_task_id, answer_task_id)
    """
    if not message_id or not isinstance(message_id, str):
        raise ValueError("Message_id must be a non-empty string")

    db_config = load_postgres_config()
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # === 1. Получаем actor_id ===
            cur.execute(
                "SELECT actor_id FROM dialogs.row_messages WHERE id = %s",
                (message_id,)
            )
            msg_row = cur.fetchone()
            if not msg_row:
                raise RuntimeError(f"Message {message_id} not found in dialogs.row_messages")
            actor_id = str(msg_row['actor_id'])

            # === 2. Фиксируем активность для lifecycle ===
            lifecycle_mgr = LifecycleManager(db_config)
            lifecycle_mgr.record_activity(actor_id, 'user_activity')

            # === 3. ID типов задач ===
            cur.execute(
                "SELECT id FROM orchestrator.task_types WHERE type_name = %s",
                ("question_preprocessing",)
            )
            prep_type_row = cur.fetchone()
            if not prep_type_row:
                raise RuntimeError("Task type 'question_preprocessing' not found. Apply V004 migration.")
            prep_type_id = prep_type_row["id"]

            cur.execute(
                "SELECT id FROM orchestrator.task_types WHERE type_name = %s",
                ("user_answer_generation",)
            )
            gen_type_row = cur.fetchone()
            if not gen_type_row:
                raise RuntimeError("Task type 'user_answer_generation' not found.")
            gen_type_id = gen_type_row["id"]

            # === 4. Создаём preprocessing (родитель) ===
            cur.execute("""
                INSERT INTO orchestrator.orchestrator_tasks (
                    task_type_id, input_data, priority, status, agent_version, created_at
                ) VALUES (%(tid)s, %(data)s, %(prio)s, 'pending', %(ver)s, NOW())
                RETURNING id
            """, {
                "tid": prep_type_id,
                "data": Json({"message_id": message_id}),
                "prio": QUESTION_PREPROCESSING_PRIORITY,
                "ver": agent_version,
            })
            prep_task_id = str(cur.fetchone()["id"])

            # === 5. Создаём answer_generation с parent_task_id ===
            cur.execute("""
                INSERT INTO orchestrator.orchestrator_tasks (
                    task_type_id, parent_task_id, input_data, priority, status, agent_version, created_at
                ) VALUES (%(tid)s, %(parent)s, %(data)s, %(prio)s, 'pending', %(ver)s, NOW())
                RETURNING id
            """, {
                "tid": gen_type_id,
                "parent": prep_task_id,
                "data": Json({"message_id": message_id}),
                "prio": 0.8,
                "ver": agent_version,
            })
            gen_task_id = str(cur.fetchone()["id"])

            conn.commit()
            logger.info(
                f"Pipeline created: preprocessing={prep_task_id[:8]} → "
                f"generation={gen_task_id[:8]} (actor={actor_id[:8]})"
            )
            return prep_task_id, gen_task_id

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"Database error in on_user_message: {e}", exc_info=True)
        raise
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Unexpected error in on_user_message: {e}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()


def schedule_memory_extraction(
    message_ids: Optional[list] = None,
    priority: float = 0.3
) -> str:
    """
    Создаёт задачу извлечения гипотез в долговременную память.
    Вызывается оркестратором при переходе в sleep или вручную.
    
    Логика:
    1. Формирование input_data (mode: auto / explicit).
    2. Делегирование в create_orchestrator_task() с валидацией типа задачи.
    3. Возврат UUID созданной задачи.
    
    Args:
        message_ids: если указан — конкретные сообщения (иначе автовыборка из БД)
        priority: приоритет задачи (по умолчанию 0.3 — ниже ответа пользователю)
        
    Returns:
        str: UUID созданной задачи
        
    Raises:
        RuntimeError: если тип задачи не найден или ошибка БД
    """
    input_data: Dict[str, Any] = {"mode": "auto"}
    if message_ids:
        input_data["message_ids"] = message_ids
        input_data["mode"] = "explicit"
    
    return create_orchestrator_task(
        task_type_name="memory_extraction",
        input_data=input_data,
        priority=priority
    )


def schedule_verification_proposal(priority: float = 0.2) -> str:
    """Создаёт задачу проверки необходимости верификации."""
    return create_orchestrator_task(
        task_type_name="verification_proposal",
        input_data={"mode": "auto"},
        priority=priority
    )


# Константа приоритета для обновления графа памяти
GRAPH_UPDATE_PRIORITY = 0.2

def schedule_graph_update(
    hypothesis_id: str,
    priority: float = GRAPH_UPDATE_PRIORITY,
) -> str:
    """Создаёт задачу обновления графа знаний из подтверждённой гипотезы."""
    return create_orchestrator_task(
        task_type_name="graph_update",
        input_data={"hypothesis_id": hypothesis_id},
        priority=priority,
    )


def create_orchestrator_task(
    task_type_name: str,
    input_data: Dict[str, Any],
    priority: float = 0.5,
    parent_task_id: Optional[str] = None
) -> str:
    """
    Универсальная функция создания задачи оркестратора.
    Единственная точка входа для создания задач. Все модули должны
    использовать эту функцию вместо прямого INSERT.

    Args:
        task_type_name: Имя типа задачи (user_answer_generation и т.д.)
        input_data: Данные для задачи (message_id и т.д.)
        priority: Приоритет задачи (0.0-1.0, по умолчанию 0.5)
        parent_task_id: UUID родительской задачи для зависимостей (опционально)
        
    Returns:
        str: UUID созданной задачи
        
    Raises:
        RuntimeError: если тип задачи не найден или ошибка БД
    """
    db_config = load_postgres_config()
    
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 1. Проверяем существование типа задачи
                cur.execute(
                    "SELECT id FROM orchestrator.task_types WHERE type_name = %s",
                    (task_type_name,)
                )
                row = cur.fetchone()
                if not row:
                    raise RuntimeError(
                        f"Task type '{task_type_name}' not found in orchestrator.task_types. "
                        "Ensure migration was applied."
                    )
                task_type_id = row["id"]
                
                # 2. Создаём задачу с PHS-штампами и parent_task_id
                cur.execute("""
                    INSERT INTO orchestrator.orchestrator_tasks (
                        task_type_id, parent_task_id, input_data, priority, status, 
                        agent_version, created_at
                    ) VALUES (
                        %s, %s, %s, %s, 'pending', %s, NOW()
                    )
                    RETURNING id
                """, (
                    task_type_id, 
                    parent_task_id, 
                    Json(input_data), 
                    priority, 
                    agent_version
                ))
                
                task_id = str(cur.fetchone()["id"])
                conn.commit()
                
                logger.info(
                    f"Orchestrator task created: type={task_type_name}, "
                    f"task_id={task_id[:8]}, priority={priority}, "
                    f"parent_task_id={parent_task_id[:8] if parent_task_id else 'None'}"
                )
                return task_id
                
    except psycopg2.Error as e:
        logger.error(f"Database error creating orchestrator task: {e}", exc_info=True)
        raise RuntimeError(f"Failed to create orchestrator task: {e}") from e


# Константа приоритета для уточнения гипотез
HYPOTHESIS_REFINEMENT_PRIORITY = 0.6

def schedule_hypothesis_refinement(
    hypothesis_id: str,
    user_comment: str,
    verification_session_id: Optional[str] = None,
    metadata_updates: Optional[dict] = None,
    priority: float = 0.5
) -> str:
    """Создаёт задачу LLM-уточнения гипотезы."""
    return create_orchestrator_task(
        task_type_name="hypothesis_refinement",
        input_data = {
            "hypothesis_id": hypothesis_id,
            "user_comment": user_comment,
            "verification_session_id": verification_session_id,
            "metadata_updates": metadata_updates or {},
        },
        priority=priority,
    )


def schedule_topic_classification(priority: float = 0.4) -> str:
    """Планирует задачу классификации гипотез по темам."""
    from db_manager.db_manager import load_postgres_config
    db_config = load_postgres_config()
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id FROM orchestrator.task_types WHERE type_name = 'topic_classification'
            """)
            row = cur.fetchone()
            if not row: raise RuntimeError("task_type 'topic_classification' not found")
            task_type_id = row[0]
            
            cur.execute("""
                INSERT INTO orchestrator.orchestrator_tasks (task_type_id, input_data, priority, status, agent_version)
                VALUES (%s, %s, %s, 'pending', %s) RETURNING id
            """, (task_type_id, '{"limit": 200}', priority, agent_version))
            task_id = str(cur.fetchone()[0])
            conn.commit()
            return task_id


def schedule_form_classification(priority: float = 0.35) -> str:
    """Планирует задачу классификации гипотез по формам."""
    return create_orchestrator_task(
        task_type_name="form_classification",
        input_data={"limit": 200},
        priority=priority
    )


def schedule_graph_route_and_create(dialogue_id: Optional[str] = None, priority: float = 0.4) -> str:
    """Создаёт задачу детерминированного роутинга и создания узлов графа памяти."""
    return create_orchestrator_task("graph_route_and_create", {"dialogue_id": dialogue_id}, priority)


def schedule_graph_merge_resolve(priority: float = 0.3, parent_task_id: Optional[str] = None) -> str:
    """Создаёт задачу LLM-разрешения слияний графа памяти."""
    return create_orchestrator_task("graph_merge_resolve", {"mode": "pipeline"}, priority, parent_task_id=parent_task_id)


def schedule_graph_relation_linker(priority: float = 0.2, parent_task_id: Optional[str] = None) -> str:
    """Создаёт задачу фонового построения связей между узлами графа памяти внутри тем."""
    return create_orchestrator_task("graph_relation_linker", {"mode": "pipeline"}, priority, parent_task_id=parent_task_id)


def schedule_graph_summarize(priority: float = 0.1, parent_task_id: Optional[str] = None) -> str:
    """Создаёт задачу фонового сжатия описаний узлов графа памяти."""
    return create_orchestrator_task("graph_summarize", {"mode": "pipeline"}, priority, parent_task_id=parent_task_id)


# === Константа приоритета преданализа ===
QUESTION_PREPROCESSING_PRIORITY = 0.9

def schedule_question_preprocessing(message_id: str, priority: float = QUESTION_PREPROCESSING_PRIORITY) -> str:
    """
    Создаёт задачу преданализа вопроса пользователя (роутинг + выборка знаний).
    Запускается из интерфейса сразу после получения сообщения, СТРО ДО user_answer_generation.
    """
    return create_orchestrator_task(
        task_type_name="question_preprocessing",
        input_data={"message_id": message_id},
        priority=priority,
    )


def schedule_entity_clustering(priority: float = 0.15, parent_task_id: Optional[str] = None) -> str:
    """Создаёт задачу батчевой кластеризации fact-узлов в entity-агрегаторы."""
    return create_orchestrator_task("entity_clustering", {"mode": "auto"}, priority, parent_task_id=parent_task_id)


def schedule_entity_binding(priority: float = 0.15, parent_task_id: Optional[str] = None) -> str:
    """Создаёт задачу инкрементальной привязки fact-узлов к существующим entity."""
    return create_orchestrator_task("entity_binding", {"mode": "auto"}, priority, parent_task_id=parent_task_id)