"""
main-srv/src/orchestrator/orchestrator_entry.py

Входной интерфейс оркестратора.
Единая точка входа для создания задач оркестратора.
Все модули (session_manager, lifecycle_manager, memory_service, verification_service) должны использовать этот интерфейс вместо прямого SQL INSERT.

Поддерживаемые типы задач:
- user_answer_generation: генерация финального ответа пользователю.
- memory_extraction: извлечение гипотез из закрытых диалогов в долговременную память.
- verification_proposal: проверка наличия draft-гипотез и отправка NOTIFY в UI.
- hypothesis_refinement: LLM-уточнение гипотезы по комментарию пользователя.
- schedule_graph_update: обновление графа знаний из подтверждённой гипотезы.

Архитектура:
- Универсальная функция create_orchestrator_task() с валидацией task_type.
Специализированные обёртки: on_user_message, schedule_memory_extraction, schedule_verification_proposal, schedule_hypothesis_refinement.

Интеграция с жизненным циклом:
- Активность фиксируется с actor_id из сообщения.
- Агент-версия передаётся глобально через version.py.
"""

__version__ = "1.3.0"
__description__ = "Entry point for orchestrator"

import logging
import psycopg2
from typing import Optional, Dict, Any
from psycopg2.extras import RealDictCursor, Json
from services.lifecycle_manager  import LifecycleManager
from db_manager.db_manager import load_postgres_config
from version import __version__ as agent_version

logger = logging.getLogger(__name__)


def on_user_message(message_id: str) -> str:
    """
    Создаёт задачу генерации ответа при получении сообщения пользователя.
    
    Логика:
    1. Создание задачи user_answer_generation с приоритетом 0.8.  
    """
    if not message_id or not isinstance(message_id, str):
        raise ValueError("Message_id must be a non-empty string")

    db_config = load_postgres_config()
    conn = None

    try:
        conn = psycopg2.connect(**db_config)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # === 1. Получаем actor_id из сообщения ===
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

            # === 3. Получаем ID типа задачи ===
            cur.execute(
                "SELECT id FROM orchestrator.task_types WHERE type_name = %s",
                ("user_answer_generation",)
            )
            type_row = cur.fetchone()
        
            if not type_row:
                raise RuntimeError(
                    "Task type 'user_answer_generation' not found in orchestrator.task_types. "
                    "Ensure V001 migration was applied."
                )

            generation_type_id = type_row["id"]

            # === 4. Создаём задачу ГЕНЕРАЦИИ ОТВЕТА ===
            cur.execute("""
                INSERT INTO orchestrator.orchestrator_tasks (
                    task_type_id,
                    input_data,
                    priority,
                    status,
                    agent_version,
                    created_at
                ) VALUES (
                    %(task_type_id)s,
                    %(input_data)s,
                    %(priority)s,
                    'pending',
                    %(agent_version)s,
                    NOW()
                )
                RETURNING id
            """, {
                "task_type_id": generation_type_id,
                "input_data": Json({"message_id": message_id}),
                "priority": 0.8,
                "agent_version": agent_version
            })
            generation_task_id = str(cur.fetchone()["id"])

            conn.commit()

            logger.info(
                f"Task created: generation={generation_task_id[:8]} (prio=0.8), "
                f"activity recorded for actor {actor_id[:8]}"
            )
            return generation_task_id

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"Database error while creating orchestrator tasks: {e}", exc_info=True)
        raise
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Unexpected error in orchestrator_entry: {e}", exc_info=True)
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