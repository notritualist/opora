"""
main-srv/src/orchestrator/orchestrator_entry.py

Входной интерфейс оркестратора.
Единая точка входа для создания задач оркестратора.
Все модули (session_manager, lifecycle_manager, memory_service) должны использовать этот интерфейс вместо прямого SQL INSERT.
Поддерживаемые типы задач:
- user_answer_generation: генерация финального ответа пользователю.
- memory_extraction: извлечение гипотез из закрытых диалогов в долговременную память.
Архитектура:
- Универсальная функция create_orchestrator_task() с валидацией task_type.
- Специализированные обёртки: on_user_message, schedule_memory_extraction.
Интеграция с жизненным циклом:
- Активность фиксируется с actor_id из сообщения.
- Агент-версия передаётся глобально через version.py.
"""

__version__ = "1.1.0"
__description__ = "Entry point for orchestrator"

import logging
import psycopg2
from typing import Optional, Dict, Any
from psycopg2.extras import RealDictCursor, Json
from services.lifecycle_manager  import LifecycleManager
from db_manager.db_manager import load_postgres_config
# Глобальная версия проекта (из pyproject.toml через version.py)
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
    actor_id: Optional[str] = None,
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
        actor_id: если указан — анализировать только этого пользователя
        message_ids: если указан — конкретные сообщения (иначе автовыборка из БД)
        priority: приоритет задачи (по умолчанию 0.3 — ниже ответа пользователю)
        
    Returns:
        str: UUID созданной задачи
        
    Raises:
        RuntimeError: если тип задачи не найден или ошибка БД
    """
    input_data: Dict[str, Any] = {"mode": "auto"}
    if actor_id:
        input_data["actor_id"] = actor_id
    if message_ids:
        input_data["message_ids"] = message_ids
        input_data["mode"] = "explicit"
    
    return create_orchestrator_task(
        task_type_name="memory_extraction",
        input_data=input_data,
        priority=priority
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