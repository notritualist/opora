"""
main-srv/src/dialog_services/dialogue_manager.py

Модуль управления диалогами, работающий напрямую с БД (без in-memory состояния).
Отвечает за:
- Создание новых диалогов и закрытие существующих.
- Закрытие диалогов по таймауту (вызывается оркестратором).
- Закрытие диалогов для активных сессий при старте системы.
Все функции работают напрямую с базой данных через psycopg2.
"""

version = "1.1.0"
description = "Module for dialog management with DB-configurable timeout"

import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

def _get_dialogue_timeout_minutes(cur) -> float:
    """
    Загружает таймаут неактивности диалога из state.settings.
    
    Args:
        cur: psycopg2 cursor
    
    Returns:
        float: таймаут в минутах (по умолчанию 30.0)
    """
    cur.execute("""
        SELECT value_float FROM state.settings 
        WHERE param_name = 'dialogue_inactivity_timeout_minutes'
    """)
    row = cur.fetchone()
    if not row or row['value_float'] is None:
        logger.warning("Missing 'dialogue_inactivity_timeout_minutes' in settings, using default 30.0")
        return 30.0
    return float(row['value_float'])

def check_dialogue_timeouts(db_config: dict) -> int:
    """
    Закрывает все диалоги, у которых last_activity_at старше таймаута.
    Вызывается оркестратором на каждом пульсе.
    """
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        timeout_minutes = _get_dialogue_timeout_minutes(cur)
        
        # 1. Находим диалоги, которые уйдут в таймаут
        cur.execute("""
            SELECT id, actor_id FROM dialogs.dialogues
            WHERE status = 'active'
              AND last_activity_at < NOW() - (%s * INTERVAL '1 minute')
        """, (timeout_minutes,))
        
        doomed_dialogues = cur.fetchall()
        if not doomed_dialogues:
            return 0
            
        # 2. Закрываем диалоги одним UPDATE
        dialogue_ids = [str(d['id']) for d in doomed_dialogues]
        cur.execute("""
            UPDATE dialogs.dialogues
            SET 
                status = 'completed',
                reason = 'inactivity_timeout'::dialog_close_reason,
                end_at = NOW()
            WHERE id = ANY(%s::uuid[])
        """, (dialogue_ids,))
        
        count = cur.rowcount
        conn.commit()
        
        if count > 0:
            logger.debug(f"Closed {count} dialogue(s) due to inactivity timeout ({timeout_minutes} min)")
        
        return count

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def ensure_active_dialogue(
    db_config: dict,
    session_id: str,
    actor_id: str,
    agent_version: str
) -> str: 
    """
    Возвращает ID активного диалога или создаёт новый.
    ВАЖНО: Не проверяет таймаут! Таймауты обрабатываются оркестратором.
    Логика:
    1. Ищет активный диалог для session_id + actor_id.
    2. Если найден → обновляет last_activity_at, возвращает ID.
    3. Если не найден → создаёт новый, возвращает ID.
    """
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        cur.execute("""
            SELECT id, last_activity_at 
            FROM dialogs.dialogues 
            WHERE actor_id = %s AND session_id = %s AND status = 'active'
            ORDER BY start_at DESC LIMIT 1
        """, (actor_id, session_id))
        
        active_dialogue = cur.fetchone()
        now = datetime.now(timezone.utc)
        dialogue_id = None
        
        if active_dialogue:
            # Диалог активен (оркестратор гарантировал, что он не просрочен)
            dialogue_id = str(active_dialogue['id'])
            cur.execute(
                "UPDATE dialogs.dialogues SET last_activity_at = %s WHERE id = %s",
                (now, dialogue_id)
            )
        else:
            # Активного диалога нет → создаём новый
            logger.debug("No active dialogue found. Creating new one.")
            dialogue_id = _create_dialogue(cur, session_id, actor_id, agent_version)
        
        conn.commit()
        return dialogue_id

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def close_active_dialogue(db_config: dict, session_id: str, actor_id: str, reason: str):
    """
    Закрывает текущий активный диалог с указанной причиной.
    """
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT id FROM dialogs.dialogues
            WHERE actor_id = %s AND session_id = %s AND status = 'active'
            ORDER BY start_at DESC LIMIT 1
        """, (actor_id, session_id))
        row = cur.fetchone()
        
        if row:
            dialogue_id = str(row['id'])
            
            _close_dialogue(cur, dialogue_id, reason)
            logger.info(f"Active dialogue {dialogue_id[:8]} closed with reason: {reason}")
            conn.commit()
        else:
            logger.debug("No active dialogue to close for this session/actor.")
            
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def close_dangling_dialogues(db_config: dict) -> int:
    """
    Завершает все зависшие активные диалоги при перезапуске системы.
    """
    logger.info("Checking for dangling dialogues...")
    conn = psycopg2.connect(**db_config)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE dialogs.dialogues
                SET status = 'completed', reason = 'system_restart'::dialog_close_reason, end_at = NOW()
                WHERE status = 'active'
                AND session_id IN (SELECT id FROM dialogs.sessions WHERE status = 'active')
            """)
            count = cur.rowcount
            conn.commit()
            if count > 0:
                logger.warning(f"Closed {count} dangling dialogues on startup.")
            return count
    except Exception as e:
        logger.error(f"Error closing dangling dialogues: {e}", exc_info=True)
        conn.rollback()
        return 0
    finally:
        conn.close()

def _create_dialogue(cur, session_id: str, actor_id: str, agent_version: str) -> str:
    cur.execute("""
        INSERT INTO dialogs.dialogues (session_id, actor_id, agent_version)
        VALUES (%s, %s, %s)
        RETURNING id
    """, (session_id, actor_id, agent_version))
    new_id = str(cur.fetchone()['id'])
    logger.debug(f"New dialogue created: {new_id[:8]}")
    return new_id

def _close_dialogue(cur, dialogue_id: str, reason: str):
    cur.execute("""
        UPDATE dialogs.dialogues
        SET status = 'completed', reason = %s::dialog_close_reason, end_at = NOW()
        WHERE id = %s
    """, (reason, dialogue_id))