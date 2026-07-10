"""
main-srv/src/memory_service/verification_service.py

Модуль управления сессиями и действиями верификации гипотез.

Обязанности:
- Получение draft-гипотез с пагинацией и сортировкой.
- Управление сессиями верификации (создание, завершение, отложение).
- Запись действий пользователя (confirm / reject / edit / skip) в memory.verification_actions.
- Обновление статусов гипотез в memory.hypotheses.
- Получение контекста источников (сообщений диалога) для отображения в CLI.
- Проверка возможности предложения верификации (с учётом deferred_until).
- Закрытие задач оркестратора verification_proposal после ответа пользователя.

Интеграция:
Используется консольным интерфейсом (console_interface.py) для интерактивного разбора.
Связывает memory.hypotheses с memory.verification_sessions и orchestrator.orchestrator_steps.
"""

version = "1.1.1"
description = "Hypothesis verification session management"

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


# =============================================================================
# === ПОЛУЧЕНИЕ DRAFT-ГИПОТЕЗ ================================================
# =============================================================================
def get_unverified_hypotheses(db_config: dict, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
    """Возвращает гипотезы со статусом 'needs_clarification', готовые к верификации."""
    conn = psycopg2.connect(**db_config)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT h.id, h.hypothesis_text, h.source_message_ids, h.domain_code,
                       h.topic_id, t.name AS topic_name, h.knowledge_source, 
                       h.confidence, h.status, h.created_at,
                       h.dialogue_id  -- НОВОЕ
                FROM memory.hypotheses h
                LEFT JOIN memory.topics t ON t.id = h.topic_id
                WHERE h.status = 'needs_clarification'::memory.hypothesis_status
                ORDER BY h.confidence DESC, h.created_at ASC
                LIMIT %s OFFSET %s
            """, (limit, offset))
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def get_draft_count(db_config: dict, actor_id: Optional[str] = None) -> int:
    """Возвращает количество draft-гипотез."""
    conn = psycopg2.connect(**db_config)
    try:
        with conn.cursor() as cur:
            query = """
                SELECT COUNT(*) FROM memory.hypotheses
                WHERE status = 'needs_clarification'::memory.hypothesis_status
            """
            cur.execute(query)
            return cur.fetchone()[0]
    finally:
        conn.close()


# =============================================================================
# === КОНТЕКСТ ИСТОЧНИКОВ ====================================================
# =============================================================================
def get_source_context(
    db_config: dict,
    source_message_ids: List[str],
    context_window: int = 3
) -> List[Dict[str, Any]]:
    """
    Получает сообщения-источники гипотезы и окружающий контекст.
    
    Таблица: dialogs.row_messages
    Колонки: id, actor_id, actor_type, row_text, timestamp, dialogue_id
    
    Args:
        source_message_ids: ID сообщений, из которых извлечена гипотеза
        context_window: Сколько сообщений до и после каждого источника включить
    
    Returns:
        Список сообщений с флагом is_source
    """
    if not source_message_ids:
        return []
    
    conn = psycopg2.connect(**db_config)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 1. Получаем сами сообщения-источники (без ordinal пока)
            cur.execute("""
                SELECT
                    rm.id,
                    rm.dialogue_id,
                    rm.actor_id,
                    rm.actor_type,
                    rm.row_text,
                    rm.timestamp
                FROM dialogs.row_messages rm
                WHERE rm.id = ANY(%s::uuid[])
                ORDER BY rm.timestamp ASC
            """, (source_message_ids,))
            source_msgs = [dict(r) for r in cur.fetchall()]
            
            if not source_msgs:
                return []
            
            # 2. Получаем ВСЕ сообщения из диалогов источников с правильным ordinal
            dialogue_ids = list({str(m['dialogue_id']) for m in source_msgs})
            
            context_msgs = []
            for did in dialogue_ids:
                # Получаем ВСЕ сообщения диалога с ordinal
                cur.execute("""
                    SELECT
                        rm.id,
                        rm.dialogue_id,
                        rm.actor_id,
                        rm.actor_type,
                        rm.row_text,
                        rm.timestamp,
                        ROW_NUMBER() OVER (ORDER BY rm.timestamp ASC) as ordinal
                    FROM dialogs.row_messages rm
                    WHERE rm.dialogue_id = %s::uuid
                    ORDER BY rm.timestamp ASC
                """, (did,))
                
                all_dialog_msgs = [dict(r) for r in cur.fetchall()]
                
                # Находим ordinal'ы source-сообщений в этом диалоге
                source_ordinals = []
                for m in all_dialog_msgs:
                    if str(m['id']) in source_message_ids:
                        source_ordinals.append(m['ordinal'])
                
                if not source_ordinals:
                    continue
                
                # Определяем диапазон контекста
                min_ord = min(source_ordinals) - context_window
                max_ord = max(source_ordinals) + context_window
                
                # Фильтруем сообщения в диапазоне
                for m in all_dialog_msgs:
                    if min_ord <= m['ordinal'] <= max_ord:
                        m['is_source'] = str(m['id']) in source_message_ids
                        context_msgs.append(m)
            
            # 3. Сортируем по времени
            context_msgs.sort(key=lambda x: x['timestamp'])
            return context_msgs
    finally:
        conn.close()

# =============================================================================
# === УПРАВЛЕНИЕ СЕССИЯМИ ====================================================
# =============================================================================
def create_session(
    db_config: dict, 
    total: int,
    proposal_task_id: Optional[str] = None,
    hypothesis_ids: Optional[List[str]] = None
) -> str:
    """
    Создаёт новую сессию верификации со статусом 'active'.
    Связывает с задачей оркестратора и списком hypothesis (snapshot на момент предложения).
    Сессия остаётся 'active' до ответа пользователя ([Y]/[N]).
    
    Args:
        proposal_task_id: UUID задачи verification_proposal (может быть None)
        hypothesis_ids: ID hypothesis на момент NOTIFY (snapshot)
    """
    conn = psycopg2.connect(**db_config)
    try:
        with conn.cursor() as cur:
            hypothesis_array = hypothesis_ids if hypothesis_ids else []
            cur.execute("""
                INSERT INTO memory.verification_sessions 
                (status, hypotheses_total, proposal_task_id, hypothesis_ids, started_at) 
                VALUES (
                    'active'::memory.verification_session_status, 
                    %s,
                    %s::uuid,
                    %s::uuid[],
                    now()
                ) 
                RETURNING id
            """, (total, proposal_task_id, hypothesis_array))
            session_id = str(cur.fetchone()[0])
            conn.commit()
            logger.info(
                "Verification session created: %s (total=%d, task=%s, hypotheses=%d)",
                session_id[:8], total,
                proposal_task_id[:8] if proposal_task_id else 'None',
                len(hypothesis_array)
            )
            return session_id
    finally:
        conn.close()


def complete_session(db_config: dict, session_id: str) -> None:
    """Завершает сессию верификации."""
    conn = psycopg2.connect(**db_config)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE memory.verification_sessions
                SET status = 'completed'::memory.verification_session_status,
                    ended_at = now()
                WHERE id = %s::uuid
            """, (session_id,))
            conn.commit()
            logger.info("Verification session completed: %s", session_id[:8])
    finally:
        conn.close()


def defer_session(db_config: dict, session_id: str, defer_minutes: float) -> None:
    """Откладывает сессию на N минут."""
    defer_until = datetime.now(timezone.utc) + timedelta(minutes=defer_minutes)
    conn = psycopg2.connect(**db_config)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE memory.verification_sessions
                SET status = 'deferred'::memory.verification_session_status,
                    deferred_until = %s,
                    ended_at = now()
                WHERE id = %s::uuid
            """, (defer_until, session_id))
            conn.commit()
            logger.info("Verification session deferred: %s until %s", session_id[:8], defer_until)
    finally:
        conn.close()


def can_propose_verification(db_config: dict) -> bool:
    """
    Проверяет, можно ли предложить верификацию.
    Возвращает False если есть активная сессия или отложенная сессия, чей defer_until ещё не наступил.
    """
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 1 FROM memory.verification_sessions
                    WHERE (status = 'active'::memory.verification_session_status)
                       OR (status = 'deferred'::memory.verification_session_status AND deferred_until > NOW())
                    LIMIT 1
                """)
                return cur.fetchone() is None
    except Exception as e:
        logger.warning(f"Error checking can_propose_verification: {e}")
        return True  # В случае ошибки разрешаем предложение


def get_defer_minutes(db_config: dict) -> float:
    """Получает настройку verification_defer_minutes."""
    conn = psycopg2.connect(**db_config)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT value_float FROM state.settings
                WHERE param_name = 'verification_defer_minutes'
            """)
            row = cur.fetchone()
            return float(row[0]) if row else 30.0
    finally:
        conn.close()

# =============================================================================
# === ДЕЙСТВИЯ ВЕРИФИКАЦИИ ===================================================
# =============================================================================
def record_action(
    db_config: dict,
    session_id: str,
    hypothesis_id: str,
    action_type: str,  # confirmed | rejected | edited | skipped
    actor_id: Optional[str] = None,
    original_text: Optional[str] = None,
    updated_text: Optional[str] = None,
    user_comment: Optional[str] = None,
    orchestrator_step_id: Optional[str] = None,
    prompt_id: Optional[str] = None,
    metadata_updates: Optional[Dict[str, Any]] = None,  # ← НОВОЕ
) -> str:
    """
    Записывает действие верификации и обновляет статус гипотезы.
    Возвращает ID действия.
    """
    # Маппинг action_type → новый статус гипотезы
    status_map = {
        'confirmed': 'confirmed',
        'rejected': 'refuted',
        'edited': 'confirmed',
        'skipped': None,  # Не меняем статус
    }
    new_hypothesis_status = status_map.get(action_type)
    
    conn = psycopg2.connect(**db_config)
    try:
        with conn.cursor() as cur:
            # 1. Вставляем действие
            cur.execute("""
                INSERT INTO memory.verification_actions (
                    session_id, hypothesis_id, action_type,
                    original_text, updated_text, user_comment,
                    orchestrator_step_id, prompt_id
                ) VALUES (
                    %s::uuid, %s::uuid,
                    %s::memory.verification_action_type,
                    %s, %s, %s, %s::uuid, %s::uuid
                )
                RETURNING id
            """, (
                session_id, hypothesis_id, action_type,
                original_text, updated_text, user_comment,
                orchestrator_step_id, prompt_id,
            ))
            action_id = str(cur.fetchone()[0])
            
            # 2. Обновляем статус гипотезы и метаданные верификации
            if new_hypothesis_status:
                if action_type == 'edited' and updated_text:
                    cur.execute("""
                        UPDATE memory.hypotheses
                        SET status = %s::memory.hypothesis_status,
                            hypothesis_text = %s,
                            updated_at = NOW(),
                            verified_at = NOW(),
                            verified_by_actor_id = %s::uuid
                        WHERE id = %s::uuid
                    """, (new_hypothesis_status, updated_text, actor_id, hypothesis_id))
                else:
                    cur.execute("""
                        UPDATE memory.hypotheses
                        SET status = %s::memory.hypothesis_status,
                            updated_at = NOW(),
                            verified_at = NOW(),
                            verified_by_actor_id = %s::uuid
                        WHERE id = %s::uuid
                    """, (new_hypothesis_status, actor_id, hypothesis_id))
                
                # === НОВОЕ: Обновляем метаданные, если переданы ===
                if metadata_updates:
                    update_fields = []
                    update_params = []
                    
                    if 'domain_code' in metadata_updates:
                        update_fields.append("domain_code = %s")
                        update_params.append(metadata_updates['domain_code'])
                    
                    if 'knowledge_source' in metadata_updates:
                        update_fields.append("knowledge_source = %s")
                        update_params.append(metadata_updates['knowledge_source'])
                    
                    if 'topic_id' in metadata_updates:
                        update_fields.append("topic_id = %s::uuid")
                        update_params.append(metadata_updates['topic_id'])
                    
                    if update_fields:
                        update_params.append(hypothesis_id)
                        cur.execute(f"""
                            UPDATE memory.hypotheses
                            SET {', '.join(update_fields)}, updated_at = NOW()
                            WHERE id = %s::uuid
                        """, update_params)
            
            # 3. Обновляем счётчики сессии
            counter_column = f"hypotheses_{action_type}" if action_type != 'confirmed' else "hypotheses_confirmed"
            cur.execute(f"""
                UPDATE memory.verification_sessions
                SET {counter_column} = {counter_column} + 1
                WHERE id = %s::uuid
            """, (session_id,))
            
            conn.commit()
            logger.debug(
                "Verification action recorded: %s | hypothesis=%s | type=%s",
                action_id[:8], hypothesis_id[:8], action_type
            )
            return action_id
    except Exception as e:
        conn.rollback()
        logger.error("Error recording verification action: %s", e, exc_info=True)
        raise
    finally:
        conn.close()


def complete_verification_proposal_task(
    db_config: dict, 
    task_id: str, 
    user_choice: str,
    output_extra: Optional[Dict[str, Any]] = None
) -> None:
    """
    Закрывает задачу verification_proposal после ответа пользователя в консоли.
    
    Задача остаётся в статусе 'running' после отправки NOTIFY оркестратором.
    Консоль вызывает эту функцию после выбора [Y] (верифицировать) или 
    [N] (отложить), переводя задачу в 'completed' с указанием выбора.
    
    Пока задача 'running', оркестратор не создаёт новых verification_proposal
    (см. проверку has_active_proposal в orchestrator.py).
    
    Args:
        db_config: параметры подключения к PostgreSQL
        task_id: UUID задачи оркестратора
        user_choice: выбор пользователя ('verify_now' | 'deferred')
        output_extra: дополнительные данные для output_data (session_id, defer_min и т.д.)
    """
    if not task_id:
        logger.warning("complete_verification_proposal_task: task_id is empty, skipping")
        return
    
    # output без timestamp (у задачи уже есть created_at, started_at, completed_at в столбцах)
    # добавляем привязку к verification session, чтобы метрика показывала всю связку:
    # orchestrator_tasks ↔ memory.verification_sessions ↔ memory.verification_actions
    output = {
        "user_choice": user_choice,
        "completed_by": "console_interface"
    }
    if output_extra:
        output.update(output_extra)
    
    try:
        import json
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE orchestrator.orchestrator_tasks
                    SET status = 'completed'::task_status,
                        output_data = %s::jsonb,
                        completed_at = NOW(),
                        total_latency = EXTRACT(EPOCH FROM (NOW() - created_at)),
                        run_latency = EXTRACT(EPOCH FROM (NOW() - started_at))
                    WHERE id = %s
                """, (json.dumps(output), task_id))
                conn.commit()
        logger.info(
            "Verification proposal task %s completed (choice=%s)",
            task_id[:8], user_choice
        )
    except Exception as e:
        logger.error(
            "Failed to complete verification proposal task %s: %s",
            task_id[:8] if task_id else "None", e, exc_info=True
        )


def close_dangling_verification_sessions(db_config: dict) -> int:
    """
    Завершает все зависшие сессии верификации (active/deferred) при перезапуске системы.
    Вызывается из main.py при старте агента.
    
    Аналог SessionManager.close_dangling_sessions и close_dangling_dialogues.
    """
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE memory.verification_sessions
                    SET status = 'completed'::memory.verification_session_status,
                        ended_at = NOW()
                    WHERE status IN (
                        'active'::memory.verification_session_status,
                        'deferred'::memory.verification_session_status
                    )
                """)
                count = cur.rowcount
                conn.commit()
                
                if count > 0:
                    logger.warning(f"Closed {count} dangling verification session(s) on startup")
                return count
    except Exception as e:
        logger.error(f"Error closing dangling verification sessions: {e}", exc_info=True)
        return 0