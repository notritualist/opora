"""
main-srv/src/session_services/session_manager.py

Менеджер сессий и диалогов для интерфейсов агента.
Отвечает за:
- Привязку пользователей (console:<username>) к акторам (owner/user).
- Создание физических сессий (dialogs.sessions).
- Управление жизненным циклом диалогов через dialog_services.
- Сохранение сообщений в dialogs.row_messages с привязкой к dialogue_id.
- Корректное завершение диалогов и сессий при выходе.
- Очистку зависших сессий при рестарте.
Таблицы БД: users.actors, users.actors_external_ids, dialogs.sessions,
dialogs.dialogues, dialogs.row_messages
"""

version = "1.1.0"
description = "Session and dialog manager for the agent console interface"

import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional
from datetime import datetime, timezone
from dialog_services.dialogue_manager import ensure_active_dialogue, close_active_dialogue

# Логгер модуля — подхватит настройки из main.py
logger = logging.getLogger(__name__)


class SessionManager:
    """
    Менеджер сессий и диалогов для интерфейсов.
    Принцип работы:
    - Каждый запуск консоли = новая физическая сессия (dialogs.sessions)
    - Логический диалог (dialogs.dialogues) создаётся лениво при первом сообщении
    - Таймаут неактивности и ручное завершение диалога (Ctrl+N) обрабатываются прозрачно
    - Все сообщения пишутся в dialogs.row_messages с обязательным dialogue_id
    
    Атрибуты:
        db_config (dict): параметры подключения к PostgreSQL
        agent_version (str): версия агента из pyproject.toml
        console_user_id (str): идентификатор в формате "console:<username>"
        session_id (Optional[str]): UUID текущей физической сессии
        actor_id (Optional[str]): UUID текущего актора (owner или user)
        actor_type (str): Тип актора: 'owner' или 'user'
        current_dialogue_id (Optional[str]): UUID текущего активного диалога
        _conn: кэш соединения с БД
    """

    def __init__(self, db_config: dict, agent_version: str, console_user_id: str):
        """
        Инициализация менеджера сессий и диалогов.
        
        Args:
            db_config: dict с параметрами подключения (host, port, dbname, user, password)
            agent_version: строка версии из pyproject.toml
            console_user_id: идентификатор пользователя, например "console:debian"
        """
        self.db_config = db_config
        self.agent_version = agent_version
        self.console_user_id = console_user_id
        
        # Поля заполняются в процессе работы
        self.session_id: Optional[str] = None
        self.actor_id: Optional[str] = None      # UUID актора (owner или user)
        self.actor_type: str = 'owner'           # Тип: 'owner' или 'user'
        self.actor_external_id: Optional[str] = None  # кэш внешнего ID
        self.current_dialogue_id: Optional[str] = None # Кэш ID текущего диалога
        self._conn = None
            
        logger.debug(f"SessionManager created for {console_user_id}")

    def _get_conn(self):
        """Возвращает активное соединение с БД, создавая при необходимости."""
        if self._conn is None or self._conn.closed:
            logger.debug("Opening PostgreSQL connection")
            try:
                self._conn = psycopg2.connect(**self.db_config)
                logger.debug("Database connection opened successfully")
            except psycopg2.Error as e:
                logger.error(f"PostgreSQL connection error: {e}", exc_info=True)
                raise
        return self._conn

    def _query(self, sql: str, params: Optional[tuple] = None, fetch: bool = False):
        """
        Выполняет SQL-запрос с авто-коммитом.
        
        ВАЖНО: commit() вызывается ДО return, чтобы данные сразу попадали в БД.
        """
        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                logger.debug(f"SQL: {sql[:100]}... Params: {params}")
                cur.execute(sql, params or ()) # ← params or () гарантирует, что в execute попадёт tuple
                result = cur.fetchone() if fetch else None
                conn.commit()
                logger.debug("Query executed successfully")
                return result
        except psycopg2.Error as e:
            conn.rollback()
            logger.error(
                f"Database error: {e}\nSQL: {sql}\nParams: {params}", 
                exc_info=True
            )
            raise
        except Exception as e:
            conn.rollback()
            logger.error(
                f"Unexpected error during query execution: {e}\nSQL: {sql}", 
                exc_info=True
            )
            raise

    def ensure_actor_linked(self) -> bool:
        """
        Привязывает текущего пользователя консоли к актору.
        Логика:
        - Сначала проверяем, есть ли уже привязка у этого console_user_id → если да, возвращаем False
        - Если нет → проверяем, занят ли owner ДРУГИМ console-юзером
        - Если owner свободен → привязываем к owner
        - Если owner занят → создаём нового актора type='user' и привязываем к нему 
        
        Returns:
            bool: True, если привязка создана сейчас; False, если уже была
        """
        logger.info(f"Checking actor binding for {self.console_user_id}")
        
        try:
            # === ШАГ 1: ПРОВЕРЯЕМ, ЕСТЬ ЛИ УЖЕ ПРИВЯЗКА У ЭТОГО ПОЛЬЗОВАТЕЛЯ ===
            existing = self._query("""
                SELECT aei.id, aei.actor_id, a.type
                FROM users.actors_external_ids aei
                JOIN users.actors a ON aei.actor_id = a.id
                WHERE aei.source = 'console'::external_source 
                AND aei.source_id = %s
            """, params=(self.console_user_id,), fetch=True)
            
            if existing:
                self.actor_id = str(existing['actor_id'])
                self.actor_type = str(existing['type'])
                self.actor_external_id = str(existing['id'])
                logger.info(
                    f"{self.console_user_id} already bound to {self.actor_type}#{self.actor_id[:8]}, "
                    f"external_id={self.actor_external_id[:8]}"
                )
                return False
            
            # === ШАГ 2: Пользователь новый — определяем, к кому привязывать ===
            existing_owner = self._query("""
                SELECT aei.source_id, aei.actor_id
                FROM users.actors_external_ids aei
                JOIN users.actors a ON aei.actor_id = a.id
                WHERE a.type = 'owner'::actor_type 
                AND aei.source = 'console'::external_source
                AND aei.source_id != %s
                LIMIT 1
            """, params=(self.console_user_id,), fetch=True)
            
            if existing_owner:
                logger.info(f"Owner already taken by {existing_owner['source_id']}. Creating user for {self.console_user_id}")
                
                new_actor = self._query("""
                    INSERT INTO users.actors (type, metadata, access, verified, agent_version)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                """, params=('user', '{}', True, True, self.agent_version), fetch=True)
                
                if not new_actor:  # ← Явная проверка на None
                    logger.error("Failed to create new actor (no RETURNING id)")
                    raise RuntimeError("Failed to create actor")
                
                self.actor_id = str(new_actor['id'])
                self.actor_type = 'user'
            
            else:
                owner_row = self._query("""
                    SELECT id FROM users.actors 
                    WHERE type = 'owner'::actor_type 
                    ORDER BY created_at ASC 
                    LIMIT 1
                """, fetch=True)
                
                if not owner_row:
                    logger.critical("Actor 'owner' not found in database")
                    raise RuntimeError("Owner actor not found")
                
                self.actor_id = str(owner_row['id'])
                self.actor_type = 'owner'
            
            # === ШАГ 3: Создаём привязку внешнего ID ===
            ext_row = self._query("""
                INSERT INTO users.actors_external_ids 
                (actor_id, source, source_id, authorized, agent_version)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, params=(
                self.actor_id,
                'console',
                self.console_user_id,
                True,
                self.agent_version
            ), fetch=True)
            
            if ext_row:
                self.actor_external_id = str(ext_row['id'])
                logger.debug(f"actor_external_id saved: {self.actor_external_id[:8]}")
            
            logger.info(f"Binding created: {self.actor_type}#{self.actor_id[:8]} ↔ {self.console_user_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error during actor binding:  {e}", exc_info=True)
            raise

    @staticmethod
    def close_dangling_sessions(db_config: dict) -> int:
        """
        Завершает зависшие активные сессии и диалоги при перезапуске системы.
        Вызывается из main.py перед стартом интерфейса.
        Сначала закрывает диалоги (reason='system_restart'), затем сессии.
        
        Args:
            db_config: параметры подключения к PostgreSQL
        Returns:
            int: количество закрытых сессий
        """
        from dialog_services.dialogue_manager import close_dangling_dialogues
        # Сначала закрываем зависшие диалоги, затем сессии
        close_dangling_dialogues(db_config)
        
        logger.info("Checking for dangling sessions...")
        try:
            with psycopg2.connect(**db_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE dialogs.sessions
                        SET status = 'completed'::session_status, closed_at = NOW(), updated_at = NOW(), 
                            reason = 'system_restart'::session_close_reason
                        WHERE status = 'active'
                    """)
                    count = cur.rowcount
                    conn.commit()
                    if count > 0:
                        logger.warning(f"Closed {count} dangling sessions on startup")
                    return count
        except Exception as e:
            logger.error(f"Error closing dangling sessions: {e}", exc_info=True)
            return 0        

    def _calculate_sleep_duration(self) -> Optional[str]:
        """
        Вычисляет длительность простоя с момента завершения последней сессии актора.
        
        Returns:
            str | None: интервал в формате PostgreSQL INTERVAL или None
        """
        if not self.actor_id:
            return None
        
        try:
            row = self._query("""
                SELECT closed_at
                FROM dialogs.sessions
                WHERE actor_id = %s AND status = 'completed'
                ORDER BY closed_at DESC
                LIMIT 1
            """, params=(self.actor_id,), fetch=True)
            
            if not row or row['closed_at'] is None:
                return None
            
            # PostgreSQL интервал вычисляется автоматически при UPDATE
            return "NOW() - %s::timestamptz"  # Не работает через параметр
            # Лучше вернуть timestamp и вычислить в SQL
        except Exception:
            return None
        
    def create_session(self) -> str:
        """
        Создаёт новую физическую сессию в БД.
    
        Логика:
        1. Читаем closed_at последней сессии для sleep_duration.
        2. Вставляем запись в dialogs.sessions.
        
        Returns:
            str: UUID новой сессии.
            
        Raises:
            RuntimeError: если сессия уже активна или actor_id не задан.
        """
        if self.session_id:
            logger.warning(f"Session already active: {self.session_id[:8]}")
            raise RuntimeError("Session already active")
        if not self.actor_id:
            logger.error("Actor_id not set. Call ensure_actor_linked() first")
            raise RuntimeError("Actor_id not set")
        
        logger.info("Creating new session with momentary slice...")
        
        # Вычисляем sleep_duration
        last_closed = self._query("""
            SELECT closed_at FROM dialogs.sessions
            WHERE actor_id = %s AND status = 'completed'
            ORDER BY closed_at DESC LIMIT 1
        """, params=(self.actor_id,), fetch=True)
        
        sleep_duration_expr = "NULL"
        sleep_params = []
        if last_closed and last_closed['closed_at']:
            sleep_duration_expr = "NOW() - %s::timestamptz"
            sleep_params = [last_closed['closed_at']]
        
        conn = self._get_conn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 1. Вставляем сессию
                cur.execute(f"""
                    INSERT INTO dialogs.sessions (
                        actor_id, actor_external_id, status, agent_version, sleep_duration
                    ) VALUES (
                        %s, %s, 'active', %s, {sleep_duration_expr}
                    ) RETURNING id
                """, (self.actor_id, self.actor_external_id, self.agent_version, *sleep_params))
                session_row = cur.fetchone()
                if not session_row:
                    raise RuntimeError("Failed to create session in DB")
                session_id = str(session_row['id'])
                
                conn.commit()
                
            self.session_id = session_id
            logger.info(f"Session created: {session_id[:8]}")
            return session_id
           
        except Exception as e:
            conn.rollback()
            self.session_id = None
            logger.error(f"Error creating session: {e}", exc_info=True)
            raise

    def rotate_dialogue(self, reason: str = 'user_new_dialogue'):
        """
        Вручную завершает текущий активный диалог и сбрасывает кэш.
        Вызывается при Ctrl+N из интерфейса.
        Диалог закрывается с указанной причиной. Следующее сообщение создаст новый.
        
        Args:
            reason: причина закрытия (должна соответствовать dialog_close_reason ENUM)
        """
        if not self.session_id or not self.actor_id:
            logger.warning("Cannot rotate dialogue: no active session/actor")
            return
            
        logger.info(f"Rotating dialogue. Reason: {reason}")
               
        close_active_dialogue(self.db_config, self.session_id, self.actor_id, reason)
        self.current_dialogue_id = None

    def save_message(self, content: str) -> str:
        """
        Сохраняет сообщение пользователя в dialogs.row_messages.
        Автоматически обеспечивает наличие активного диалога.
               
        Заполняет поля согласно миграциям БД:
        - actor_id, actor_type (из self.actor_type: 'owner' или 'user')
        - session_id
        - dialogue_id (автоматически через ensure_active_dialogue)
        - row_text (сырой текст)
        - agent_version, timestamp
               
        Args:
            content: текст сообщения
            
        Returns:
            str: UUID сохранённого сообщения
            
        Raises:
            RuntimeError: если сессия не создана или actor_id не задан
        """
        if not self.session_id:
            logger.error("Session not created. Call create_session() first")
            raise RuntimeError("Session not created. Call create_session() first")
        if not self.actor_id:
            logger.error("Actor_id not set")
            raise RuntimeError("Actor_id not set")
        
        logger.debug(f"Saving message: {len(content)} characters")
        
        try:
            # V002: === ШАГ 0: ГАРАНТИРУЕМ АКТИВНЫЙ ДИАЛОГ (ПРОВЕРКА ТАЙМАУТА) ===
            # Функция вернет ID существующего диалога или создаст новый, если истек таймаут
            self.current_dialogue_id = ensure_active_dialogue(
                self.db_config, self.session_id, self.actor_id, self.agent_version
            )

            # === ШАГ 1: Вычисляем parent_message_id и user_think_latency ===
            parent_message_id: Optional[str] = None
            user_think_latency: Optional[float] = None

            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT m.id, m.timestamp
                        FROM dialogs.row_messages m
                        WHERE m.session_id = %s
                            AND (
                                 (m.actor_type = 'system' 
                                AND m.parent_message_id IN (
                                    SELECT id FROM dialogs.row_messages 
                                     WHERE session_id = %s AND actor_id = %s
                                )
                                )
                                OR (m.actor_id = %s AND m.actor_type != 'system')
                            )
                        ORDER BY m.timestamp DESC
                        LIMIT 1
                    """, (self.session_id, self.session_id, self.actor_id, self.actor_id))
                    
                    prev_row = cur.fetchone()
                    if prev_row:
                        parent_message_id = str(prev_row['id'])
                        prev_timestamp = prev_row['timestamp']
                        current_timestamp = datetime.now(timezone.utc)
                        user_think_latency = (current_timestamp - prev_timestamp).total_seconds()
                        logger.debug(
                            f"parent_message_id: {parent_message_id[:8]},  "
                            f"user_think_latency: {user_think_latency:.2f} sec"
                        )
            
            # === ШАГ 2: Вставляем сообщение с dialogue_id ===
            row = self._query("""
                INSERT INTO dialogs.row_messages 
                (
                    parent_message_id,
                    actor_id, 
                    actor_type, 
                    session_id, 
                    dialogue_id,  
                    row_text,
                    answer_latency,
                    agent_version, 
                    orchestrator_step_id,
                    timestamp
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, params=(
                parent_message_id,
                self.actor_id,
                self.actor_type,
                self.session_id,
                self.current_dialogue_id,
                content,
                user_think_latency,
                self.agent_version,
                None,                  # orchestrator_step_id
                datetime.now(timezone.utc),
            ), fetch=True)
            
            if not row:
                logger.error("Failed to save message (no RETURNING id)")
                raise RuntimeError("Failed to save message (no RETURNING id)")
            
            msg_id = str(row['id'])
            logger.debug(f"Message saved: {msg_id[:8]}")

            return msg_id
            
        except ValueError as e:
            logger.error(f"Validation error: {e}")
            raise
        except Exception as e:
            logger.error(f"Error saving message: {e}", exc_info=True)
            raise

    def update_activity(self):
        """Обновляет updated_at текущей сессии."""
        if not self.session_id:
            logger.debug("No active session to update activity")
            return
        try:
            self._query("""
                UPDATE dialogs.sessions SET updated_at = NOW() WHERE id = %s
            """, params=(self.session_id,))
            logger.debug(f"Activity updated for session {self.session_id[:8]}")
        except Exception as e:
            logger.error(f"Error updating activity: {e}", exc_info=True)
            raise

    def close_session(self, reason: str = "unknown"):
        """
        Завершает сессию и активный диалог: status='completed', closed_at=NOW(), reason=?.
        
        Args:
            reason: причина завершения (должна соответствовать ENUM session_close_reason в БД)
        """
        if not self.session_id:
            logger.debug("No active session to close")
            return
        
        logger.info(f"Closing session {self.session_id[:8]} with reason: {reason}")
        try:
            # Перед закрытием сессии обязательно закрываем активный диалог
            if self.actor_id:
                close_active_dialogue(self.db_config, self.session_id, self.actor_id, 'session_end')

            self._query("""
                UPDATE dialogs.sessions 
                SET status = 'completed'::session_status, 
                    closed_at = NOW(),
                    reason = %s::session_close_reason
                WHERE id = %s
            """, params=(reason, self.session_id,))
            logger.info(f"Session {self.session_id[:8]} closed successfully")
            self.session_id = None
        except Exception as e:
            logger.error(f"Error closing session: {e}", exc_info=True)
            raise

    def cleanup(self):
        """Закрывает соединение с БД."""
        if self._conn and not self._conn.closed:
            try:
                self._conn.close()
                logger.debug("Database connection closed")
            except Exception as e:
                logger.error(f"Error closing connection: {e}")
            finally:
                self._conn = None
        else:
            logger.debug("Database connection already closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.session_id:
                self.close_session()
        except Exception as e:
            logger.error(f"Error closing session in context manager: {e}")
        finally:
            self.cleanup()
        return False

    def wait_for_agent_response(self, user_message_id: str, timeout_seconds: int = 120) -> str:
        """
        Блокирующее ожидание появления ответа агента в БД.
        
        Проверяет dialogs.row_messages на появление сообщения с:
        - parent_message_id = user_message_id
        - actor_type = 'system'
        
        Args:
            user_message_id (str): ID сообщения пользователя
            timeout_seconds (int): Максимальное время ожидания (сек)
            
        Returns:
            str: Чистый текст ответа агента (без <think/COT>)
            
        Raises:
            TimeoutError: Если ответ не появился за timeout_seconds
        """
        import time
        start_time: float = time.time()
        
        logger.debug(f"Waiting for response to message {user_message_id[:8]}...")
        
        while True:
            elapsed: float = time.time() - start_time
            if elapsed >= timeout_seconds:
                logger.error(f"Response timeout: {timeout_seconds} sec")
                raise TimeoutError(
                    f"No response received within {timeout_seconds} sec for message {user_message_id}"
                )
            
            try:
                with psycopg2.connect(**self.db_config) as conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("""
                            SELECT row_text
                            FROM dialogs.row_messages
                            WHERE parent_message_id = %s
                              AND actor_type = 'system'::actor_type
                            ORDER BY timestamp DESC
                            LIMIT 1
                        """, (user_message_id,))
                        row = cur.fetchone()
                        if row:
                            logger.debug(f"Response received: {len(row['row_text'])} characters")
                            return row["row_text"]
            except Exception as e:
                logger.warning(f"Error waiting for response: {e}")
            
            remaining: float = timeout_seconds - elapsed
            time.sleep(min(0.5, remaining))