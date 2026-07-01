"""
main-srv/src/services/lifecycle_manager.py

Глобальный менеджер жизненного цикла агента.
Принципы:
- Состояние агента (off/sleep/active) ЕДИНО для всей системы.
- Поле actor_id фиксирует ИНИЦИАТОРА изменения, но не создает отдельных состояний.
- Оркестратор является источником истины для переходов по таймауту.
- handle_startup гарантирует закрытие ЛЮБОЙ зависшей записи.
Обязанности:
- Старт и остановка состояний lifecycle
- Обработка штатного старта и восстановления после креша
- Фиксация причин выключения (shutdown_reasons)
- Пробуждение из сна
- Штатное выключение с переходом в состояние 'off'.
Таблицы БД: state.agent_lifecycle, state.shutdown_reasons
"""
version = "1.1.0"
description = "Global agent lifecycle manager"

import logging
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional
from version import __version__ as agent_version

logger = logging.getLogger(__name__)

class LifecycleManager:
    """
    Управляет глобальным жизненным циклом агента (off/sleep/active).
    Все состояния хранятся в БД (схема `state`), кэширования нет.
    """
    def __init__(self, db_config: dict):
        """
        Инициализация менеджера.
        Args:
            db_config (dict): параметры подключения к PostgreSQL
        """
        self.db_config = db_config

    # =========================================================================
    # ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ (ГЛОБАЛЬНЫЕ)
    # =========================================================================
    def _get_global_lifecycle(self) -> dict | None:
        """
        Возвращает текущее активное глобальное состояние.
        ВАЖНО: Не фильтрует по actor_id — состояние едино для всех.
        """
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, state_type, started_at, updated_at, actor_id, shutdown_reason_id
                    FROM state.agent_lifecycle
                    WHERE ended_at IS NULL
                    LIMIT 1
                """)
                return cur.fetchone()

    def _get_last_lifecycle(self) -> dict | None:
        """
        Возвращает последнюю запись lifecycle (по started_at DESC).
        """
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, state_type, started_at, ended_at, actor_id, shutdown_reason_id
                    FROM state.agent_lifecycle
                    ORDER BY started_at DESC
                    LIMIT 1
                """)
                return cur.fetchone()

    def _close_global_lifecycle(self, reason: str) -> None:
        """
        Завершает текущее активное глобальное состояние.
        ВАЖНО: Не обновляет shutdown_reason_id (только для off).
        """
        current = self._get_global_lifecycle()
        if not current:
            return

        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE state.agent_lifecycle
                    SET ended_at = %s, updated_at = NOW()
                    WHERE id = %s
                """, (datetime.now(timezone.utc), current['id']))
                conn.commit()
        logger.debug(f"Closed global lifecycle state: {current['state_type']} (id={current['id'][:8]})")

    def _start_new_lifecycle(
        self, 
        actor_id: str, 
        reason: str, 
        state_type: str = 'active', 
        shutdown_id: str | None = None
    ) -> None:
        """
        Создаёт новую запись глобального состояния.
        shutdown_id допустим только для state_type='off'.
        
        Защита: перед созданием новой записи принудительно закрывает ВСЕ
        зависшие записи (ended_at IS NULL), чтобы избежать UniqueViolation
        из-за прошлых багов или гонок.
        """
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                # === БРОНЯ: Закрываем все возможные висяки ===
                cur.execute("""
                    UPDATE state.agent_lifecycle
                    SET ended_at = NOW(), updated_at = NOW()
                    WHERE ended_at IS NULL
                """)
                
                # === Создаём новую запись ===
                cur.execute("""
                    INSERT INTO state.agent_lifecycle (
                        actor_id, state_type, reason_change, shutdown_reason_id, agent_version
                    ) VALUES (
                        %s, %s::state.agent_state_type, %s::state.lifecycle_change_reason, %s, %s
                    )
                """, (actor_id, state_type, reason, shutdown_id, agent_version))
                conn.commit()
                
        logger.info(
            f"Started new global lifecycle: {state_type} ({reason}), "
            f"initiated by actor {actor_id[:8]}"
        )

    def _convert_to_off(
        self, 
        record_id: str, 
        shutdown_id: str, 
        ended_at: datetime
    ) -> None:
        """
        Конвертирует существующую запись в состояние off.
        
        Используется при crash recovery для устранения дублирования:
        вместо закрытия active/sleep и вставки новой off, обновляем запись на месте.
        
        Args:
            record_id: ID записи для конвертации
            shutdown_id: ссылка на state.shutdown_reasons.id
            ended_at: время завершения (обычно NOW())
        """
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE state.agent_lifecycle
                    SET 
                        state_type = 'off',
                        reason_change = 'crash_recovery',
                        ended_at = %s,
                        shutdown_reason_id = %s,
                        updated_at = NOW()
                    WHERE id = %s
                """, (ended_at, shutdown_id, record_id))
                conn.commit()
        logger.debug(f"Converted lifecycle record {record_id[:8]} to off (crash_recovery)")

    def _get_inactivity_sleep_minutes(self) -> float:
     return self._get_setting_float("inactivity_sleep_minutes", default=5.0)
            
    def _get_setting_float(self, param_name: str, default: float = 0.2) -> float:
        """
        Получает числовое значение параметра из state.settings.
        
        Args:
            param_name: имя параметра
            default: значение по умолчанию, если параметр не найден
            
        Returns:
            float: значение параметра или default
        """
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT value_float FROM state.settings 
                    WHERE param_name = %s
                """, (param_name,))
                row = cur.fetchone()
                if not row or row[0] is None:
                    return default
                return float(row[0])

    # =========================================================================
    # ПУБЛИЧНЫЕ МЕТОДЫ ДЛЯ ОРКЕСТРАТОРА
    # =========================================================================
    def record_activity(self, actor_id: str, reason: str = 'user_activity') -> None:
        """Регистрирует активность агента, обновляя временную метку active-состояния
        или пробуждая из сна.

        Логика:
        - Если активного жизненного цикла нет → создаётся новый active.
        - Если текущее состояние sleep:
            * Определяется причина пробуждения (user_wake_up / agent_wake_up).
            * Закрывается текущий sleep-цикл.
            * Запускается новый active-цикл.
        - Если состояние active → обновляется только updated_at.

        Args:
            actor_id: Идентификатор инициатора (например, 'user' или 'orchestrator').
            reason: Причина активности ('user_activity' или 'agent_activity').

        Returns:
            None
        """
        current = self._get_global_lifecycle()
        if not current:
            self._start_new_lifecycle(actor_id, reason, 'active')
            return

        if current['state_type'] == 'sleep':
            wake_reason = 'user_wake_up' if reason == 'user_activity' else 'agent_wake_up'
            
            self._close_global_lifecycle(wake_reason)
            self._start_new_lifecycle(actor_id, wake_reason, 'active')
        else:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE state.agent_lifecycle
                        SET updated_at = NOW()
                        WHERE id = %s
                    """, (current['id'],))
                    conn.commit()
            logger.debug(f"Activity recorded by {actor_id[:8]}, state remains active")

    def check_inactivity(self) -> None:
        """
        Проверяет бездействие и переводит агента в sleep при необходимости.

        Логика:
        1. Если агент в состоянии active и время бездействия > inactivity_sleep_minutes - переводит lifecycle в sleep.
        2. Иначе — ничего не делает.

        Вызывается оркестратором в главном цикле (~1 сек).
        """
        current = self._get_global_lifecycle()
        if not current or current['state_type'] != 'active':
            return

        # === 1. Проверяем время бездействия ===
        now = datetime.now(timezone.utc)
        last_activity = current['updated_at']
        
        # Защита от naive datetime (на случай если БД вернула без timezone)
        if last_activity.tzinfo is None:
            last_activity = last_activity.replace(tzinfo=timezone.utc)
        
        elapsed_sec = (now - last_activity).total_seconds()
        threshold_sec = self._get_inactivity_sleep_minutes() * 60

        if elapsed_sec <= threshold_sec:
            return

        logger.info(
            f"Inactivity timeout: {elapsed_sec:.1f}s > {threshold_sec:.0f}s. "
            f"Transitioning active → sleep."
        )

        # Получаем actor_id из текущей записи lifecycle (исправление Pylance)
        actor_id = current.get('actor_id')
        actor_id_str = str(actor_id) if actor_id else "00000000-0000-0000-0000-000000000000"

        # === 2. Переводим lifecycle в sleep ===
        self._close_global_lifecycle('inactivity_timeout')
        self._start_new_lifecycle(actor_id_str, 'inactivity_timeout', 'sleep')

    # =========================================================================
    # PHS: STARTUP / SHUTDOWN
    # =========================================================================
    def _record_shutdown_reason(self, shutdown_type: str, actor_id: str) -> str:
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    INSERT INTO state.shutdown_reasons (actor_id, shutdown_type, timestamp)
                    VALUES (%s, %s, %s) RETURNING id
                """, (actor_id, shutdown_type, datetime.now(timezone.utc)))
                row = cur.fetchone()
                return str(row['id'])

    def _prompt_shutdown_reason(self) -> str:
        print("\nAgent was offline. Please specify the reason:")
        reasons = {
            'maintenance': 'Scheduled equipment maintenance',
            'crash': 'Crash',
            'forced_shutdown': 'Forced shutdown',
            'user_absence': 'Long-term absence of the user',
            'agent_modification': 'Agent refinement and testing'
        }
        for i, (enum_val, desc) in enumerate(reasons.items(), start=1):
            print(f"  [{i}] {desc}")

        enum_list = list(reasons.keys())
        import sys
        while True:
            print("Your choice (1-5):   ", end="  ", flush=True)
            choice = sys.stdin.readline().strip()
            if choice.isdigit() and 1 <= int(choice) <= len(enum_list):
                return enum_list[int(choice) - 1]
            print("Invalid choice. Please try again.")

    def _get_shutdown_type_by_id(self, shutdown_id: str) -> Optional[str]:
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT shutdown_type FROM state.shutdown_reasons WHERE id = %s
                """, (shutdown_id,))
                row = cur.fetchone()
                return row[0] if row else None

    def handle_startup(self, actor_id: str) -> None:
        """
        Вызывается при запуске. Корректно различает штатный старт и креш.
    
        Логика:
        1. Если есть запись с ended_at=NULL:
        - state_type='off' → штатный старт. Закрываем off, стартуем active.
        - state_type='active'/'sleep' → креш. Запрашиваем причину, КОНВЕРТИРУЕМ запись в off (без дублирования), стартуем active.
        2. Если нет активной записи → стартуем active.
        """
        logger.info("Starting lifecycle recovery...")
        
        current = self._get_global_lifecycle()

        if current is None:
            self._start_new_lifecycle(actor_id, 'startup', 'active')
            return

        state_type = current['state_type']
        downtime_start = current['started_at']
        downtime_duration = (datetime.now(timezone.utc) - downtime_start).total_seconds()
        now = datetime.now(timezone.utc)

        # === ИСПРАВЛЕНИЕ: off с ended_at=NULL — это штатное выключение ===
        if state_type == 'off':
            logger.info(
                f"Detected graceful shutdown state: off (id={current['id'][:8]}). "
                f"Closing and starting active."
            )

            shutdown_id = current.get('shutdown_reason_id')
            shutdown_type = None
            if shutdown_id:
                shutdown_type = self._get_shutdown_type_by_id(shutdown_id)
            
            if not shutdown_type:
                logger.warning("No shutdown_reason_id in off state.")

            self._close_global_lifecycle('startup')
            self._start_new_lifecycle(actor_id, 'startup', 'active')
            return

        # === active или sleep с ended_at=NULL — это креш ===
        if state_type in ('active', 'sleep'):
            logger.warning(f"Detected dangling lifecycle state: {state_type}. Treating as crash.")
        
            shutdown_type = self._prompt_shutdown_reason()
            shutdown_id = self._record_shutdown_reason(shutdown_type, actor_id)
            
            # Конвертируем запись в off
            self._convert_to_off(current['id'], shutdown_id, now)
            self._start_new_lifecycle(actor_id, 'startup', 'active')
            return

    def handle_graceful_shutdown(self, actor_id: str, exit_reason: str) -> None:
        """
        Обрабатывает штатное выключение агента.

        Выполняет закрытие lifecycle и переход в off.
        Args:
            actor_id: UUID актора, инициировавшего выключение.
            exit_reason: Причина выхода (user_exit, user_command).
        """
        logger.info(f"Handling graceful shutdown (reason: {exit_reason})...")

        # === Закрытие lifecycle ===
        shutdown_type = self._prompt_shutdown_reason()
        shutdown_id = self._record_shutdown_reason(shutdown_type, actor_id)

        self._close_global_lifecycle('shutdown_command')
        self._start_new_lifecycle(actor_id, 'shutdown_command', 'off', shutdown_id)
        logger.info("Graceful shutdown completed.")