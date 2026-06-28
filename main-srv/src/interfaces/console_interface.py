"""
main-srv/src/interfaces/console_interface.py

Консольный интерфейс для диалога с агентом.
Возможности:
- Многострочный ввод (Shift+Enter = новая строка, Enter = отправить)
- Ctrl+N = Новый диалог (разрыв контекста, rotate_dialogue)
- Ctrl+D = Корректный выход
- Автоматическая привязка пользователя ОС к актору (owner/user)
- Управление сессиями через SessionManager
- Интеграция с жизненным циклом: старт/завершение с фиксацией actor_id
Таблицы БД: dialogs.sessions, dialogs.row_messages, users.actors, users.actors_external_ids
Версия миграции: V001
"""

version = "1.1.0"
description = "Console interface for dialogue with an agent (owner mode)"

import logging
import pwd
import os
from session_services.session_manager import SessionManager
from prompt_toolkit import PromptSession, print_formatted_text
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.keys import Keys
from services.lifecycle_manager import LifecycleManager

# Получаем логгер для этого модуля
logger = logging.getLogger(__name__)

def _get_current_console_user() -> str:
    """
    Определяет уникальное имя текущего пользователя операционной системы.
    Возвращает строку в формате:  "console: <username> "
    Пример:  "console:debian ",  "console:root "

    Это значение будет использоваться как source_id в users.actors_external_ids
    """
    try:
        username = pwd.getpwuid(os.getuid()).pw_name
        return f"console:{username}"
    except Exception as e:
        logger.warning(f"Failed to determine OS username: {e}. Using 'console:unknown'")
        return "console:unknown"

def _print_welcome(agent_version: str, console_user_id: str, actor_type: str):
    """Выводит приветственное сообщение в консоль."""
    print(f"\n{'='*85}")
    print(f"🤖  Agent (version {agent_version})")
    print(f"👤  Mode: {actor_type} (access level) | User: {console_user_id}")
    print(f"💡  Enter = send, Alt+Enter = new line, exit/выход or Ctrl+D to quit")
    print(f"{'='*85}\n")

def _print_status(message: str, is_success: bool):
    """Выводит цветное сообщение статуса в консоль."""
    COLOR_GREEN = "\033[92m"
    COLOR_RED = "\033[91m"
    COLOR_RESET = "\033[0m"
    symbol = "✓" if is_success else "✗"
    color = COLOR_GREEN if is_success else COLOR_RED
    print(f"{color}[{symbol}] {message}{COLOR_RESET}")

def create_prompt_session(session_manager: SessionManager) -> PromptSession:
    """
    Создаёт сессию prompt_toolkit с поддержкой:
    - Enter = отправить сообщение
    - Alt+Enter = новая строка
    - Ctrl+N = Новый диалог (очистка контекста, вызов session_manager.rotate_dialogue)
    - Ctrl+D = аварийный выход
    - Ctrl+C = игнорируется (чтобы не выходить случайно)
    """
    bindings = KeyBindings()
    # Alt+Enter = новая строка (приоритет выше, чем Enter)
    @bindings.add(Keys.Escape, Keys.Enter)
    def _(event):
        event.current_buffer.insert_text('\n')

    # Enter = отправить
    @bindings.add(Keys.Enter)
    def _(event):
        event.current_buffer.validate_and_handle()

    # Ctrl+D = аварийный выход (не зависит от раскладки)
    @bindings.add('c-d')
    def _(event):
        raise KeyboardInterrupt()

    # Ctrl+C = игнорировать (чтобы не выходить случайно вместо копирования)
    @bindings.add('c-c')
    def _(event):
        pass

    # Ctrl+N = Новый диалог (разрыв контекста)
    @bindings.add('c-n')
    def _(event):
        # Используем print_formatted_text вместо print() — не ломает верстку prompt_toolkit
        print_formatted_text(HTML('\n<b>[!]</b> Initiating new dialogue...'))
        
        session_manager.rotate_dialogue("user_new_dialogue")
        
        print_formatted_text(HTML('<b>[OK]</b> New dialogue started.\n'))
        
        # Очищаем буфер, чтобы не отправить мусор
        event.current_buffer.text = ""
        
        # Принудительно перерисовываем промпт
        event.app.invalidate()

    return PromptSession(
        key_bindings=bindings,
        multiline=True,
        enable_history_search=True,
    )

def get_user_input(session: PromptSession) -> str:
    """
    Получает ввод от пользователя.
    Выбрасывает KeyboardInterrupt при Ctrl+D.
    """
    try:
        result = session.prompt(message='\n👤 You: ')
        return (result or "").strip()
    except (EOFError, KeyboardInterrupt):
        raise KeyboardInterrupt()

def run_console_interface(db_config: dict, agent_version: str, lifecycle_mgr: LifecycleManager):
    """
    Главная точка входа для консольного интерфейса.
    Args:
        db_config: словарь с параметрами подключения к PostgreSQL
        agent_version: строка версии агента из pyproject.toml
        lifecycle_mgr: экземпляр LifecycleManager для управления состоянием агента
    """

    # === ШАГ 1: Инициализация ===
    console_user_id = _get_current_console_user()
    logger.info(f"Starting console interface. User: {console_user_id}, version: {agent_version}")

    # === ШАГ 2: Инициализация сервиса сессий ===
    session_service = SessionManager(db_config, agent_version, console_user_id)

    # Строго допустимые значения из session_close_reason ENUM
    exit_reason: str = "unknown"

    try:
        # === ШАГ 3: Привязка пользователя к актору owner ===
        owner_linked = session_service.ensure_actor_linked()
        if owner_linked:
            logger.info(f"User {console_user_id} linked to actor (type: {session_service.actor_type})")
            _print_status(f"User {console_user_id} activated as {session_service.actor_type}", True)
        else:
            logger.debug(f"User {console_user_id} already linked to {session_service.actor_type}")
        
        # === ШАГ 4: === PHS Lifecycle Initialization (Вызываем после того, как actor_id гарантированно определён) ===
        # Гарантируем, что actor_id установлен (после ensure_actor_linked/create_session это всегда так)
        assert session_service.actor_id is not None, "actor_id must be set after ensure_actor_linked"
        lifecycle_mgr.handle_startup(session_service.actor_id)
        logger.info("Lifecycle initialized, agent state ready.")

        # === ШАГ 5: Создание новой сессии ===
        # Каждый запуск консоли = новая сессия (не возобновляем старые)
        session_id = session_service.create_session()
        logger.info(f"New dialog session created: {session_id}")
        _print_status(f"Session #{session_id[:8]} started", True)
        
        # === ШАГ 6: Вывод приветствия (теперь все данные известны) ===
        _print_welcome(agent_version, console_user_id, session_service.actor_type)
        
        # === ШАГ 7: Создаём сессию ввода с привязкой менеджера (для Ctrl+N) ===
        prompt_session = create_prompt_session(session_service)

        # === ШАГ 8: Основной цикл диалога ===
        while True:
            try:
                # Получаем многострочный ввод
                user_input = get_user_input(prompt_session)
                
                # Обработка команд выхода
                if user_input.lower() in ("exit", "выход"):
                    logger.info("User entered exit command")
                    exit_reason = "user_command"
                    break
                
                if not user_input:
                    continue
                
                logger.debug(f"Received user message: {len(user_input)} chars")
                
                # 8.1: Сохраняем сообщение в БД
                message_id = session_service.save_message(content=user_input)
                logger.debug(f"Message saved to DB with ID: {message_id[:8]}")
                
                # 8.2: Создаём задачу для оркестратора
                from orchestrator.orchestrator_entry import on_user_message
                try:
                    orchestrator_task_id = on_user_message(message_id=message_id)
                    logger.debug(f"Orchestrator task submitted: {orchestrator_task_id[:8]}...")
                except Exception as e:
                    logger.error(f"Failed to submit orchestrator task for message {message_id[:8]}: {e}", exc_info=True)
                    # Не прерываем цикл — просто ждём ответа (он не придёт, но интерфейс останется жив)
                    orchestrator_task_id = None
                                                 
                # 8.3: Обновляем время активности сессии
                session_service.update_activity()

                # 8.4: Показываем статус обработки
                status_text = "⚙️  Agent is thinking..."
                print(f"\n{status_text}", end="", flush=True)

                # 8.5: Ожидаем ответ от агента
                agent_response = session_service.wait_for_agent_response(
                    user_message_id=message_id,
                    timeout_seconds=300
                )

                # 8.6: Заменяем статус на ответ
                if agent_response:
                    print(f"\r{' ' * len(status_text)}\r🤖 Agent: {agent_response}\n", end="", flush=True)
                    logger.info("Agent response received: {len(agent_response)} chars")
                else:
                    print(f"\r{' ' * len(status_text)}\r🤖 Agent: [No response received]\n", end="", flush=True)
                    logger.warning("Timeout waiting for agent response")

            except KeyboardInterrupt:
                logger.info("Session interrupted by user (Ctrl+D)")
                
                exit_reason = "user_exit"
                break
                
            except Exception as e:
                logger.error(f"Error in dialog loop: {e}", exc_info=True)
                _print_status(f"Processing error: {e}", False)
                exit_reason = "loop_error"
                continue
            
    except Exception as e:
        logger.critical(f"Critical error in console interface: {e}", exc_info=True)
        _print_status(f"Critical error: {e}", False)
        exit_reason = "critical_error"
    
    # === ШАГ 9: Завершение сессии ===
    finally:
        logger.info(f"Closing dialog session with reason: {exit_reason}")
       
        # === Graceful Shutdown (Lifecycle) ===
        if exit_reason in ("user_command", "user_exit"):
            assert session_service.actor_id is not None, "actor_id must be set for graceful shutdown"
            lifecycle_mgr.handle_graceful_shutdown(session_service.actor_id, exit_reason)
                
        session_service.close_session(reason=exit_reason)
        _print_status("Session completed. Data saved to DB.", True)
        
        session_service.cleanup()
        logger.debug("Console interface resources released")

    return 0