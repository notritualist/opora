"""
main-srv/src/interfaces/console_interface.py

Консольный интерфейс (CLI) для интерактивного диалога с агентом в режиме владельца (owner).

Основные возможности:
1. Управление вводом (prompt_toolkit):
   - Enter: отправить сообщение.
   - Alt+Enter (или Shift+Enter): перенос строки.
   - Ctrl+N: разрыв контекста (rotate_dialogue), начало нового диалога.
   - Ctrl+D: корректный выход из приложения.
   - Ctrl+C: игнорируется (защита от случайного выхода).

2. Управление жизненным циклом и сессиями:
   - Автоматическая привязка пользователя ОС (console:username) к актору в БД.
   - Обработка "зависших" состояний (dangling state): при старте проверяет, был ли агент в active/sleep, 
     и через UI запрашивает причину предыдущего завершения (shutdown_type).
   - Интеграция с LifecycleManager и SessionManager.

3. Фоновый режим верификации гипотез (LISTEN/NOTIFY):
   - Отдельный поток слушает PostgreSQL канал 'verification_channel'.
   - При получении события от оркестратора (наличие draft-гипотез) прерывает блокирующий prompt 
     через кастомное исключение PromptInterruptedException.
   - Предлагает пользователю интерактивную верификацию:
     [Y] подтвердить, [N] отклонить, [E] редактировать, [C] контекст, [S] пропустить, [Q] выход.
   - Режим [E] позволяет менять метаданные (домен, тема, форма) и запускать LLM-уточнение текста 
     (hypothesis_refinement) с последующим ручным подтверждением.

4. Обработка shutdown:
   - При выходе (Ctrl+D, exit) запрашивает причину завершения и корректно закрывает сессии и лайфцикл.
"""

version = "1.3.0"
description = "Console interface for dialogue with an agent (owner mode)"

import logging
import pwd
import os
import select
import time
import threading
import queue
import psycopg2
import psycopg2.extensions
from typing import Optional

from prompt_toolkit import PromptSession, print_formatted_text
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.keys import Keys
from prompt_toolkit.styles import Style

from session_services.session_manager import SessionManager
from services.lifecycle_manager import LifecycleManager
from orchestrator.orchestrator_entry import schedule_hypothesis_refinement
from memory_service.verification_service import (
    get_unverified_hypotheses, 
    get_source_context,
    create_session,
    complete_session,
    defer_session,
    can_propose_verification,
    get_defer_minutes,
    record_action,
    complete_verification_proposal_task,
)

# Получаем логгер для этого модуля
logger = logging.getLogger(__name__)


class PromptInterruptedException(Exception):
    """
    Кастомное исключение для прерывания prompt из фонового потока.
    Позволяет отличить app.exit() (NOTIFY от оркестратора) от нажатия Ctrl+D (EOFError).
    """
    pass

# =============================================================================
# === СТИЛИ ДЛЯ PROMPT_TOOLKIT ===============================================
# =============================================================================
VERIFICATION_STYLE = Style.from_dict({
    'header':    'bold #00FF00',
    'hypothesis':'#FFFF00',
    'source':    '#888888 italic',
    'action':    'bold #00BFFF',
    'success':   'bold #00FF00',
    'error':     'bold #FF0000',
    'warning':   'bold #FFA500',
    'context':   '#AAAAAA',
    'highlight': 'bg:#444444 #FFFFFF bold',
})

def _print_html(html_text: str) -> None:
    """Выводит форматированный текст через prompt_toolkit."""
    print_formatted_text(HTML(html_text), style=VERIFICATION_STYLE)


def _prompt_html(session: PromptSession, text: str) -> str:
    """Запрашивает ввод с HTML-подсветкой."""
    try:
        result = session.prompt(HTML(text), style=VERIFICATION_STYLE, validator=None)
        if result is None:
            return ""
        return (result or "")
    except PromptInterruptedException:
        # Прерывание из фонового потока (NOTIFY) — возвращаем пустую строку
        return ""
    except EOFError:
        # Ctrl+D — пробрасываем наружу для выхода из программы
        raise
    except KeyboardInterrupt:
        # Ctrl+C — игнорируем
        return ""
    except Exception:
        return ""

# =============================================================================
# === БАЗОВЫЕ ФУНКЦИИ КОНСОЛИ ================================================
# =============================================================================
def _get_current_console_user() -> str:
    """
    Определяет уникальное имя текущего пользователя операционной системы.
    Возвращает строку в формате: "console:<username>"
    Пример: "console:debian", "console:root"
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
    @bindings.add(Keys.Escape, Keys.Enter)
    def _(event):
        event.current_buffer.insert_text('\n')
    @bindings.add(Keys.Enter)
    def _(event):
        event.current_buffer.validate_and_handle()
    @bindings.add('c-d')
    def _(event):
        event.app.exit(exception=EOFError())
    @bindings.add('c-c')
    def _(event):
        pass
    @bindings.add('c-n')
    def _(event):
        print_formatted_text(HTML('\n<b>[!]</b> Initiating new dialogue...'))
        session_manager.rotate_dialogue("user_new_dialogue")
        print_formatted_text(HTML('<b>[OK]</b> New dialogue started.\n'))
        event.current_buffer.text = ""
        event.app.invalidate()
    return PromptSession(
        key_bindings=bindings,
        multiline=True,
        enable_history_search=True,
    )


def get_user_input(session: PromptSession) -> str:
    """
    Получает ввод от пользователя.
    """
    try:
        result = session.prompt(message='\n👤 You: ', validator=None)
        if result is None:
            return ""
        return (result or "").strip()
    except PromptInterruptedException:
        # Прерывание из фонового потока (NOTIFY) — просто возвращаем управление в цикл
        return ""
    except EOFError:
        # Ctrl+D — пробрасываем для корректного выхода из программы
        raise
    except KeyboardInterrupt:
        # Ctrl+C — игнорируется
        return ""
    except Exception:
        return ""

# =============================================================================
# === SHUTDOWN REASON PROMPT (PROMPT_TOOLKIT + DB ENUM) =====================
# =============================================================================
def _prompt_shutdown_reason_ui(prompt_session: PromptSession, lifecycle_mgr: LifecycleManager) -> str:
    """
    Запрашивает причину выключения через prompt_toolkit.
    Валидация вынесена в Python-код, чтобы избежать загрязнения состояния 
    PromptSession при прерывании через app.exit() из фонового потока.
    """
    reasons = lifecycle_mgr._get_shutdown_types_from_db()
    if not reasons:
        logger.warning("shutdown_type ENUM is empty, falling back to 'crash'")
        return 'crash'
    
    print_formatted_text(HTML('\n<warning>⚠️  Agent was offline. Please specify the reason:</warning>'))
    for i, reason in enumerate(reasons, start=1):
        print_formatted_text(HTML(f'  [{i}] {reason}'))
    
    while True:
        try:
            choice = prompt_session.prompt(
                f'Your choice (1-{len(reasons)}): ',
                validate_while_typing=False
            ).strip()
            
            if choice.isdigit() and 1 <= int(choice) <= len(reasons):
                return reasons[int(choice) - 1]
            else:
                print_formatted_text(HTML(f'<error>Введите число от 1 до {len(reasons)}</error>'))
        except PromptInterruptedException:
            continue
        except EOFError:
             # Пользователь нажал Ctrl+D — используем значение по умолчанию
            print_formatted_text(HTML('\n<warning>Interrupted. Using default: crash</warning>'))
            return 'crash'
        except KeyboardInterrupt:
            print_formatted_text(HTML('\n<warning>Interrupted. Using default: crash</warning>'))
            return 'crash'
        except Exception:
            continue

# =============================================================================
# === ФОНОВЫЙ ПОТОК LISTEN/NOTIFY ============================================
# =============================================================================
def _verification_event_listener(db_config: dict, event_queue: queue.Queue, stop_event: threading.Event, prompt_session: PromptSession):
    """
    Фоновый поток. Слушает канал 'verification_channel'.
    При получении NOTIFY кладет payload в event_queue и прерывает блокирующий prompt.
    """
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        curs = conn.cursor()
        curs.execute("LISTEN verification_channel;")
        logger.debug("LISTEN verification_channel started")
        while not stop_event.is_set():
            if select.select([conn], [], [], 1.0) == ([], [], []):
                continue
            conn.poll()
            while conn.notifies:
                notify = conn.notifies.pop(0)
                logger.info(f"Received NOTIFY: {notify.payload}")
                event_queue.put(notify.payload)

                # === ИСПРАВЛЕНИЕ: Прерываем блокирующий prompt ===
                if prompt_session and hasattr(prompt_session, 'app') and prompt_session.app:
                    try:
                        prompt_session.app.exit(exception=PromptInterruptedException())
                    except Exception as e:
                        logger.debug(f"Failed to exit prompt app: {e}")

    except Exception as e:
        if not stop_event.is_set():
            logger.error(f"Verification listener error: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()

# =============================================================================
# === РЕЖИМ ВЕРИФИКАЦИИ ======================================================
# =============================================================================
def run_verification_mode(
    db_config: dict, 
    actor_id: str, 
    prompt_session: PromptSession,
    proposal_task_id: Optional[str] = None,
    draft_count: int = 0,
    session_id: Optional[str] = None
) -> None:
    """
    Интерактивный цикл разбора draft-гипотез.
    
    Args:
        proposal_task_id: UUID задачи оркестратора (для закрытия после завершения)
        draft_count: количество hypothesis (для метрик)
        session_id: существующая active-сессия (созданная до диалога [Y]/[N])
    """
    hypotheses = get_unverified_hypotheses(db_config)
    if not hypotheses:
        _print_html("<context>Нет гипотез для верификации.</context>\n")
        if session_id:
            complete_session(db_config, session_id)
        return
    
    # Используем существующую сессию или создаём новую (fallback)
    if not session_id:
        session_id = create_session(db_config, len(hypotheses))
    total = len(hypotheses)
    _print_html(f"\n<header>{'='*60}</header>")
    _print_html(f"<header>  🔬 РЕЖИМ ВЕРИФИКАЦИИ ({total} гипотез)</header>")
    _print_html(f"<header>{'='*60}</header>\n")
    for idx, hyp in enumerate(hypotheses, 1):
        hyp_id = str(hyp['id'])
        hyp_text = hyp['hypothesis_text']
        confidence = hyp.get('confidence', 0.0)
        domain_code = hyp.get('domain_code', 'unknown')
        topic_name = hyp.get('topic_name') or (str(hyp.get('topic_id', ''))[:8] if hyp.get('topic_id') else 'Без темы')
        form_code = hyp.get('form_code') or 'unknown'
        knowledge_source = hyp.get('knowledge_source', 'unknown')
        source_ids = hyp.get('source_message_ids') or []
        _print_html(
            f"\n<action>── Гипотеза {idx}/{total} ──</action>"
            f"\n<context>  📊 Уверенность: {confidence:.0%}</context>"
            f"\n<context>  🏷️ Домен: {domain_code} | Источник: {knowledge_source}</context>"
            f"\n<context>  📚 Тема: {topic_name}</context>"
            f"\n<context>  🧩 Форма: {form_code}</context>"
            f"\n<context>  🆔 ID: {hyp_id[:8]}</context>"
            f"\n<hypothesis>  📌 {hyp_text}</hypothesis>"
        )
        action_taken = False
        while not action_taken:
            action_raw = _prompt_html(
                prompt_session,
                "\n<action>Действие: [Y] подтвердить / [N] отклонить / "
                "[E] редактировать / [C] контекст / [S] пропустить / [Q] выход: </action>"
            )
            action = (action_raw or "").strip().lower()
            if not action:
                continue
            if action == 'c':
                _show_source_context(db_config, source_ids)
                continue
            elif action == 'y':
                record_action(db_config, session_id, hyp_id, 'confirmed', actor_id=actor_id)
                _print_html("<success>  ✅ Подтверждено</success>")
                action_taken = True
            elif action == 'n':
                record_action(db_config, session_id, hyp_id, 'rejected', actor_id=actor_id)
                _print_html("<error>  ❌ Отклонено</error>")
                action_taken = True
            elif action == 's':
                record_action(db_config, session_id, hyp_id, 'skipped', actor_id=actor_id)
                _print_html("<context>  ⏭ Пропущено</context>")
                action_taken = True
            elif action == 'e':
                _handle_edit_mode(db_config, session_id, hyp, actor_id, prompt_session)
                action_taken = True
            elif action == 'q':
                _print_html("\n<warning>Выход из верификации. Создаём отложенную сессию для блокировки повторного предложения.</warning>")
                # Создаём deferred сессию, чтобы оркестратор не спамил предложениями
                defer_min = get_defer_minutes(db_config)
                defer_session(db_config, session_id, defer_min)
                _print_html(f"<warning>⏰ Верификация отложена на {defer_min:.0f} мин.</warning>")
                _print_verification_summary(db_config, session_id)
                # Закрываем задачу оркестратора
                if proposal_task_id:
                    complete_verification_proposal_task(
                        db_config, proposal_task_id, 'deferred',
                        {'draft_count': draft_count, 'defer_min': defer_min, 'session_id': session_id}
                    )
                return
            else:
                _print_html("<error>  Неизвестная команда.</error>")
    # После успешного завершения откладываем сессию на defer_min,
    # чтобы оркестратор не предлагал верификацию снова сразу.
    defer_min = get_defer_minutes(db_config)
    defer_session(db_config, session_id, defer_min)
    _print_html(f"\n<header>✅ Все {total} гипотез разобрано!</header>")
    _print_verification_summary(db_config, session_id)
    _print_html(f"<warning>⏰ Следующая верификация возможна через {defer_min:.0f} мин.</warning>\n")
    
    # Закрываем задачу оркестратора после успешного завершения верификации
    if proposal_task_id:
        handled_hyp_ids = [str(h['id']) for h in hypotheses]
        complete_verification_proposal_task(
            db_config, proposal_task_id, 'verified',
            {
                'draft_count': len(hypotheses),
                'verification_session_id': session_id,
                'hypotheses_total': total,
                'handled_hypothesis_ids': handled_hyp_ids
            }
        )


def _show_source_context(db_config: dict, source_message_ids: list) -> None:
    """
    Показывает ТОЛЬКО сообщения-источники гипотезы (без контекстного окна).
    """
    if not source_message_ids:
        _print_html("<warning>  Источник не указан</warning>")
        return
    
    context = get_source_context(db_config, source_message_ids, context_window=0)
    if not context:
        _print_html("<warning>  Сообщения-источники не найдены</warning>")
        return
    
    _print_html("\n<context>  ── Сообщения-источники гипотезы ──</context>")
    for msg in context:
        role = msg.get('actor_type', msg.get('role', 'unknown'))
        content = (msg.get('row_text') or msg.get('content') or '')
        _print_html(f"<highlight>  ▶ [{role}] {content}</highlight>")
    _print_html("<context>  ── Конец источников ──</context>\n")


def _handle_edit_mode(db_config: dict, session_id: str, hypothesis: dict, actor_id: str, prompt_session: PromptSession) -> None:
    hyp_id = str(hypothesis['id'])
    hyp_text = hypothesis['hypothesis_text']
    source_ids = hypothesis.get('source_message_ids') or []
    _print_html("\n<action>✏️  Режим редактирования</action>")
    _show_source_context(db_config, source_ids)

    # === 1. ЗАПРОС МЕТАДАННЫХ ===
    new_domain, new_source, new_topic_id, new_topic_name, new_form_code, metadata_changed = _prompt_metadata_edit(
        db_config, hypothesis, prompt_session
    )

    # === 2. ЗАПРОС КОММЕНТАРИЯ К ТЕКСТУ ===
    _print_html("<action>  Введите комментарий/исправление текста (пустая строка для пропуска):</action>")
    comment_lines = []
    while True:
        line = _prompt_html(prompt_session, "  <context>> </context>")
        if not line or not line.strip():
            break
        comment_lines.append(line)
    user_comment = "\n".join(comment_lines).strip()

    if not user_comment and not metadata_changed:
        _print_html("<warning>  Никаких изменений не внесено.</warning>")
        return

    # === 3. Формируем metadata_updates для композера ===
    metadata_updates = {}
    if metadata_changed:
        metadata_updates = {
            'domain_code': new_domain,
            'knowledge_source': new_source,
            'topic_id': new_topic_id,
            'form_code': new_form_code,  # ← НОВОЕ
        }

    # === РАННИЙ ВЫХОД: пустой комментарий — только метаданные ===
    if not user_comment:
        if metadata_updates:
            with psycopg2.connect(**db_config) as conn:
                with conn.cursor() as cur:
                    uf, up = [], []
                    if 'domain_code' in metadata_updates:
                        uf.append("domain_code = %s"); up.append(metadata_updates['domain_code'])
                    if 'knowledge_source' in metadata_updates:
                        uf.append("knowledge_source = %s"); up.append(metadata_updates['knowledge_source'])
                    if 'topic_id' in metadata_updates:
                        uf.append("topic_id = %s::uuid"); up.append(metadata_updates['topic_id'])
                    if 'form_code' in metadata_updates:
                        uf.append("form_code = %s"); up.append(metadata_updates['form_code'])
                    if uf:
                        up.append(hyp_id)
                        cur.execute(f"UPDATE memory.hypotheses SET {', '.join(uf)}, updated_at = NOW() WHERE id = %s::uuid", up)
                    meta_note = f"[metadata: {', '.join(f'{k}={v}' for k, v in metadata_updates.items())}]"
                    
                    # ✅ Добавляем обновление статуса
                    cur.execute("""
                        UPDATE memory.hypotheses
                        SET status = 'confirmed'::memory.hypothesis_status,
                            verified_at = NOW(),
                            verified_by_actor_id = %s::uuid
                        WHERE id = %s::uuid
                    """, (actor_id, hyp_id))

                    cur.execute("""
                        INSERT INTO memory.verification_actions (
                            session_id, hypothesis_id, action_type,
                            original_text, updated_text, user_comment
                        ) VALUES (%s::uuid, %s::uuid, 'edited'::memory.verification_action_type, %s, %s, %s)
                    """, (session_id, hyp_id, hyp_text, hyp_text, meta_note))
                    cur.execute("UPDATE memory.verification_sessions SET hypotheses_edited = hypotheses_edited + 1 WHERE id = %s::uuid", (session_id,))
                    conn.commit()
            _print_html(f"<context>  📋 Метаданные обновлены: домен={new_domain}, источник={new_source}, тема={new_topic_name}</context>")
            _print_html("<success>  ✅ Изменения применены (только метаданные)</success>")
        return
    
    # === 4. ЗАПУСК ЗАДАЧИ (композер ТОЛЬКО генерирует текст, НЕ меняет БД) ===
    _print_html("<action>  ⚙️ Генерация уточнённой версии через LLM...</action>")
    try:
        task_id = schedule_hypothesis_refinement(
            hypothesis_id=hyp_id,
            user_comment=user_comment,  # ← БЕЗ заглушки, передаём как есть
            verification_session_id=session_id,
            metadata_updates=metadata_updates,
        )
        
        # Ждём результат - получаем ВЕСЬ output_data
        output_data = _wait_for_refinement_result(db_config, task_id, timeout=120)
        
        if not output_data or not output_data.get('refined_text'):
            _print_html("<error>  ⚠ Не удалось получить уточнённую гипотезу.</error>")
            return
        
        refined_text = output_data['refined_text']
        original_text = output_data.get('original_text', hyp_text)
        prompt_id = output_data.get('prompt_id')
        step_id = output_data.get('orchestrator_step_id')

        # === 5. ПОДТВЕРЖДЕНИЕ С ВОЗМОЖНОСТЬЮ РУЧНОЙ ПРАВКИ ===
        current_text = refined_text

        while True:
            _print_html(f"\n<success>  📝 Уточнённая гипотеза:</success>")
            _print_html(f"<hypothesis>  {current_text}</hypothesis>")
            if metadata_changed:
                _print_html(f"<context>  📋 Метаданные обновлены: домен={new_domain}, источник={new_source}, тема={new_topic_name}, форма={new_form_code}</context>")

            action_raw = _prompt_html(
                prompt_session,
                "\n<action>Действие: [Y] принять / [E] редактировать / [N] откатить: </action>"
            )
            action = (action_raw or "").strip().lower()

            if action in ('e', 'edit', 'р', 'редактировать'):
                _print_html("<action>  ✏️  Введите исправленный текст:</action>")
                edit_lines = []
                while True:
                    line = _prompt_html(prompt_session, "  <context>> </context>")
                    if not line and not edit_lines:
                        _print_html("<warning>  Редактирование отменено</warning>")
                        break
                    if not line or not line.strip():
                        break
                    edit_lines.append(line)
                if edit_lines:
                    current_text = "\n".join(edit_lines).strip()
                continue

            if action in ('y', 'yes', 'д', 'да'):
                final_comment = user_comment or ""
                if metadata_updates:
                    meta_note = f"[metadata: {', '.join(f'{k}={v}' for k, v in metadata_updates.items())}]"
                    final_comment = f"{final_comment} {meta_note}".strip()
                
                record_action(
                    db_config=db_config,
                    session_id=session_id,
                    hypothesis_id=hyp_id,
                    action_type='edited',
                    actor_id=actor_id,
                    original_text=original_text,
                    updated_text=current_text,
                    user_comment=final_comment,
                    orchestrator_step_id=step_id,
                    prompt_id=prompt_id,
                    metadata_updates=metadata_updates,
                )
                _print_html("<success>  ✅ Изменения применены</success>")
                break

            if action in ('n', 'no', 'н', 'нет'):
                _print_html("<context>  ⏪ Изменения отклонены, гипотеза не изменена</context>")
                break

            _print_html("<error>  Неизвестная команда. Используйте Y/E/N.</error>")

    except Exception as e:
        logger.error("Edit mode error: %s", e, exc_info=True)
        _print_html(f"<error>  ❌ Ошибка: {e}</error>")


def _wait_for_refinement_result(db_config: dict, task_id: str, timeout: int = 120) -> dict | None:
    """Возвращает весь output_data задачи, а не только refined_text."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            with psycopg2.connect(**db_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT status, output_data FROM orchestrator.orchestrator_tasks WHERE id = %s", (task_id,))
                    row = cur.fetchone()
                    if not row: 
                        return None
                    status, output = row
                    if status == 'completed' and output:
                        return output  # ← Возвращаем весь output_data
                    if status in ('failed', 'cancelled'):
                        return None
        except Exception:
            pass
        time.sleep(0.5)
    return None


def _print_verification_summary(db_config: dict, session_id: str) -> None:
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM memory.verification_sessions WHERE id = %s", (session_id,))
                columns = [desc[0] for desc in cur.description]
                row = cur.fetchone()
                if not row: return
                s = dict(zip(columns, row))
    except Exception:
        return
    _print_html(f"\n<header>{'─'*40}</header>")
    _print_html(f"<header>  📋 Итоги верификации</header>")
    _print_html(f"<context>  Всего:      {s.get('hypotheses_total', 0)}</context>")
    _print_html(f"<success>  ✅ Подтв.:  {s.get('hypotheses_confirmed', 0)}</success>")
    _print_html(f"<error>  ❌ Откл.:   {s.get('hypotheses_rejected', 0)}</error>")
    _print_html(f"<action>  ✏️ Изм.:    {s.get('hypotheses_edited', 0)}</action>")
    _print_html(f"<context>  ⏭ Пропущ.: {s.get('hypotheses_skipped', 0)}</context>")
    _print_html(f"<header>{'─'*40}</header>\n")


def _prompt_metadata_edit(db_config: dict, hyp: dict, prompt_session: PromptSession) -> tuple:
    """
    Запрашивает новые значения домена, источника и темы через UI.
    Возвращает (domain_code, knowledge_source, topic_id, changed).
    """
    current_domain = hyp.get('domain_code', 'unknown')
    current_source = hyp.get('knowledge_source', 'unknown')
    current_topic_id = hyp.get('topic_id')
    current_topic_name = hyp.get('topic_name') or 'unknown'
    current_form = hyp.get('form_code') or 'unknown'
    
    _print_html("\n<action>📋 Редактирование метаданных (Enter = оставить текущее)</action>")
    
    # Домен
    new_domain = _prompt_html(
        prompt_session,
        f"<context>  🏷️ Домен (текущий: {current_domain}): </context>"
    ).strip()
    if not new_domain:
        new_domain = current_domain
    
    # Источник
    new_source = _prompt_html(
        prompt_session,
        f"<context>  📚 Источник (текущий: {current_source}): </context>"
    ).strip()
    if not new_source:
        new_source = current_source
    
    # Тема
    new_topic_name = _prompt_html(
        prompt_session,
        f"<context>  📖 Тема (текущая: {current_topic_name}): </context>"
    ).strip()

    # Форма
    new_form_raw = _prompt_html(
        prompt_session,
        f"<context>  🧩 Форма (текущая: {current_form}) [fact/goal/task/project/entity/skill/event]: </context>"
    ).strip().lower()

    new_topic_id = current_topic_id
    topic_changed = False
    if new_topic_name:
        if new_topic_name.lower() in ('без темы', 'none', 'нет', '-'):
            new_topic_id = None
            topic_changed = True
        elif new_topic_name != current_topic_name:
            # Ищем или создаём тему в БД
            try:
                with psycopg2.connect(**db_config) as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT id FROM memory.topics WHERE name = %s", (new_topic_name,))
                        row = cur.fetchone()
                        if row:
                            new_topic_id = str(row[0])
                        else:
                            cur.execute(
                                "INSERT INTO memory.topics (name) VALUES (%s) RETURNING id",
                                (new_topic_name,)
                            )
                            new_topic_id = str(cur.fetchone()[0])
                        conn.commit()
                topic_changed = True
            except Exception as e:
                logger.warning(f"Failed to get/create topic: {e}")
                _print_html(f"<error>  ⚠ Ошибка работы с темой: {e}</error>")
                new_topic_id = current_topic_id
    
    new_form_code = current_form
    form_changed = False
    if new_form_raw in ('без формы', 'none', '-', ''):
        if new_form_raw == '-' and current_form != 'unknown':
            new_form_code = None
            form_changed = True
        # else — Enter (пустая строка), оставляем текущее
    elif new_form_raw in ('fact', 'goal', 'task', 'project', 'entity', 'skill', 'event'):
        if new_form_raw != current_form:
            new_form_code = new_form_raw
            form_changed = True
    elif new_form_raw:
        _print_html(f"<warning>  Неизвестная форма '{new_form_raw}', оставляем текущую</warning>")

    changed = (
        new_domain != current_domain or
        new_source != current_source or
        topic_changed or
        form_changed
    )
    
    # Возвращаем имя темы для красивого вывода в UI
    display_topic_name = new_topic_name if new_topic_name else 'unknown'
    return new_domain, new_source, new_topic_id, display_topic_name, new_form_code, changed

# =============================================================================
# === ГЛАВНАЯ ТОЧКА ВХОДА ====================================================
# =============================================================================
def run_console_interface(db_config: dict, agent_version: str, lifecycle_mgr: LifecycleManager):
    """
    Главная точка входа для консольного интерфейса.
    Args:
        db_config: словарь с параметрами подключения к PostgreSQL
        agent_version: строка версии агента из pyproject.toml
        lifecycle_mgr: экземпляр LifecycleManager для управления состоянием агента
    """
    console_user_id = _get_current_console_user()
    logger.info(f"Starting console interface. User: {console_user_id}, version: {agent_version}")
    session_service = SessionManager(db_config, agent_version, console_user_id)
    exit_reason: str = "unknown"
    event_queue = queue.Queue()
    stop_listener = threading.Event()
    
    # === FIX: Создаём prompt_session ДО try, чтобы Pylance не ругался ===
    # Это также активирует key bindings (Ctrl+C блокировка) до handle_startup
    prompt_session = create_prompt_session(session_service)

    try:
        owner_linked = session_service.ensure_actor_linked()
        if owner_linked:
            logger.info(f"User {console_user_id} linked to actor (type: {session_service.actor_type})")
            _print_status(f"User {console_user_id} activated as {session_service.actor_type}", True)
        else:
            logger.debug(f"User {console_user_id} already linked to {session_service.actor_type}")
        assert session_service.actor_id is not None, "actor_id must be set after ensure_actor_linked"
        
        # === ШАГ: Проверяем dangling state и запрашиваем причину через UI ===
        current = lifecycle_mgr._get_global_lifecycle()
        shutdown_type = None
        if current and current['state_type'] in ('active', 'sleep'):
            shutdown_type = _prompt_shutdown_reason_ui(prompt_session, lifecycle_mgr)
        
        lifecycle_mgr.handle_startup(session_service.actor_id, shutdown_type=shutdown_type)
        logger.info("Lifecycle initialized, agent state ready.")
        
        session_id = session_service.create_session()
        logger.info(f"New dialog session created: {session_id}")
        _print_status(f"Session #{session_id[:8]} started", True)
        _print_welcome(agent_version, console_user_id, session_service.actor_type)
        print("💡 Разделять смысловые темы через Ctrl+N — это улучшит качество извлечения фактов в память.")
        print("   Диалог автоматически завершается по константе неактивности и создается новый.\n")

        listener_thread = threading.Thread(
            target=_verification_event_listener,
            args=(db_config, event_queue, stop_listener, prompt_session),
            daemon=True,
            name="VerificationListener"
        )
        listener_thread.start()
        
        while True:
                try:
                    # 1. ПРОВЕРКА СОБЫТИЙ ОТ ОРКЕСТРАТОРА
                    try:
                        event_payload = event_queue.get_nowait()
                        logger.info(f"Intercepted verification event: {event_payload}")
                        try:
                            import json
                            payload = json.loads(event_payload)
                            draft_count = payload.get('draft_count', 0)
                            proposal_task_id = payload.get('task_id')
                        except Exception:
                            draft_count = 0
                            proposal_task_id = None
                            payload = {}
                        
                        if not can_propose_verification(db_config):
                            logger.debug("Verification already active or deferred, closing stale task %s", proposal_task_id[:8] if proposal_task_id else "None")
                            if proposal_task_id:
                                complete_verification_proposal_task(
                                    db_config, proposal_task_id, 'stale_proposal',
                                    {'reason': 'active_session_already_exists', 'draft_count': draft_count}
                                )
                            continue
                        
                        if draft_count > 0 and proposal_task_id:
                            hypothesis_ids = payload.get('hypothesis_ids', [])
                            # Создаём сессию как 'active'. Пока она active — оркестратор не спамит.
                            temp_session_id = create_session(
                                db_config, draft_count,
                                proposal_task_id=str(proposal_task_id),
                                hypothesis_ids=hypothesis_ids
                            )
                            
                            _print_html(f"\n<action>🔍 Найдено {draft_count} неверифицированных фактов из диалогов.</action>")
                            answer_raw = _prompt_html(
                                prompt_session,
                                "<action>Верифицировать сейчас? [Y] да / [N] отложить: </action>"
                            )
                            answer = (answer_raw or "").strip().lower()
                                                        
                            if answer in ('y', 'yes', 'д', 'да'):
                                # Передаём существующую active-сессию в режим верификации
                                run_verification_mode(
                                    db_config, session_service.actor_id, prompt_session,
                                    proposal_task_id=proposal_task_id,
                                    draft_count=draft_count,
                                    session_id=temp_session_id
                                )
                            else:
                                # ТОЛЬКО СЕЙЧАС помечаем как deferred (после ответа пользователя)
                                defer_min = get_defer_minutes(db_config)
                                defer_session(db_config, temp_session_id, defer_min)
                                complete_verification_proposal_task(
                                    db_config, proposal_task_id, 'deferred',
                                    {
                                        'draft_count': draft_count, 
                                        'defer_min': defer_min,
                                        'verification_session_id': temp_session_id,
                                        'hypothesis_ids_count': len(hypothesis_ids)
                                    }
                                )
                                _print_html(f"<warning>⏰ Верификация отложена на {defer_min:.0f} мин.</warning>\n")
                        continue  # после обработки события — проверить очередь ещё раз
                    except queue.Empty:
                        pass  # очередь пуста — идём к вводу пользователя
                    
                    # 2. СТАНДАРТНЫЙ ВВОД ПОЛЬЗОВАТЕЛЯ
                    user_input = get_user_input(prompt_session)
                    if user_input.lower() in ("exit", "выход"):
                        exit_reason = "user_command"
                        break
                    if not user_input:
                        continue
                    message_id = session_service.save_message(content=user_input)
                    from orchestrator.orchestrator_entry import on_user_message
                    try:
                        on_user_message(message_id=message_id)
                    except Exception as e:
                        logger.error(f"Failed to submit orchestrator task: {e}", exc_info=True)
                    session_service.update_activity()
                    status_text = "⚙️  Agent is thinking..."
                    print(f"\n{status_text}", end="", flush=True)
                    agent_response = session_service.wait_for_agent_response(user_message_id=message_id, timeout_seconds=300)
                    if agent_response:
                        print(f"\r{' ' * len(status_text)}\r🤖 Agent: {agent_response}\n", end="", flush=True)
                    else:
                        print(f"\r{' ' * len(status_text)}\r🤖 Agent: [No response received]\n", end="", flush=True)
                except KeyboardInterrupt:
                    exit_reason = "user_exit"
                    break
                except EOFError:
                    # === НОВОЕ: Корректный выход по Ctrl+D ===
                    exit_reason = "user_exit"
                    break
                except Exception as e:
                    logger.error(f"Error in dialog loop: {e}", exc_info=True)
                    _print_status(f"Processing error: {e}", False)
                    exit_reason = "loop_error"
                    continue
                    
    except Exception as e:
        logger.critical(f"Critical error in console interface: {e}", exc_info=True)
        exit_reason = "critical_error"
    finally:
        stop_listener.set()
        logger.info(f"Closing dialog session with reason: {exit_reason}")
        if exit_reason in ("user_command", "user_exit"):
            if session_service.actor_id:
                shutdown_type = _prompt_shutdown_reason_ui(prompt_session, lifecycle_mgr)
                lifecycle_mgr.handle_graceful_shutdown(session_service.actor_id, exit_reason, shutdown_type=shutdown_type)
        session_service.close_session(reason=exit_reason)
        _print_status("Session completed. Data saved to DB.", True)
        session_service.cleanup()
    return 0