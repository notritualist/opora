"""
main-srv/src/memory_service/verification_composer.py

Модуль выполнения задач подсистемы верификации гипотез.

Обрабатывает два типа задач оркестратора:
1. verification_proposal:
   - Проверяет наличие неотложенных (deferred) сессий верификации.
   - Считает количество draft-гипотез в memory.hypotheses.
   - Отправляет асинхронное событие через pg_notify в канал 'verification_channel'.
   - При наличии draft-гипотез задача остаётся в статусе 'running' до ответа пользователя.
   - Консоль закрывает задачу через verification_service.complete_verification_proposal_task().
   - Не использует LLM.

2. hypothesis_refinement:
   - Выполняет LLM-уточнение гипотезы на основе комментария пользователя.
   - Загружает промпт 'hypothesis_refinement' из orchestrator.prompts.
   - Вызывает ModelService для генерации уточнённого текста.
   - Сохраняет метрики LLM, артефакты и рассуждения через service_metrics.

Таблицы БД:
memory.hypotheses, memory.verification_sessions, memory.verification_actions,
orchestrator.prompts, orchestrator.orchestrator_steps, orchestrator.orchestrator_tasks,
metrics.llm_internal, metrics.llm_artifacts, orchestrator.reasonings.
"""

__version__ = "1.2.0"
__description__ = "Composer for verification and hypothesis refinement tasks"

import json
import logging
import psycopg2
import psycopg2.extensions
from psycopg2.extras import RealDictCursor


from services.service_metrics import (
    create_orchestrator_step,
    complete_step_success,
    complete_step_error,
    save_llm_metrics,
    save_llm_artifacts,
    save_reasoning,
    complete_task_success,
    complete_task_error,
)
from model_service.model_service import ModelService
from db_manager.db_manager import load_postgres_config
from version import __version__ as agent_version

logger = logging.getLogger(__name__)

REFINEMENT_PROMPT_NAME = "hypothesis_refinement"

def compose_verification_proposal(task_id: str, input_data: dict) -> None:
    """
    Выполняет задачу verification_proposal.
    Проверяет условия и отправляет pg_notify в консоль.
    
    ВАЖНО: При наличии draft-гипотез задача НЕ закрывается после отправки NOTIFY.
    Она остаётся в статусе 'running' до тех пор, пока пользователь в консоли
    не ответит на предложение верификации ([Y] или [N]).
    Консоль закрывает задачу через verification_service.complete_verification_proposal_task().
    
    Это гарантирует, что оркестратор не создаст новых задач verification_proposal
    пока текущая не обработана пользователем (проверка has_active_proposal в orchestrator.py).
    """
    db_config = load_postgres_config()
    step_id = None
    
    try:
        step_id = create_orchestrator_step(
            task_id=task_id,
            step_number=1,
            step_type_name="verification_proposal",
            input_data={"mode": input_data.get("mode", "auto")}
        )
        
        # ВАЖНО: Открываем соединение и СРАЗУ ставим AUTOCOMMIT, 
        # чтобы NOTIFY гарантированно ушел без ожидания COMMIT.
        conn = psycopg2.connect(**db_config)
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        
        try:
            with conn.cursor() as cur:
                # Проверка 1: нет ли активной или отложенной сессии
                cur.execute("""
                    SELECT 1 FROM memory.verification_sessions
                    WHERE (status = 'active'::memory.verification_session_status)
                       OR (status = 'deferred'::memory.verification_session_status AND deferred_until > NOW())
                    LIMIT 1
                """)
                if cur.fetchone():
                    complete_step_success(step_id, {"reason": "session_active_or_deferred"})
                    complete_task_success(task_id, {"reason": "session_active_or_deferred"})
                    return
                
                # Проверка 2: draft-гипотезы (ID и count для payload)
                cur.execute("""
                    SELECT id FROM memory.hypotheses
                    WHERE status = 'draft'::memory.hypothesis_status
                    ORDER BY confidence DESC, created_at ASC
                    LIMIT 200
                """)
                draft_rows = cur.fetchall()
                draft_count = len(draft_rows)
                draft_ids = [str(row[0]) for row in draft_rows]
                
                if draft_count > 0:
                    # task_id добавляем, чтобы консоль могла связать deferred/active session
                    payload = json.dumps({
                        "draft_count": draft_count,
                        "hypothesis_ids": draft_ids,
                        "task_id": task_id
                    })
                    cur.execute("NOTIFY verification_channel, %s", (payload,))
                    logger.info(
                        "Sent NOTIFY verification_channel: %d drafts (task=%s, waiting for user)",
                        draft_count, task_id[:8]
                    )
                    # output_data шага — полезные данные без task_id дубляжа (task_id есть в столбце)
                    complete_step_success(step_id, {
                        "draft_count": draft_count,
                        "draft_hypothesis_ids": draft_ids,
                    })
                    # ЗАДАЧУ НЕ ЗАКРЫВАЕМ. Остаётся running, консоль закроет её после [Y]/[N]
                    # через verification_service.complete_verification_proposal_task()
                else:
                    complete_step_success(step_id, {"reason": "no_drafts"})
                    complete_task_success(task_id, {"reason": "no_drafts"})
        finally:
            conn.close()
                    
    except Exception as exc:
        logger.exception("Error in compose_verification_proposal (task=%s): %s", task_id[:8], exc)
        if step_id:
            complete_step_error(step_id, "verification_composer", str(exc))
        complete_task_error(task_id, "verification_composer", str(exc))

def compose_hypothesis_refinement(task_id: str, input_data: dict) -> None:
    """
    Выполняет задачу hypothesis_refinement.
    LLM-уточнение гипотезы на основе комментария пользователя.
    """
    db_config = load_postgres_config()
    step_id = None
    
    try:
        hypothesis_id = input_data["hypothesis_id"]
        user_comment = input_data["user_comment"]

        # Получаем hypothesis_text, actor_id и source_message_ids из БД
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT hypothesis_text, actor_id, source_message_ids
                    FROM memory.hypotheses
                    WHERE id = %s::uuid
                """, (hypothesis_id,))
                hyp_row = cur.fetchone()
                if not hyp_row:
                    raise RuntimeError(f"Hypothesis {hypothesis_id} not found")
                hypothesis_text = hyp_row['hypothesis_text']
                actor_id = str(hyp_row['actor_id'])
                source_message_ids = hyp_row['source_message_ids'] or []
        
        # Получаем source_context из сообщений-источников
        from memory_service.verification_service import get_source_context
        source_msgs = get_source_context(db_config, source_message_ids, context_window=0)
        source_context_parts = []
        for m in source_msgs:
            role = 'User' if m.get('actor_type') == 'owner' else 'Agent'
            text = (m.get('row_text') or '').strip()
            source_context_parts.append(f"[{role}]: {text}")
        source_context = "\n".join(source_context_parts)
        
        # 1. Загрузка промпта из БД
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, text, params
                    FROM orchestrator.prompts
                    WHERE name = %s
                      AND status IN ('testing'::public.prompt_status, 'active'::public.prompt_status)
                    ORDER BY created_at DESC
                    LIMIT 1
                """, (REFINEMENT_PROMPT_NAME,))
                prompt_row = cur.fetchone()
                
                if not prompt_row:
                    raise RuntimeError(f"Prompt '{REFINEMENT_PROMPT_NAME}' not found in orchestrator.prompts")
                
                prompt_id = str(prompt_row[0])
                prompt_template = prompt_row[1]
                model_params = prompt_row[2] or {}
        
        # 2. Подстановка переменных
        prompt_text = (
            prompt_template
            .replace("{{hypothesis_text}}", hypothesis_text)
            .replace("{{source_context}}", source_context)
            .replace("{{user_comment}}", user_comment)
        )
        
        # 3. Создание шага оркестратора
        step_id = create_orchestrator_step(
            task_id=task_id,
            step_number=1,
            step_type_name="hypothesis_refinement",
            input_data={
                "hypothesis_id": input_data.get("hypothesis_id"),
                "user_comment_length": len(user_comment),
                "verification_session_id": input_data.get("verification_session_id"),
            }
        )
        
        # 4. Вызов LLM через ModelService
        model_name = model_params.get("model_name")
        if not model_name:
            raise RuntimeError(f"Prompt '{REFINEMENT_PROMPT_NAME}' has no 'model_name' in params")
        
        model = ModelService()
        model_info = model.get_model_info(model_name)
        n_ctx = model_info.get("n_ctx", 32768)
        
        safe_params = {
            k: v for k, v in model_params.items()
            if k in [
                "temperature", "top_p", "top_k", "min_p", "max_tokens",
                "presence_penalty", "repetition_penalty", "stop", "chat_template_kwargs"
            ]
        }
        
        messages_for_llm = [{"role": "user", "content": prompt_text}]
        
        result = model.generate(
            messages=messages_for_llm,
            model_name=model_name,
            **safe_params
        )
        
        if not result.get("success"):
            raise RuntimeError(f"LLM generation failed: {result.get('error')}")
        
        refined_text = (result.get("content") or result.get("response") or "").strip()
        raw_response = result.get("raw_response") or refined_text
        reasoning_text = result.get("reasoning_content") or result.get("reasoning")
        metrics = result.get("metrics", {})
        timings = metrics.get("timings", {})
        usage = metrics.get("usage", {})
        
        # 5. Сохранение метрик LLM
        llm_metric_id = save_llm_metrics(
            orchestrator_step_id=step_id,
            prompt_id=prompt_id,
            host="main-srv",
            model=metrics.get("model", model_name),
            param=safe_params,
            cache_n=timings.get("cache_n", 0),
            prompt_tokens=usage.get("prompt_tokens", 0),
            completion_tokens=usage.get("completion_tokens", 0),
            total_tokens=usage.get("total_tokens", 0),
            host_nctx=metrics.get("host_nctx", n_ctx),
            prompt_ms=timings.get("prompt_ms", 0.0),
            prompt_per_token_ms=timings.get("prompt_per_token_ms", 0.0),
            prompt_per_second=timings.get("prompt_per_second", 0.0),
            predicted_per_second=timings.get("predicted_per_second", 0.0),
            resp_time=timings.get("predicted_ms", 0.0) / 1000,
            net_latency=0.0,
            full_time=0.0
        )
        
        # 6. Сохранение артефактов
        save_llm_artifacts(
            llm_metric_id=llm_metric_id,
            orchestrator_step_id=step_id,
            messages=messages_for_llm,
            raw_response=raw_response,
            final_params=safe_params
        )
        
        # 7. Сохранение рассуждений (если есть)
        if reasoning_text and reasoning_text.strip():
            save_reasoning(
                orchestrator_step_id=step_id,
                content=reasoning_text.strip(),
                content_type="reflection"
            )
        
        # 8. Создание verification_action ПОСЛЕ LLM (здесь уже есть step_id и prompt_id)
        verification_session_id = input_data.get("verification_session_id")
        verification_action_id = None
        if verification_session_id:
            with psycopg2.connect(**db_config) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO memory.verification_actions (
                            session_id, hypothesis_id, actor_id, action_type,
                            original_text, updated_text, user_comment,
                            orchestrator_step_id, prompt_id
                        ) VALUES (
                            %s::uuid, %s::uuid, %s::uuid,
                            'edited'::memory.verification_action_type,
                            %s, %s, %s, %s::uuid, %s::uuid
                        )
                        RETURNING id
                    """, (
                        verification_session_id, hypothesis_id, actor_id,
                        hypothesis_text, refined_text, user_comment,
                        step_id, prompt_id
                    ))
                    verification_action_id = str(cur.fetchone()[0])
                    conn.commit()

        # 9. Завершение шага и задачи: output — ТОЛЬКО verification_action_id
        output_data = {
            "refined_text": refined_text,
            "verification_action_id": verification_action_id,
        }
        complete_step_success(step_id, output_data)
        complete_task_success(task_id, output_data)
        
        logger.info("Hypothesis refined successfully: task=%s", task_id[:8])
        
    except Exception as exc:
        logger.exception("Error in compose_hypothesis_refinement (task=%s): %s", task_id[:8], exc)
        if step_id:
            complete_step_error(step_id, "verification_composer", str(exc))
        complete_task_error(task_id, "verification_composer", str(exc))