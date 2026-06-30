"""
main-srv/src/memory_service/memory_composer.py
Модуль выполнения задачи извлечения гипотез в долговременную память.
Аналог response_composer.py для пайплайна user_answer_generation.
Логика:
- Загрузка промпта экстракции из orchestrator.prompts.
- Выборка необработанных сообщений из закрытых диалогов.
- Чанкинг сообщений по токенам (защита от переполнения контекста).
- Вызов ModelService для извлечения гипотез с полями:
  * knowledge_source (user/agent/external) — источник знания
  * entity (slug) — идентификатор сущности для связности
  * relation (property/recommendation/preference/constraint) — тип отношения
- Сохранение гипотез в memory.hypotheses.
- Запись метрик LLM, артефактов, рассуждений через service_metrics.
- Пометка сообщений как обработанных в memory.message_analyses.
- Завершение задачи и шага оркестратора.
Не выполняет прямой работы с очередью задач — это делает orchestrator.
"""
version = "1.0.0"
description = "Composer for memory extraction tasks"

import logging

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
        
def compose_memory_extraction(task_id: str, input_data: dict) -> None:
    """
    Выполняет задачу извлечения гипотез в долговременную память.
    Обрабатывает ЗАКРЫТЫЕ диалоги по одному, не смешивая сообщения из разных диалогов.
    
    Логика:
    1. Загружает промпт экстракции из orchestrator.prompts.
    2. Получает список необработанных закрытых диалогов.
    3. Для каждого диалога:
       - Получает его сообщения
       - Чанкует по токенам
       - Извлекает гипотезы через LLM
       - Сохраняет гипотезы, метрики, артефакты
       - Помечает сообщения как обработанные
    4. Завершает задачу и шаг оркестратора.
    """
    from memory_service.hypothesis_service import (
        get_unprocessed_dialogues,
        get_dialogue_messages,
        get_already_processed_ids,
        get_extraction_prompt,
        save_hypotheses,
        mark_messages_analyzed,
        chunk_messages_by_tokens,
        render_extraction_prompt,
        build_extraction_user_prompt,
        parse_extraction_response
    )
    
    db_config = load_postgres_config()
    step_id = None
    
    try:
        # === 1. Загружаем промпт экстракции из БД ===
        prompt_row = get_extraction_prompt(db_config)
        if not prompt_row:
            raise RuntimeError(
                "Extraction prompt 'memory_hypothesis_extractor' "
                "not found in orchestrator.prompts"
            )
        
        prompt_id = str(prompt_row['id'])
        system_prompt_template = prompt_row['text']
        model_params = prompt_row['params'] or {}
        
        system_prompt = render_extraction_prompt(system_prompt_template, db_config)
        
        model_name = model_params.get("model_name")
        if not model_name:
            raise RuntimeError("Extraction prompt has no 'model_name' in params")
        
        # === 2. Получаем список необработанных диалогов ===
        dialogue_ids = get_unprocessed_dialogues(db_config, limit=5)
        
        if not dialogue_ids:
            logger.info("No unprocessed closed dialogues for memory extraction")
            step_id = create_orchestrator_step(
                task_id=task_id,
                step_number=1,
                step_type_name="memory_extraction",
                input_data={"dialogues_count": 0}
            )
            complete_step_success(
                step_id,
                output_data={"hypotheses_count": 0, "reason": "no_dialogues"}
            )
            complete_task_success(
                task_id,
                output_data={"hypotheses_count": 0, "reason": "no_dialogues"}
            )
            return
        
        logger.info(f"Found {len(dialogue_ids)} unprocessed dialogues")
        
        # === 3. Создаём шаг оркестратора ===
        step_id = create_orchestrator_step(
            task_id=task_id,
            step_number=1,
            step_type_name="memory_extraction",
            input_data={"dialogues_count": len(dialogue_ids)}
        )
        
        # === 4. Инициализация ModelService ===
        model = ModelService()
        model_info = model.get_model_info(model_name)
        n_ctx = model_info.get("n_ctx", 32768)
        
        # Фильтруем параметры для LLM
        safe_params = {
            k: v for k, v in model_params.items()
            if k in [
                "temperature", "top_p", "top_k", "min_p", "max_tokens",
                "presence_penalty", "repetition_penalty", "stop", "chat_template_kwargs"
            ]
        }
        
        total_hypotheses = 0
        total_tokens_used = 0
        total_dialogues_processed = 0
        total_chunks_with_errors = 0
        
        # === 5. Обрабатываем каждый диалог ОТДЕЛЬНО ===
        for dialogue_id in dialogue_ids:
            logger.info(f"Processing dialogue {dialogue_id[:8]}")
            
            # 5.1. Получаем сообщения диалога
            messages = get_dialogue_messages(db_config, dialogue_id)
            
            if not messages:
                logger.warning(f"Dialogue {dialogue_id[:8]} is empty, skipping")
                continue
            
            # 5.2. Двойная проверка уже обработанных сообщений
            all_ids = [str(m['id']) for m in messages]
            already_processed = get_already_processed_ids(db_config, all_ids)
            messages = [m for m in messages if str(m['id']) not in already_processed]
            
            if not messages:
                logger.info(f"All messages in dialogue {dialogue_id[:8]} already processed")
                continue
            
            # 5.3. Определяем actor_id для привязки гипотез
            actor_id = None
            for m in messages:
                if m['actor_type'] in ('user', 'owner'):
                    actor_id = str(m['actor_id'])
                    break
            if not actor_id:
                actor_id = str(messages[0]['actor_id'])
            
            # 5.4. Чанкинг сообщений по токенам
            chunks = chunk_messages_by_tokens(messages)
            logger.info(
                f"Dialogue {dialogue_id[:8]}: {len(messages)} messages → {len(chunks)} chunks"
            )
            
            # 5.5. Обработка каждого чанка
            for chunk_idx, chunk in enumerate(chunks):
                user_prompt, chunk_msg_ids = build_extraction_user_prompt(chunk)
                
                messages_for_llm = [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ]
                
                try:
                    result = model.generate(
                        messages=messages_for_llm,
                        model_name=model_name,
                        **safe_params
                    )
                except Exception as e:
                    logger.error(
                        f"ModelService failed on dialogue {dialogue_id[:8]}, chunk {chunk_idx}: {e}",
                        exc_info=True
                    )
                    total_chunks_with_errors += 1
                    continue
                
                if not result.get("success"):
                    logger.error(
                        f"LLM returned failure on dialogue {dialogue_id[:8]}, chunk {chunk_idx}: "
                        f"{result.get('error')}"
                    )
                    total_chunks_with_errors += 1
                    continue
                
                raw_response = result.get("response", "") or result.get("content", "")
                reasoning_text = result.get("reasoning_content") or result.get("reasoning")
                
                # Парсинг гипотез
                hypotheses = parse_extraction_response(raw_response, chunk_msg_ids)
                
                # Сохранение гипотез
                saved_count = save_hypotheses(
                    db_config=db_config,
                    hypotheses=hypotheses,
                    actor_id=actor_id,
                    orchestrator_step_id=step_id,
                    prompt_id=prompt_id,
                    agent_version=agent_version
                )
                total_hypotheses += saved_count
                
                # Сохранение LLM-метрик
                metrics = result.get("metrics", {})
                timings = metrics.get("timings", {})
                usage = metrics.get("usage", {})
                
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
                total_tokens_used += usage.get("total_tokens", 0)
                
                # Сохранение артефактов
                save_llm_artifacts(
                    llm_metric_id=llm_metric_id,
                    orchestrator_step_id=step_id,
                    messages=messages_for_llm,
                    raw_response=raw_response,
                    final_params=safe_params
                )
                
                # Сохранение рассуждений
                if reasoning_text and reasoning_text.strip():
                    save_reasoning(
                        orchestrator_step_id=step_id,
                        content=reasoning_text.strip(),
                        content_type="messages"
                    )
                
                # Помечаем сообщения как обработанные
                mark_messages_analyzed(
                    db_config=db_config,
                    message_ids=chunk_msg_ids,
                    hypotheses_count=saved_count,
                    tokens_used=usage.get("total_tokens", 0),
                    orchestrator_step_id=step_id,
                    llm_metric_id=llm_metric_id,
                    prompt_id=prompt_id,
                    agent_version=agent_version
                )
            
            total_dialogues_processed += 1
        
        # === 6. Завершение шага и задачи ===
        output_data = {
            "hypotheses_count": total_hypotheses,
            "dialogues_processed": total_dialogues_processed,
            "chunks_with_errors": total_chunks_with_errors,
            "total_tokens": total_tokens_used
        }
        complete_step_success(step_id, output_data=output_data)
        complete_task_success(task_id, output_data=output_data)
        logger.info(
            f"Memory extraction completed: {total_hypotheses} hypotheses from "
            f"{total_dialogues_processed} dialogues, {total_tokens_used} tokens"
        )
        
    except Exception as exc:
        logger.exception(
            "Error in memory_extraction (task_id=%s): %s", task_id[:8], exc
        )
        if step_id:
            complete_step_error(
                step_id, error_module="memory_composer", error_message=str(exc)
            )
        complete_task_error(
            task_id=task_id,
            error_module="memory_composer",
            error_message=str(exc)
        )