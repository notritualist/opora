"""
main-srv/src/memory_service/topic_composer.py

Модуль выполнения задачи классификации гипотез по справочнику тем.
Модель возвращает названия тем, код сам матчит их к topic_id.
"""
version = "1.1.0"
description = "Composer for hypothesis topic classification tasks"

import json
import re
import logging
from typing import Optional
from services.service_metrics import (
    create_orchestrator_step, complete_step_success, complete_step_error,
    save_llm_metrics, save_llm_artifacts, save_reasoning,
    complete_task_success, complete_task_error,
)
from model_service.model_service import ModelService
from db_manager.db_manager import load_postgres_config
from version import __version__ as agent_version
from services.tokens_counter import count_tokens_qwen
from memory_service.hypothesis_service import (
    get_unclassified_hypotheses, get_all_topics, assign_topics_to_hypotheses,
    get_topic_classification_prompt, MAX_TOPIC_BATCH_TOKENS, MAX_TOPICS_PER_BATCH
)


logger = logging.getLogger(__name__)

# Справочник источников знаний (человекочитаемые описания для промпта)
KNOWLEDGE_SOURCE_DESCRIPTIONS = {
    'user': 'сообщил пользователь (личный факт, предпочтение, план)',
    'agent': 'предложил ассистент (совет, рекомендация, инструкция)',
    'external': 'внешний контекст (новость, статистика, закон, мировое событие)',
}


def compose_topic_classification(task_id: str, input_data: dict) -> None:
    db_config = load_postgres_config()
    step_id = None
    try:
        # 1. Загрузка промпта и справочника
        prompt_row = get_topic_classification_prompt(db_config)
        if not prompt_row:
            raise RuntimeError(f"Prompt 'hypothesis_topic_classifier' not found")
        
        prompt_id = str(prompt_row['id'])
        system_prompt_template = prompt_row['text']
        model_params = prompt_row['params'] or {}
        model_name = model_params.get("model_name")
        if not model_name:
            raise RuntimeError("Prompt has no 'model_name' in params")

        topics = get_all_topics(db_config)
        if not topics:
            logger.warning("Topics dictionary is empty, skipping classification")
            step_id = create_orchestrator_step(task_id=task_id, step_number=1, step_type_name="topic_classification", input_data={})
            complete_step_success(step_id, {"reason": "no_topics"})
            complete_task_success(task_id, {"reason": "no_topics"})
            return

        # === НОВОЕ: Загружаем описания доменов для подсказок модели ===
        from memory_service.hypothesis_service import get_active_domains_with_descriptions
        domains_list = get_active_domains_with_descriptions(db_config)
        domain_descriptions = {d['code']: d.get('description') or d['name'] for d in domains_list}
        
        
        # === НОВОЕ: Строим маппинг "Название темы (lower)" -> topic_id ===
        topic_name_to_id = {t['name'].strip().lower(): str(t['id']) for t in topics}
        
        # Форматируем справочник тем БЕЗ UUID (только название и описание)
        topics_lines = []
        for t in topics:
            desc = t.get('description') or 'без описания'
            topics_lines.append(f"- «{t['name']}» | Описание: {desc}")
        topics_list_str = "\n".join(topics_lines)
        
        # 2. Получаем неклассифицированные гипотезы
        limit = input_data.get("limit", 200)
        hypotheses = get_unclassified_hypotheses(db_config, limit=limit)
        if not hypotheses:
            logger.info("No unclassified hypotheses found")
            step_id = create_orchestrator_step(task_id=task_id, step_number=1, step_type_name="topic_classification", input_data={})
            complete_step_success(step_id, {"reason": "no_hypotheses"})
            complete_task_success(task_id, {"reason": "no_hypotheses"})
            return

        # 3. Создаём шаг
        step_id = create_orchestrator_step(
            task_id=task_id, step_number=1, step_type_name="topic_classification",
            input_data={"hypotheses_count": len(hypotheses), "topics_count": len(topics)}
        )

        # 4. Батчинг гипотез с учётом токенов
        model = ModelService()
        model_info = model.get_model_info(model_name)
        n_ctx = model_info.get("n_ctx", 16384)
        
        safe_params = {
            k: v for k, v in model_params.items()
            if k in [
                "temperature", "top_p", "top_k", "min_p", "max_tokens",
                "presence_penalty", "repetition_penalty", "stop", "chat_template_kwargs"
            ]
        }
        
        total_assigned = 0
        total_tokens = 0
        last_llm_metric_id: Optional[str] = None
        
        current_batch = []
        current_tokens = count_tokens_qwen(topics_list_str) + 1000  # Базовый оверхед промпта
        
        for h in hypotheses:
            h_str = f"ID: {h['id']} | Текст: {h['hypothesis_text']}"
            h_tokens = count_tokens_qwen(h_str)
            
            metric_id = None 
            if (current_tokens + h_tokens > MAX_TOPIC_BATCH_TOKENS or len(current_batch) >= MAX_TOPICS_PER_BATCH) and current_batch:
                assigned, tokens, metric_id = process_batch(
                    model, model_name, safe_params, n_ctx, step_id, prompt_id, 
                    system_prompt_template, topics_list_str, current_batch, topic_name_to_id, domain_descriptions
                )
                total_assigned += assigned
                total_tokens += tokens
                current_batch = []
                current_tokens = count_tokens_qwen(topics_list_str) + 1000
            
            current_batch.append(h)
            current_tokens += h_tokens
            if metric_id:
                    last_llm_metric_id = metric_id
        
        # Обрабатываем остаток
        if current_batch:
            assigned, tokens, metric_id = process_batch(
                model, model_name, safe_params, n_ctx, step_id, prompt_id, 
                system_prompt_template, topics_list_str, current_batch, topic_name_to_id, domain_descriptions
            )
            total_assigned += assigned
            total_tokens += tokens
            if metric_id:
                    last_llm_metric_id = metric_id

        output_data = {"hypotheses_processed": len(hypotheses), "topics_assigned": total_assigned, "total_tokens": total_tokens}
        complete_step_success(step_id, output_data, llm_metric_id=last_llm_metric_id)
        complete_task_success(task_id, output_data)
        logger.info(f"Topic classification completed: {total_assigned}/{len(hypotheses)} assigned")

    except Exception as exc:
        logger.exception("Error in topic_classification (task=%s): %s", task_id[:8], exc)
        if step_id: complete_step_error(step_id, "topic_composer", str(exc))
        complete_task_error(task_id, "topic_composer", str(exc))


def process_batch(model, model_name, safe_params, n_ctx, step_id, prompt_id, template, topics_str, batch_hypotheses, topic_name_to_id, domain_descriptions):
    
    """Обрабатывает один батч гипотез через LLM."""
    # === НОВОЕ: Формируем расширенное описание каждой гипотезы ===
    hypotheses_lines = []
    for h in batch_hypotheses:
        domain_code = h.get('domain_code', 'general')
        knowledge_source = h.get('knowledge_source', 'user')
        
        # Человекочитаемые описания
        domain_desc = domain_descriptions.get(domain_code, domain_code)
        source_desc = KNOWLEDGE_SOURCE_DESCRIPTIONS.get(knowledge_source, knowledge_source)
        
        hypotheses_lines.append(
            f"- ID: {h['id']} | Текст: {h['hypothesis_text']}\n"
            f"  ↳ Домен: {domain_code} ({domain_desc}) | Источник: {knowledge_source} ({source_desc})"
        )
    hypotheses_str = "\n".join(hypotheses_lines)
        
    prompt_text = template.replace("{topics_list}", topics_str).replace("{hypotheses_list}", hypotheses_str)
    messages = [{"role": "user", "content": prompt_text}]
    
    result = model.generate(messages=messages, model_name=model_name, **safe_params)
    if not result.get("success"):
        logger.error(f"LLM failed in topic batch: {result.get('error')}")
        return 0, 0, None
    
    # ИСПРАВЛЕНИЕ: проверяем оба поля (content и response)
    raw_response = result.get("content", "") or result.get("response", "")
    logger.debug(f"Topic classifier raw response (first 500 chars): {raw_response[:500]}")
    
    assignments = _parse_topic_response(raw_response, batch_hypotheses, topic_name_to_id)
    
    # Сохраняем метрики
    metrics = result.get("metrics", {})
    timings = metrics.get("timings", {})
    usage = metrics.get("usage", {})
    
    llm_metric_id = save_llm_metrics(
        orchestrator_step_id=step_id, prompt_id=prompt_id, host="main-srv",
        model=metrics.get("model", model_name), param=safe_params,
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
    save_llm_artifacts(
        llm_metric_id=llm_metric_id, orchestrator_step_id=step_id, 
        messages=messages, raw_response=raw_response, final_params=safe_params
    )
    
    # === НОВОЕ: Сохранение рассуждений (если модель их вернула) ===
    reasoning_text = result.get("reasoning_content") or result.get("reasoning")
    if reasoning_text and reasoning_text.strip():
        save_reasoning(
            orchestrator_step_id=step_id,
            content=reasoning_text.strip(),
            content_type="messages"
        )
    
    db_config = load_postgres_config()
    updated = assign_topics_to_hypotheses(db_config, assignments)
    return updated, usage.get("total_tokens", 0), llm_metric_id


def _parse_topic_response(raw_response: str, batch_hypotheses: list, topic_name_to_id: dict) -> list:
    """Парсит JSON-ответ классификатора и матчит topic_name к topic_id."""
    clean = raw_response.strip()
    if clean.startswith("```"):
        clean = re.sub(r'^```(?:json)?\n?', '', clean)
        clean = re.sub(r'\n?```$', '', clean)
    
    try:
        data = json.loads(clean)
    except json.JSONDecodeError:
        match = re.search(r'\[[\s\S]*\]', clean)
        if match:
            try: 
                data = json.loads(match.group())
            except: 
                return []
        else: 
            return []
    
    if not isinstance(data, list): 
        return []
    
    assignments = []
    hyp_ids_in_batch = {str(h['id']) for h in batch_hypotheses}
    
    for item in data:
        if not isinstance(item, dict): 
            continue
        hyp_id = item.get("hypothesis_id")
        topic_name = item.get("topic_name")
        
        if not hyp_id or str(hyp_id) not in hyp_ids_in_batch: 
            continue
        
        # Если модель вернула null / None / пустую строку — topic_id = None
        if not topic_name or str(topic_name).lower() in ('null', 'none', ''):
            assignments.append({"hypothesis_id": str(hyp_id), "topic_id": None})
        else:
            # === НОВОЕ: Матчим название темы к ID из справочника ===
            topic_name_clean = str(topic_name).strip().lower()
            # Убираем кавычки «» "", если модель их добавила
            topic_name_clean = topic_name_clean.strip('«»""\'').lower()
            
            topic_id = topic_name_to_id.get(topic_name_clean)
            
            if topic_id:
                assignments.append({"hypothesis_id": str(hyp_id), "topic_id": topic_id})
            else:
                # Попытка нестрогого поиска (если модель немного изменила название)
                matched_id = None
                for name, tid in topic_name_to_id.items():
                    if topic_name_clean in name or name in topic_name_clean:
                        matched_id = tid
                        break
                
                if matched_id:
                    assignments.append({"hypothesis_id": str(hyp_id), "topic_id": matched_id})
                else:
                    # Не нашли тему — оставляем null
                    logger.warning(f"Topic name '{topic_name}' not found in dictionary for hypothesis {hyp_id[:8]}")
                    assignments.append({"hypothesis_id": str(hyp_id), "topic_id": None})
    
    return assignments