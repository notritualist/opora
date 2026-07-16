"""
main-srv/src/memory_service/form_composer.py

Композер задачи form_classification: LLM-классификация гипотез по структурным формам.

Логика работы и архитектура классификации:
1. Подготовка данных:
   - Загружает до 100 гипотез с form_code IS NULL.
   - Загружает справочник форм (fact, goal, task, project, entity, skill, event) 
     и активный промпт 'hypothesis_form_classifier' из БД.
2. Динамический чанкинг (Token-aware chunking):
   - Разбивает гипотезы на батки, строго контролируя сумму токенов (лимит MAX_FORM_BATCH_TOKENS) 
     и количество гипотез в батке (MAX_FORMS_PER_BATCH), чтобы гарантировать попадание в n_ctx.
3. Вызов LLM и трассировка:
   - Формирует messages, подставляя список форм в system prompt.
   - Вызывает ModelService, сохраняет метрики (timings, usage), артефакты и reasoning_content.
4. Робастный парсинг и валидация:
   - Очищает ответ от markdown-оберток (```json ... ```).
   - Извлекает JSON-массив, сопоставляет hypothesis_id с ожидаемыми ID.
   - Жестко фильтрует form_code: если код не входит в whitelist, присваивает NULL.
5. Обновление БД:
   - Массово обновляет form_code для обработанных гипотез в памяти/графе.

Результат:
    Структурирование неформализованных гипотез, что позволяет применять 
    специфичные бизнес-логики и промпты для разных типов сущностей на следующих этапах.
"""
version = "1.1.0"
description = "Composer for form classification of hypotheses"

import json
import re
import logging
from typing import List, Dict, Any, Optional

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
from services.tokens_counter import count_tokens_qwen
from model_service.model_service import ModelService
from db_manager.db_manager import load_postgres_config
from version import __version__ as agent_version

logger = logging.getLogger(__name__)


def _render_forms_list(forms: List[Dict[str, str]]) -> str:
    lines = []
    for f in forms:
        desc = f.get('description') or f.get('name') or ''
        lines.append(f'• "{f["code"]}" — {f["name"]}. {desc}')
    return '\n'.join(lines)


def _render_hypotheses_list(hyps: List[Dict[str, Any]]) -> str:
    lines = []
    for h in hyps:
        lines.append(
            f'- id: {h["id"]}, domain: {h["domain_code"]}, source: {h["knowledge_source"]}, '
            f'text: "{h["hypothesis_text"]}"'
        )
    return '\n'.join(lines)


def _parse_forms_response(raw_response: str, expected_ids: List[str]) -> List[Dict[str, Any]]:
    """Парсит JSON-ответ классификатора форм с защитой от сбоев."""
    if not raw_response or not raw_response.strip():
        logger.warning("Empty response from form classifier LLM")
        return []

    clean = raw_response.strip()
    data = None

    if clean.startswith("```"):
        lines = clean.split("\n")
        start = 1
        end = -1 if lines[-1].strip().startswith("```") else len(lines)
        clean = "\n".join(lines[start:end]).strip()

    try:
        data = json.loads(clean)
    except json.JSONDecodeError:
        match = re.search(r'\[[\s\S]*\]', clean)
        if match:
            try:
                data = json.loads(match.group())
            except json.JSONDecodeError:
                logger.error(f"Failed to parse form classifier response: {clean[:500]}")
                return []
        else:
            logger.error(f"No JSON array in form classifier response: {clean[:500]}")
            return []

    if not isinstance(data, list):
        logger.warning(f"Expected list, got {type(data).__name__}")
        return []

    valid_forms = {'fact', 'goal', 'task', 'project', 'entity', 'skill', 'event'}
    result = []
    expected_set = set(expected_ids)

    for item in data:
        if not isinstance(item, dict):
            continue
        hyp_id = item.get('hypothesis_id')
        form_code = item.get('form_code')

        if not hyp_id or not isinstance(hyp_id, str):
            continue
        if hyp_id not in expected_set:
            continue

        if form_code is None:
            result.append({"hypothesis_id": hyp_id, "form_code": None})
            continue

        if not isinstance(form_code, str) or form_code not in valid_forms:
            logger.debug(f"Invalid form_code '{form_code}' for {hyp_id[:8]}, setting null")
            result.append({"hypothesis_id": hyp_id, "form_code": None})
            continue

        result.append({"hypothesis_id": hyp_id, "form_code": form_code})

    logger.info(f"Parsed {len(result)}/{len(data)} form assignments")
    return result


def compose_form_classification(task_id: str, input_data: dict) -> None:
    """Выполняет задачу классификации гипотез по формам."""
    from memory_service.hypothesis_service import (
        get_hypotheses_without_form,
        get_all_forms,
        get_form_classification_prompt,
        assign_forms_to_hypotheses,
        MAX_FORM_BATCH_TOKENS,
        MAX_FORMS_PER_BATCH,
    )

    db_config = load_postgres_config()
    step_id = None
    last_llm_metric_id: Optional[str] = None

    try:
        # === 1. Загружаем промпт ===
        prompt_row = get_form_classification_prompt(db_config)
        if not prompt_row:
            raise RuntimeError("Prompt 'hypothesis_form_classifier' not found")

        prompt_id = str(prompt_row['id'])
        system_prompt_template = prompt_row['text']
        model_params = prompt_row['params'] or {}

        model_name = model_params.get("model_name")
        if not model_name:
            raise RuntimeError("Form classifier prompt has no 'model_name'")

        # === 2. Получаем гипотезы без формы ===
        hypotheses = get_hypotheses_without_form(db_config, limit=100)
        if not hypotheses:
            logger.info("No hypotheses without form_code")
            step_id = create_orchestrator_step(
                task_id=task_id, step_number=1,
                step_type_name="form_classification",
                input_data={"hypotheses_count": 0}
            )
            complete_step_success(step_id, output_data={"assigned": 0, "reason": "no_hypotheses"})
            complete_task_success(task_id, output_data={"assigned": 0, "reason": "no_hypotheses"})
            return

        # === 3. Справочник форм ===
        forms = get_all_forms(db_config)
        if not forms:
            raise RuntimeError("Forms dictionary is empty")

        forms_list_str = _render_forms_list(forms)
        system_prompt = system_prompt_template.replace("{forms_list}", forms_list_str)

        # === 4. Шаг оркестратора ===
        step_id = create_orchestrator_step(
            task_id=task_id, step_number=1,
            step_type_name="form_classification",
            input_data={"hypotheses_count": len(hypotheses)}
        )

        # === 5. ModelService ===
        model = ModelService()
        model_info = model.get_model_info(model_name)
        n_ctx = model_info.get("n_ctx", 32768)

        safe_params = {
            k: v for k, v in model_params.items()
            if k in ["temperature", "top_p", "top_k", "min_p", "max_tokens",
                     "presence_penalty", "repetition_penalty", "stop", "chat_template_kwargs"]
        }

        # === 6. Чанкинг по токенам ===
        chunks: List[List[Dict[str, Any]]] = []
        current_chunk: List[Dict[str, Any]] = []
        current_tokens = 0

        for h in hypotheses:
            h_tokens = count_tokens_qwen(h['hypothesis_text'])
            if current_tokens + h_tokens > MAX_FORM_BATCH_TOKENS or len(current_chunk) >= MAX_FORMS_PER_BATCH:
                if current_chunk:
                    chunks.append(current_chunk)
                current_chunk = [h]
                current_tokens = h_tokens
            else:
                current_chunk.append(h)
                current_tokens += h_tokens
        if current_chunk:
            chunks.append(current_chunk)

        logger.info(f"Form classification: {len(hypotheses)} hypotheses → {len(chunks)} chunks")

        total_assigned = 0
        total_tokens_used = 0

        for chunk_idx, chunk in enumerate(chunks):
            hyps_list_str = _render_hypotheses_list(chunk)
            user_prompt = (
                f"Проанализируй гипотезы и присвой каждой наиболее подходящую форму.\n\n"
                f"--- ГИПОТЕЗЫ ({len(chunk)} шт.) ---\n{hyps_list_str}\n--- КОНЕЦ ---\n\n"
                f"Верни JSON-массив согласно формату в системном промпте."
            )

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
                logger.error(f"ModelService failed on chunk {chunk_idx}: {e}", exc_info=True)
                continue

            if not result.get("success"):
                logger.error(f"LLM failure on chunk {chunk_idx}: {result.get('error')}")
                continue

            raw_response = result.get("response", "") or result.get("content", "")
            reasoning_text = result.get("reasoning_content") or result.get("reasoning")

            chunk_ids = [str(h['id']) for h in chunk]
            assignments = _parse_forms_response(raw_response, chunk_ids)

            saved = assign_forms_to_hypotheses(db_config, assignments)
            total_assigned += saved

            # === Метрики ===
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
            last_llm_metric_id = llm_metric_id
            total_tokens_used += usage.get("total_tokens", 0)

            save_llm_artifacts(
                llm_metric_id=llm_metric_id,
                orchestrator_step_id=step_id,
                messages=messages_for_llm,
                raw_response=raw_response,
                final_params=safe_params
            )

            if reasoning_text and reasoning_text.strip():
                save_reasoning(
                    orchestrator_step_id=step_id,
                    content=reasoning_text.strip(),
                    content_type="messages"
                )

        output_data = {
            "assigned": total_assigned,
            "hypotheses_total": len(hypotheses),
            "chunks": len(chunks),
            "total_tokens": total_tokens_used
        }
        complete_step_success(step_id, output_data=output_data, llm_metric_id=last_llm_metric_id)
        complete_task_success(task_id, output_data=output_data)
        logger.info(f"Form classification completed: {output_data}")

    except Exception as exc:
        logger.exception("Error in form_classification (task_id=%s): %s", task_id[:8], exc)
        if step_id:
            complete_step_error(step_id, error_module="form_composer", error_message=str(exc))
        complete_task_error(
            task_id=task_id,
            error_module="form_composer",
            error_message=str(exc)
        )