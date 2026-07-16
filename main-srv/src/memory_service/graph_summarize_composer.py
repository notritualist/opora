"""
main-srv/src/memory_service/graph_summarize_composer.py

Композер оркестратора для задачи graph_summarize.
Иерархическое построение и обновление саммари узлов графа с использованием LLM (CoT).

Последовательность шагов:
1. graph_route_load: Выборка узлов, требующих обновления саммари (сортировка bottom-up).
2. graph_summarize_llm: Сборка иерархического контекста, вызов LLM (prompt: graph_node_summarizer).
3. graph_summarize_tx: Транзакция обновления summary и confidence узла.

Архитектура и правила:
- Bottom-up обход: узлы сортируются по in_degree ASC (сначала листы/зависимости, потом корни).
- Контекст включает целевой узел + входящие подузлы/зависимости 
  (рёбра: refines, depends_on, contains, related_to).
- Отсекает пустые/короткие саммари дочерних узлов (< 15 символов) для экономии токенов.
- Лимит контекста: MAX_CONTEXT_TOKENS = 10000.
- Триггер пайплайна: при успешном обновлении запускает entity_clustering.
"""

version = "1.2.0"
description = "Graph summarizer: hierarchical summary builder from child/dependent nodes (CoT)"

import json
import logging
import psycopg2
from typing import Dict, Any, List, Optional
from psycopg2.extras import RealDictCursor

from db_manager.db_manager import load_postgres_config
from model_service.model_service import ModelService
from services.service_metrics import (
    create_orchestrator_step, complete_step_success, complete_step_error,
    complete_task_success, save_llm_metrics,
    save_llm_artifacts, save_reasoning, set_step_llm_metric_id, set_step_reasoning_id
)
from services.tokens_counter import count_tokens_qwen
from services.datetime_context import build_time_block

logger = logging.getLogger(__name__)
PROMPT_NAME = "graph_node_summarizer"
BATCH_LIMIT = 10
MAX_CONTEXT_TOKENS = 10000

def compose_graph_summarize(task_id: str, input_data: Dict[str, Any]) -> None:
    db_config = load_postgres_config()
    step_sel = create_orchestrator_step(task_id, 1, "graph_route_load", {})
    nodes = _select_nodes(step_sel, db_config)
    
    if not nodes:
        complete_step_success(step_sel, {"loaded": 0})  # Завершаем шаг
        return complete_task_success(task_id, output_data={"processed": 0})
    
    # Завершаем шаг с количеством загруженных узлов
    complete_step_success(step_sel, {"loaded": len(nodes)}) 

    processed = 0
    for idx, n in enumerate(nodes):
        # ИСПРАВЛЕНО: динамические номера шагов для каждого узла
        base_step = 2 + (idx * 2)  # 2,3 для первого; 4,5 для второго; ...
        
        step_llm = create_orchestrator_step(task_id, base_step, "graph_summarize_llm", {"node": str(n["id"])[:8]})
        ctx, children = _build_hierarchical_context(n, db_config)
        if not ctx:
            complete_step_error(step_llm, "graph_summarize_composer", "Context build failed")
            continue
        tok = count_tokens_qwen(ctx)
        if tok > MAX_CONTEXT_TOKENS:
            complete_step_success(step_llm, {"tokens": tok, "status": "overflow"})
            continue
        res = _call_llm(step_llm, ctx, db_config)
        if res:
            step_tx = create_orchestrator_step(task_id, base_step + 1, "graph_summarize_tx", {"node": str(n["id"])[:8]})
            _update_summary(step_tx, str(n["id"]), res["raw"], db_config)
            processed += 1
    complete_task_success(task_id, output_data={"processed": processed})

    # === ТРИГГЕР СЛЕДУЮЩЕГО ШАГА: entity_clustering ===
    if processed > 0:
        try:
            from orchestrator.orchestrator_entry import schedule_entity_clustering
            schedule_entity_clustering(priority=0.15, parent_task_id=task_id)
            logger.info("Pipeline → entity_clustering (after summarize, processed=%d)", processed)
        except Exception as e:
            logger.error("Post-summarize entity_clustering trigger failed: %s", e)


def _select_nodes(step_id: str, db_config: dict) -> List[dict]:
    """Выбирает узлы, требующие саммари. Сортировка in_degree ASC гарантирует bottom-up обход."""
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    WITH node_degrees AS (
                        SELECT n.id, COUNT(e.source_node_id) AS in_degree
                        FROM memory.graph_nodes n
                        LEFT JOIN memory.graph_edges e ON e.target_node_id = n.id AND e.is_active = TRUE
                        WHERE n.is_active = TRUE
                          AND (n.summary IS NULL OR n.needs_summary_update = TRUE)
                        GROUP BY n.id
                    )
                    SELECT n.id, n.summary, n.description, n.context_date, nd.in_degree, n.form_code
                    FROM memory.graph_nodes n
                    JOIN node_degrees nd ON nd.id = n.id
                    ORDER BY nd.in_degree ASC, n.updated_at ASC
                    LIMIT %s FOR UPDATE OF n SKIP LOCKED
                """, (BATCH_LIMIT,))
                return cur.fetchall()
    except Exception as e:
        complete_step_error(step_id, "graph_summarize_composer", str(e))
        raise


def _build_hierarchical_context(node: dict, db_config: dict) -> tuple:
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT gn.id, gn.summary, gn.form_code, ge.relation_type
                    FROM memory.graph_edges ge
                    JOIN memory.graph_nodes gn ON gn.id = ge.source_node_id
                    WHERE ge.target_node_id = %s AND ge.is_active 
                      AND ge.relation_type IN ('refines','depends_on','contains','related_to')
                    LIMIT 15
                """, (node["id"],))
                children = cur.fetchall()
                
                # ФИКС: отсекаем пустые/короткие саммари, чтобы не жечь токены на мусор
                valid_children = [
                    {"id": str(c["id"]), 
                    "form": c.get("form_code") or "unknown",
                    "summary": c["summary"], 
                    "relation_type": c["relation_type"]}
                    for c in children
                    if c["summary"] and len(c["summary"].strip()) > 15
                ]
                
                children_json = json.dumps(valid_children, ensure_ascii=False)
                
                ctx = (
                    f'[ЦЕЛЕВОЙ УЗЕЛ] id:{node["id"]}, form:{node.get("form_code") or "unknown"}, '
                    f'current_summary:"{node["summary"] or ""}", description:"{node["description"] or ""}"\n'
                    f'[ВХОДЯЩИЕ ПОДУЗЛЫ/ЗАВИСИМОСТИ] {children_json}'
                )
                return ctx, valid_children
    except Exception as e:
        logger.error("Hierarchical ctx build failed for %s: %s", str(node["id"])[:8], e, exc_info=True)
        return None, []


def _call_llm(step_id: str, ctx: str, db_config: dict) -> Optional[Dict[str, Any]]:
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT id, text, params FROM orchestrator.prompts WHERE name=%s AND status IN ('testing','active') ORDER BY created_at DESC LIMIT 1", (PROMPT_NAME,))
                p = cur.fetchone()
                if not p: raise RuntimeError("Prompt missing")
        
        params = p["params"] or {}
        # ИСПРАВЛЕНО: извлекаем model_name отдельно
        model_name = params.pop("model_name", "Qwen3.5-9B-Q4_K_M.gguf")
        
        # Добавляем время и дату
        system_with_time = p["text"] + build_time_block("summarize")
        msgs = [{"role": "system", "content": system_with_time}, {"role": "user", "content": ctx}]
        model = ModelService()
        info = model.get_model_info(model_name)
        n_ctx = info.get("n_ctx", 32768)
        total_tok = count_tokens_qwen(p["text"]) + count_tokens_qwen(ctx)
        
        res = model.generate(messages=msgs, model_name=model_name, **params)
        
        if not res.get("success"): raise RuntimeError(res.get("error"))
        raw = res.get("response", "") or res.get("content", "")
        reason = res.get("reasoning_content") or res.get("reasoning")
        met = res.get("metrics", {})
        
        m_id = save_llm_metrics(step_id, p["id"], "main-srv", met.get("model", model_name), params,
                                met.get("timings", {}).get("cache_n", 0), total_tok,
                                met.get("usage", {}).get("completion_tokens", 0),
                                met.get("usage", {}).get("total_tokens", 0), n_ctx,
                                met.get("timings", {}).get("prompt_ms", 0),
                                met.get("timings", {}).get("prompt_per_token_ms", 0),
                                met.get("timings", {}).get("prompt_per_second", 0),
                                met.get("timings", {}).get("predicted_per_second", 0),
                                met.get("timings", {}).get("predicted_ms", 0) / 1000, 0.0, 0.0, False)
        set_step_llm_metric_id(step_id, m_id)
        save_llm_artifacts(m_id, step_id, msgs, raw, params)
        r_id = None
        if reason and reason.strip():
            r_id = save_reasoning(step_id, reason.strip(), "messages")
            if r_id: set_step_reasoning_id(step_id, r_id)
        complete_step_success(step_id, {"llm_metric_id": m_id, "reasoning_id": r_id})
        return {"raw": raw.strip(), "prompt_id": p["id"]}
    except Exception as e:
        complete_step_error(step_id, "graph_summarize_composer", str(e))
        return None


def _update_summary(step_id: str, n_id: str, raw: str, db_config: dict):
    """Парсит JSON, валидирует confidence и обновляет summary узла."""
    try:
        c = raw.strip()
        if c.startswith("`"):
            s, e = c.find('{'), c.rfind('}')
            if s != -1 and e != -1:
                c = c[s:e + 1]
        d = json.loads(c)
        if not isinstance(d, dict) or "summary" not in d:
            raise ValueError("Missing summary key")
        
        new_summ = d["summary"][:500]
        conf = d.get("confidence", 0.9)
        if not isinstance(conf, (int, float)) or not (0.0 <= conf <= 1.0):
            conf = 0.9
        conf = float(conf)
        
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE memory.graph_nodes 
                    SET summary=%s, 
                        confidence = GREATEST(confidence, %s),
                        needs_summary_update = FALSE,
                        updated_at=NOW() 
                    WHERE id=%s
                """, (new_summ, conf, n_id))
                conn.commit()
        
        complete_step_success(step_id, {"summary_len": len(new_summ), "applied_confidence": conf})
    except Exception as e:
        complete_step_error(step_id, "graph_summarize_composer", str(e))