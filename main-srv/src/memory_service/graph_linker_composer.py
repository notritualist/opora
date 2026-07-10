"""
main-srv/src/memory_service/graph_linker_composer.py

Композер задачи graph_relation_linker.
Фоновое построение логических связей внутри тем по узлам.
Последовательность шагов оркестратора:
graph_route_load   — выборка темы с активными узлами
graph_linker_llm   — вызов LLM (graph_relation_linker) с CoT, сохранение метрик/рассуждений
graph_linker_tx    — транзакция вставки рёбер
Архитектура и правила:
- enable_thinking: true. Рассуждения сохраняются.
- Связи только внутри одной темы.
- Метрики, артефакты, трассировка.
"""
version = "1.1.0"
description = "Graph relation linker: background logical edge creation within topics (CoT)"

import json
import logging
import psycopg2
from typing import Dict, Any
from psycopg2.extras import RealDictCursor

from db_manager.db_manager import load_postgres_config
from model_service.model_service import ModelService
from services.service_metrics import (
    create_orchestrator_step, complete_step_success, complete_step_error,
    complete_task_success, complete_task_error, save_llm_metrics,
    save_llm_artifacts, save_reasoning, set_step_llm_metric_id, set_step_reasoning_id
)
from services.tokens_counter import count_tokens_qwen
from version import __version__ as agent_version

logger = logging.getLogger(__name__)
PROMPT_NAME = "graph_relation_linker"
MAX_CTX = 10000
BATCH_NODES = 15


def _trigger_summarize(parent_task_id: str, db_config: dict) -> None:
    """Проверяет наличие узлов без summary и запускает summarize."""
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 1 FROM memory.graph_nodes
                    WHERE is_active = TRUE
                      AND (summary IS NULL OR length(summary) < 100)
                    LIMIT 1
                """)
                if cur.fetchone():
                    from orchestrator.orchestrator_entry import schedule_graph_summarize
                    schedule_graph_summarize(priority=0.1, parent_task_id=parent_task_id)
                    logger.info("Pipeline → graph_summarize (nodes need summary)")
    except Exception as e:
        logger.warning("Failed to trigger summarize: %s", e)
        

def compose_graph_relation_linker(task_id: str, input_data: Dict[str, Any]) -> None:
    db_config = load_postgres_config()
    step_sel = create_orchestrator_step(task_id, 1, "graph_route_load", {})
    topic_id, nodes = _select_topic_nodes(step_sel, db_config)
    if not nodes:
        complete_step_success(step_sel, {"found": 0})
        return complete_task_success(task_id, output_data={"reason": "no_topics"})
    complete_step_success(step_sel, {"topic": str(topic_id)[:8], "nodes": len(nodes)})

    # Контекст строго по промпту 2 (без summary)
    ctx = json.dumps([
        {
            "id": str(n["id"]), 
            "description": n["description"] or "", 
            "context_date": str(n["context_date"]) if n["context_date"] else "null"
        } for n in nodes
    ], ensure_ascii=False)
    if count_tokens_qwen(ctx) > MAX_CTX:
        return complete_task_error(task_id, "graph_linker_composer", "Context overflow")

    step_llm = create_orchestrator_step(task_id, 2, "graph_linker_llm", {"topic": str(topic_id)[:8]})
    res = _call_llm(step_llm, ctx, db_config)
    if not res: return complete_task_error(task_id, "graph_linker_composer", "LLM failed")

    if not topic_id:
        return complete_task_error(task_id, "graph_linker_composer", "topic_id resolved as None")
        
    step_tx = create_orchestrator_step(task_id, 3, "graph_linker_tx", {"topic": str(topic_id)[:8]})
    _exec_edges(step_tx, topic_id, nodes, res["raw"], db_config)
    complete_task_success(task_id, output_data={"topic": str(topic_id)[:8]})

    # === ТРИГГЕР СЛЕДУЮЩЕГО ШАГА ПАЙПЛАЙНА ===
    _trigger_summarize(task_id, db_config)


def _select_topic_nodes(step_id: str, db_config: dict):
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # ИСПРАВЛЕНО: >= 2 вместо > 2
                cur.execute("""
                    SELECT topic_id FROM memory.graph_nodes
                    WHERE is_active AND topic_id IS NOT NULL
                    GROUP BY topic_id HAVING count(*) >= 2
                    ORDER BY count(*) DESC LIMIT 1
                """)
                t = cur.fetchone()
                if not t: 
                    return None, []
                tid = t["topic_id"]
                cur.execute("""
                    SELECT id, description, context_date, is_active, topic_id, domain_id, source_hypothesis_ids
                    FROM memory.graph_nodes
                    WHERE topic_id=%s AND is_active
                    LIMIT %s
                """, (tid, BATCH_NODES))
                return tid, cur.fetchall()
    except Exception as e:
        complete_step_error(step_id, "graph_linker_composer", str(e))
        raise


def _call_llm(step_id: str, ctx: str, db_config: dict):
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT id, text, params FROM orchestrator.prompts WHERE name=%s AND status IN ('testing','active') ORDER BY created_at DESC LIMIT 1", (PROMPT_NAME,))
                p = cur.fetchone()
                if not p: raise RuntimeError("Prompt missing")
        
        params = p["params"] or {}
        # ИСПРАВЛЕНО: извлекаем model_name отдельно, убираем из params
        model_name = params.pop("model_name", "Qwen3.5-9B-Q4_K_M.gguf")
        
        msgs = [{"role": "system", "content": p["text"]}, {"role": "user", "content": ctx}]
        model = ModelService()
        info = model.get_model_info(model_name)
        n_ctx = info.get("n_ctx", 32768)
        total_tok = count_tokens_qwen(p["text"]) + count_tokens_qwen(ctx)
        
        # params теперь НЕ содержит model_name → нет дублирования
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
        return {"raw": raw, "prompt_id": p["id"]}
    except Exception as e:
        complete_step_error(step_id, "graph_linker_composer", str(e))
        return None


def _exec_edges(step_id: str, topic_id: str, nodes: list, raw: str, db_config: dict):
    try:
        c = raw.strip()
        if c.startswith("```"):
            s, e = c.find('['), c.rfind(']')
            if s!=-1 and e!=-1: c = c[s:e+1]
        edges = json.loads(c)
        if not isinstance(edges, list): raise ValueError("Not list")
        
        # Валидация UUID из ответа модели
        valid_ids = {str(n["id"]) for n in nodes}
        
        with psycopg2.connect(**db_config) as conn:
            conn.autocommit = False
            with conn.cursor() as cur:
                created = 0
                for e in edges:
                    s_id, t_id, rel = e.get("source_id"), e.get("target_id"), e.get("relation")
                    if s_id in valid_ids and t_id in valid_ids and s_id != t_id:
                        # Валидация confidence из ответа модели
                        conf = e.get("confidence", 0.8)
                        if not isinstance(conf, (int, float)) or not (0.0 <= conf <= 1.0):
                            conf = 0.8
                        conf = float(conf)
                        
                        # Собираем source_hypothesis_ids из обоих узлов связи
                        src_hyp_ids = []
                        for n in nodes:
                            nid = str(n["id"])
                            if nid == s_id or nid == t_id:
                                hyp_ids = n.get("source_hypothesis_ids")
                                if hyp_ids:
                                    # Фикс: если драйвер вернул строку "{uuid,uuid}", extend разобьёт её по символам.
                                    if isinstance(hyp_ids, str):
                                        clean = hyp_ids.strip("{}")
                                        if clean:
                                            src_hyp_ids.extend([uid.strip() for uid in clean.split(",")])
                                    elif isinstance(hyp_ids, (list, tuple)):
                                        src_hyp_ids.extend(hyp_ids)
                        
                        # Дедупликация + жёсткая фильтрация формата UUID (отсекает '3', '}', '-' и прочий мусор)
                        src_hyp_ids = list({
                            str(uid) for uid in src_hyp_ids 
                            if uid and len(uid) == 36 and uid.count('-') == 4
                        })

                        cur.execute("""
                            INSERT INTO memory.graph_edges (actor_id,source_node_id,target_node_id,relation_type,source_hypothesis_ids,confidence,needs_review,agent_version)
                            VALUES (NULL,%s,%s,%s,%s::uuid[],%s,%s,%s) ON CONFLICT DO NOTHING
                        """, (s_id, t_id, rel, src_hyp_ids, conf, e.get("needs_review",False), agent_version))
                        created += 1
                conn.commit()
                # Помечаем target-узлы для пересуммаризации при появлении новых рёбер
                if created > 0:
                    target_ids = list({str(e.get("target_id")) for e in edges if e.get("target_id")})
                    if target_ids:
                        with conn.cursor() as c:
                            c.execute("""
                                UPDATE memory.graph_nodes 
                                SET needs_summary_update = TRUE, updated_at = NOW()
                                WHERE id = ANY(%s::uuid[]) AND summary IS NOT NULL
                            """, (target_ids,))
                        conn.commit()
                        
        complete_step_success(step_id, {"edges_created": created})
    except Exception as e:
        complete_step_error(step_id, "graph_linker_composer", str(e))