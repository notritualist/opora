"""
main-srv/src/memory_service/graph_merge_composer.py
Композер задачи graph_merge_resolve.
LLM-разрешение слияний гипотез с узлами (CoT).
"""
version = "1.1.0"
description = "Graph merge resolver: LLM-driven hypothesis-to-node integration with CoT"

import json
import logging
import psycopg2
from typing import Dict, Any, Optional, Tuple
from psycopg2.extras import RealDictCursor

from db_manager.db_manager import load_postgres_config
from model_service.model_service import ModelService
from services.service_metrics import (
    create_orchestrator_step, complete_step_success, complete_step_error,
    complete_task_success, save_llm_metrics,
    save_llm_artifacts, save_reasoning, set_step_llm_metric_id, set_step_reasoning_id
)
from services.tokens_counter import count_tokens_qwen
from version import __version__ as agent_version

logger = logging.getLogger(__name__)

LLM_PROMPT_NAME = "graph_merge_resolver"
MAX_CONTEXT_TOKENS = 8192
LLM_MAX_TOKENS = 32768
BATCH_LIMIT = 3


def compose_graph_merge_resolve(task_id: str, input_data: Dict[str, Any]) -> None:
    db_config = load_postgres_config()
    step_load = create_orchestrator_step(task_id, 1, "graph_route_load", {"batch": BATCH_LIMIT})
    hypotheses = _load_pending(step_load, db_config)
    if not hypotheses:
        complete_step_success(step_load, {"loaded": 0})
        complete_task_success(task_id, output_data={"processed": 0})
        return
    complete_step_success(step_load, {"loaded": len(hypotheses)})

    processed, errors = 0, 0
    for idx, h in enumerate(hypotheses):
        try:
            _process_single(task_id, h, db_config, step_offset=idx)  # ← передать индекс
            processed += 1
        except Exception as e:
            errors += 1
            _mark_review(str(h["id"]), "pipeline_error", str(e), db_config)

    complete_task_success(task_id, output_data={"processed": processed, "errors": errors})


def _load_pending(step_id: str, db_config: dict) -> list:
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, hypothesis_text, topic_id, confidence, 
                           knowledge_source, candidate_node_id, domain_code
                    FROM memory.hypotheses 
                    WHERE graph_merge_status = 'pending_llm'
                    ORDER BY created_at 
                    LIMIT %s FOR UPDATE SKIP LOCKED
                """, (BATCH_LIMIT,))
                return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        complete_step_error(step_id, "graph_merge_composer", str(e))
        raise


def _process_single(task_id: str, h: dict, db_config: dict, step_offset: int = 0) -> None:
    h_id, n_id = str(h["id"]), h.get("candidate_node_id")
    if not n_id:
        return _mark_review(h_id, "no_candidate", "Qdrant missing", db_config)

    base_step = 2 + (step_offset * 4)
    
    step_ctx = create_orchestrator_step(task_id, base_step, "graph_route_load", {"h": h_id[:8], "n": str(n_id)[:8]})
    ctx, node = _build_context(h, n_id, db_config)
    if not ctx or not node:
        return _mark_review(h_id, "ctx_fail", "Node missing or inactive", db_config, step_ctx)  # ← передать step_ctx

    tok = count_tokens_qwen(ctx)
    if tok > MAX_CONTEXT_TOKENS:
        return _mark_review(h_id, "ctx_overflow", f"{tok}>{MAX_CONTEXT_TOKENS}", db_config, step_ctx)
    complete_step_success(step_ctx, {"tokens": tok})

    step_llm = create_orchestrator_step(task_id, base_step + 1, "graph_merge_llm", {"h": h_id[:8], "tok": tok})
    res = _call_llm(step_llm, ctx, db_config)
    if not res:
        return _mark_review(h_id, "llm_fail", "Generation error", db_config, step_llm)

    step_parse = create_orchestrator_step(task_id, base_step + 2, "graph_route_load", {"h": h_id[:8]})
    dec = _parse_json(step_parse, res["raw"], str(n_id))
    if not dec:
        return _mark_review(h_id, "json_fail", res["raw"][:100], db_config, step_parse)

    step_tx = create_orchestrator_step(task_id, base_step + 3, "graph_merge_tx", {"h": h_id[:8], "action": dec.get("action")})
    if not _exec_tx(step_tx, h, node, dec, db_config):
        _mark_review(h_id, "tx_fail", "Rollback", db_config, step_tx)


def _build_context(h: dict, n_id: str, db_config: dict) -> Tuple[Optional[str], Optional[dict]]:
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, description, context_date, topic_id, domain_id 
                    FROM memory.graph_nodes WHERE id=%s AND is_active
                """, (n_id,))
                n = cur.fetchone()
                if not n:
                    return None, None

                desc = n["description"] or ""
                node_date = str(n["context_date"]) if n["context_date"] else "null"
                hyp_date = str(h.get("context_date")) if h.get("context_date") else "null"

                ctx = (
                    f'[УЗЕЛ] id:{n["id"]}, description:"{desc}", date:{node_date}\n'
                    f'[ФАКТ] "{h["hypothesis_text"]}", source:{h["knowledge_source"]}, fact_date:{hyp_date}'
                )
                return ctx, dict(n)
    except Exception as e:
        logger.error("Ctx build failed: %s", e, exc_info=True)
        return None, None


def _call_llm(step_id: str, ctx: str, db_config: dict) -> Optional[dict]:
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT id, text, params FROM orchestrator.prompts WHERE name=%s AND status IN ('testing','active') ORDER BY created_at DESC LIMIT 1", (LLM_PROMPT_NAME,))
                p = cur.fetchone()
                if not p: raise RuntimeError("Prompt missing")
        
        params = p["params"] or {}
        # ИСПРАВЛЕНО: извлекаем model_name отдельно
        model_name = params.pop("model_name", "Qwen3.5-9B-Q4_K_M.gguf")
        params["max_tokens"] = LLM_MAX_TOKENS
        params["stop"] = ["<|im_end|>"]
        
        msgs = [{"role": "system", "content": p["text"]}, {"role": "user", "content": ctx}]
        model = ModelService()
        info = model.get_model_info(model_name)
        n_ctx = info.get("n_ctx", 32768)
        total_tok = count_tokens_qwen(p["text"]) + count_tokens_qwen(ctx)
        
        res = model.generate(messages=msgs, model_name=model_name, **params)
        
        if not res.get("success"): raise RuntimeError(res.get("error"))
        raw = res.get("response", "") or res.get("content", "")
        reason = res.get("reasoning_content") or res.get("reasoning")
        met = res.get("metrics", {})
        
        if len(raw) > LLM_MAX_TOKENS * 4 or met.get("timings", {}).get("predicted_ms", 0) > 90000:
            raise RuntimeError("LLM stuck/nonsense")
        
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
        return {"raw": raw, "prompt_id": p["id"], "llm_metric_id": m_id}
    except Exception as e:
        logger.critical("LLM call failed: %s", e, exc_info=True)
        complete_step_error(step_id, "graph_merge_composer", str(e))
        return None


def _parse_json(step_id: str, raw: str, expected_node_id: str, fallback_conf: float = 0.8) -> Optional[dict]:
    try:
        c = raw.strip()
        if c.startswith("`"):
            s, e = c.find('{'), c.rfind('}')
            if s != -1 and e != -1:
                c = c[s:e + 1]
        d = json.loads(c)
        if not isinstance(d, dict) or not {"target_node_id", "action", "relation", "needs_review"}.issubset(d.keys()):
            raise ValueError("Missing required keys")
        
        if str(d["target_node_id"]) != str(expected_node_id):
            raise ValueError(f"target_node_id mismatch: expected {expected_node_id}, got {d['target_node_id']}")
        if d["action"] not in ("merge", "link", "separate", "supersede", "review"):
            raise ValueError(f"Invalid action: {d['action']}")
            
        conf = d.get("confidence")
        if not isinstance(conf, (int, float)) or not (0.0 <= conf <= 1.0):
            logger.warning("Invalid confidence from LLM: %s. Using fallback %.2f", conf, fallback_conf)
            d["confidence"] = fallback_conf
        else:
            d["confidence"] = float(conf)
            
        # ФИКС: безопасно извлекаем причину от LLM (если есть)
        d["reason"] = (d.get("reason") or d.get("explanation") or "").strip()
        
        return d
    except Exception as e:
        complete_step_error(step_id, "graph_merge_composer", f"JSON parse/validation failed: {e}")
        return None


def _exec_tx(step_id: str, h: dict, n: dict, d: dict, db_config: dict) -> bool:
    h_id, act, conf = str(h["id"]), d["action"], d["confidence"]
    
    # ✅ ФИКС: формируем причину для трассировки
    llm_reason = d.get("reason", "")
    trace_reason = f"llm_{act}:{llm_reason[:150]}" if llm_reason else f"llm_{act}:conf_{conf:.2f}"
    
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        conn.autocommit = False
        with conn.cursor() as cur:
            if act == "review":
                cur.execute(
                    "UPDATE memory.hypotheses SET graph_merge_status='needs_review', graph_review_reason=%s WHERE id=%s",
                    (trace_reason, h_id))
                    
            elif act == "separate":
                 cur.execute("""
                     INSERT INTO memory.graph_nodes 
                     (actor_id, domain_id, topic_id, description, context_date, 
                      source_hypothesis_ids, confidence, agent_version) 
                     VALUES (NULL, %s, %s, %s, %s, %s::uuid[], %s, %s)
                     RETURNING id
                 """, (n["domain_id"], n["topic_id"], h["hypothesis_text"],
                       d.get("context_date"), [h_id], conf, agent_version))
                 new_id = str(cur.fetchone()[0])
                 cur.execute(
                     "UPDATE memory.hypotheses SET graph_merge_status='integrated', graph_review_reason=%s, candidate_node_id=%s WHERE id=%s",
                     (trace_reason, new_id, h_id))
                     
            elif act == "merge":
                 nd = d.get("new_description", n["description"])
                 cur.execute(
                     "UPDATE memory.graph_nodes SET description=%s, confidence=GREATEST(confidence,%s), updated_at=NOW() WHERE id=%s",
                     (nd, conf, n["id"]))
                 cur.execute("""
                     INSERT INTO memory.graph_node_revisions 
                     (node_id, previous_description, new_description, hypothesis_id, actor_id) 
                     VALUES (%s, %s, %s, %s, NULL)
                 """, (n["id"], n["description"], nd, h_id))
                 cur.execute(
                     "UPDATE memory.hypotheses SET graph_merge_status='integrated', graph_review_reason=%s WHERE id=%s",
                     (trace_reason, h_id))
                     
            elif act == "supersede":
                 cur.execute(
                     "UPDATE memory.graph_nodes SET is_active=FALSE, updated_at=NOW() WHERE id=%s",
                     (n["id"],))
                 cur.execute("""
                     INSERT INTO memory.graph_nodes 
                     (actor_id, domain_id, topic_id, description, context_date, 
                      source_hypothesis_ids, confidence, agent_version) 
                     VALUES (NULL, %s, %s, %s, %s, %s::uuid[], %s, %s)
                 """, (n["domain_id"], n["topic_id"], h["hypothesis_text"],
                       d.get("context_date"), [h_id], conf, agent_version))
                 cur.execute(
                     "UPDATE memory.hypotheses SET graph_merge_status='integrated', graph_review_reason=%s WHERE id=%s",
                     (trace_reason, h_id))
                     
            elif act == "link":
                 cur.execute("""
                     INSERT INTO memory.graph_nodes 
                     (actor_id, domain_id, topic_id, description, context_date, 
                      source_hypothesis_ids, confidence, agent_version) 
                     VALUES (NULL, %s, %s, %s, %s, %s::uuid[], %s, %s)
                     RETURNING id
                 """, (n["domain_id"], n["topic_id"], h["hypothesis_text"],
                       d.get("context_date"), [h_id], conf, agent_version))
                 new_id = str(cur.fetchone()[0])
                 cur.execute(
                     "UPDATE memory.hypotheses SET graph_merge_status='integrated', graph_review_reason=%s, candidate_node_id=%s WHERE id=%s",
                     (trace_reason, new_id, h_id))
                     
        conn.commit()
        complete_step_success(step_id, {"action": act, "applied_confidence": conf, "trace_reason": trace_reason})
        return True
    except Exception as e:
        logger.critical("TX failed for hyp %s: %s", h_id[:8], e, exc_info=True)
        try:
            if conn is not None:
                conn.rollback()
        except Exception:
            pass
        complete_step_error(step_id, "graph_merge_composer", str(e))
        return False
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _mark_review(h_id: str, reason: str, detail: str, db_config: dict, step_id: Optional[str] = None) -> None:
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE memory.hypotheses SET graph_merge_status='needs_review', graph_review_reason=%s WHERE id=%s",
                    (f"{reason}:{detail}", h_id))
                conn.commit()
        # Закрываем шаг с ошибкой, если он был создан
        if step_id:
            complete_step_error(step_id, "graph_merge_composer", f"{reason}:{detail}")
    except Exception as e:
        logger.error("Mark review failed %s: %s", h_id[:8], e)