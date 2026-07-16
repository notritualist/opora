"""
main-srv/src/memory_service/graph_merge_composer.py

Композер оркестратора для задачи graph_merge_resolve.
LLM-разрешение слияний гипотез с существующими узлами графа с использованием Chain-of-Thought (CoT).

Логика пайплайна:
1. graph_route_load: Выборка батча гипотез со статусом 'pending_llm'.
2. graph_merge_ctx: Сборка контекста (описание узла + текст гипотезы + даты).
3. graph_merge_llm: Вызов LLM (prompt: graph_merge_resolver) с CoT.
4. graph_merge_parse: Парсинг JSON-ответа, валидация действия и confidence.
5. graph_merge_tx: Транзакционное применение решения в БД.

Архитектурные особенности:
- Защита от несовпадения форм (form_code): если форма гипотезы и узла отличаются, 
  LLM не вызывается, гипотеза сразу становится отдельным узлом (separate).
- Поддерживаемые действия LLM: merge, link, separate, supersede, review, contradicts.
- Post-commit синхронизация: после успешного COMMIT новые/обновленные узлы 
  асинхронно синхронизируются с Qdrant через graph_node_sync.
- Триггер пайплайна: при успешном merge/link/supersede запускает graph_relation_linker.
"""
version = "1.2.0"
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
from services.datetime_context import build_time_block

logger = logging.getLogger(__name__)

LLM_PROMPT_NAME = "graph_merge_resolver"
MAX_CONTEXT_TOKENS = 10000
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

    # Если были успешные merge → новые описания → нужен linker
    try:
        if processed > 0:
            from orchestrator.orchestrator_entry import schedule_graph_relation_linker
            schedule_graph_relation_linker(priority=0.2, parent_task_id=task_id)
            logger.info("Pipeline → graph_relation_linker (after merge, processed=%d)", processed)
    except Exception as e:
        logger.error("Post-merge linker trigger failed: %s", e)


def _load_pending(step_id: str, db_config: dict) -> list:
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, hypothesis_text, topic_id, confidence,
                        knowledge_source, candidate_node_id, domain_code, form_code
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

    # === ЗАЩИТА: несовпадение форм → отдельный узел без LLM ===
    if h.get("form_code"):
        try:
            with psycopg2.connect(**db_config) as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        "SELECT form_code FROM memory.graph_nodes WHERE id=%s",
                        (n_id,)
                    )
                    node_row = cur.fetchone()
                    if node_row and node_row.get("form_code") and node_row["form_code"] != h["form_code"]:
                        logger.info(
                            "Form mismatch (node=%s, hyp=%s) → separate without LLM for hyp %s",
                            node_row["form_code"], h["form_code"], h_id[:8]
                        )
                        _create_separate_node(h_id, h, {"domain_id": None, "topic_id": None}, db_config, n_id)
                        return
        except Exception as e:
            logger.warning("Form check failed for %s: %s", h_id[:8], e)

    base_step = 2 + (step_offset * 4)
    
    step_ctx = create_orchestrator_step(task_id, base_step, "graph_merge_ctx", {"h": h_id[:8], "n": str(n_id)[:8]})
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

    step_parse = create_orchestrator_step(task_id, base_step + 2, "graph_merge_parse", {"h": h_id[:8]})
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
                    SELECT id, description, context_date, topic_id, domain_id, form_code
                    FROM memory.graph_nodes WHERE id=%s AND is_active
                """, (n_id,))
                n = cur.fetchone()
                if not n:
                    return None, None

                desc = n["description"] or ""
                node_date = str(n["context_date"]) if n["context_date"] else "null"
                # Дата факта: из context_date (если есть) или из created_at гипотезы
                hyp_date = "null"
                if h.get("context_date"):
                    hyp_date = str(h["context_date"])
                elif h.get("created_at"):
                    ca = h["created_at"]
                    if hasattr(ca, 'strftime'):
                        hyp_date = ca.strftime("%Y-%m-%d")
                    elif isinstance(ca, str) and len(ca) >= 10:
                        hyp_date = ca[:10]
                node_form = n.get("form_code") or "unknown"
                hyp_form = h.get("form_code") or "unknown"

                ctx = (
                    f'[УЗЕЛ] id:{n["id"]}, form:{node_form}, description:"{desc}", date:{node_date}\n'
                    f'[ФАКТ] form:{hyp_form}, "{h["hypothesis_text"]}", source:{h["knowledge_source"]}, fact_date:{hyp_date}'
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
        
        params = dict(p["params"] or {})  # ← КОПИЯ, чтобы не мутировать оригинал из БД
        model_name = params.pop("model_name", "Qwen3.5-9B-Q4_K_M.gguf")
        params["max_tokens"] = LLM_MAX_TOKENS
        params["stop"] = ["<|im_end|>"]
        
        # Добавляем время и дату
        system_with_time = p["text"] + build_time_block("merge")
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
        
        # Защита от зависания: 5 минут (300с) для CoT-модели, 4x max_tokens для длины
        predicted_ms = met.get("timings", {}).get("predicted_ms", 0)
        if len(raw) > LLM_MAX_TOKENS * 4 or predicted_ms > 300000:
            logger.warning(
                "LLM output too large/slow for hyp %s: len=%d, time=%.1fs",
                step_id[:8], len(raw), predicted_ms / 1000
            )
            raise RuntimeError(f"LLM stuck/nonsense (len={len(raw)}, time={predicted_ms/1000:.1f}s)")
        
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
        if d["action"] not in ("merge", "link", "separate", "supersede", "review", "contradicts"):
            raise ValueError(f"Invalid action: {d['action']}")
            
        conf = d.get("confidence")
        if not isinstance(conf, (int, float)) or not (0.0 <= conf <= 1.0):
            logger.warning("Invalid confidence from LLM: %s. Using fallback %.2f", conf, fallback_conf)
            d["confidence"] = fallback_conf
        else:
            d["confidence"] = float(conf)
            
        # ФИКС: безопасно извлекаем причину от LLM (если есть)
        d["reason"] = (d.get("reason") or d.get("explanation") or "").strip()

        # Явное завершение шага при успешном парсинге
        complete_step_success(step_id, {"action": d["action"], "confidence": d["confidence"]})
                
        return d
    except Exception as e:
        complete_step_error(step_id, "graph_merge_composer", f"JSON parse/validation failed: {e}")
        return None


def _exec_tx(step_id: str, h: dict, n: dict, d: dict, db_config: dict) -> bool:
    h_id, act, conf = str(h["id"]), d["action"], d["confidence"]
    
    # Формируем причину для трассировки
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
                 # Fallback: если LLM не вернул context_date — берём created_at гипотезы
                 merge_ctx_date = d.get("context_date")
                 if not merge_ctx_date:
                     try:
                         cur.execute("SELECT created_at::date FROM memory.hypotheses WHERE id = %s", (h_id,))
                         r = cur.fetchone()
                         if r and r[0]:
                             merge_ctx_date = r[0]
                     except Exception:
                         pass
                 
                 sep_needs_binding = (h.get("form_code") == 'fact')
                 cur.execute("""
                     INSERT INTO memory.graph_nodes 
                     (actor_id, domain_id, topic_id, form_code, description, context_date, 
                     source_hypothesis_ids, confidence, agent_version, needs_entity_binding)
                     VALUES (NULL, %s, %s, %s, %s, %s, %s::uuid[], %s, %s, %s)
                     RETURNING id
                 """, (n["domain_id"], n["topic_id"], h.get("form_code"), h["hypothesis_text"],
                       merge_ctx_date, [h_id], conf, agent_version, sep_needs_binding))
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
                    (node_id, previous_description, new_description, hypothesis_id, actor_id, form_code) 
                    VALUES (%s, %s, %s, %s, NULL, %s)
                """, (n["id"], n["description"], nd, h_id, n.get("form_code")))
                 cur.execute(
                     "UPDATE memory.hypotheses SET graph_merge_status='integrated', graph_review_reason=%s WHERE id=%s",
                     (trace_reason, h_id))
                     
            elif act == "supersede":
                 # Fallback: если LLM не вернул context_date — берём created_at гипотезы
                 supersede_ctx_date = d.get("context_date")
                 if not supersede_ctx_date:
                     try:
                         cur.execute("SELECT created_at::date FROM memory.hypotheses WHERE id = %s", (h_id,))
                         r = cur.fetchone()
                         if r and r[0]:
                             supersede_ctx_date = r[0]
                     except Exception:
                         pass
                     
                 cur.execute(
                     "UPDATE memory.graph_nodes SET is_active=FALSE, updated_at=NOW() WHERE id=%s",
                     (n["id"],))
                 sup_needs_binding = (h.get("form_code") == 'fact')
                 cur.execute("""
                     INSERT INTO memory.graph_nodes 
                     (actor_id, domain_id, topic_id, form_code, description, context_date, 
                     source_hypothesis_ids, confidence, agent_version, needs_entity_binding)
                     VALUES (NULL, %s, %s, %s, %s, %s, %s::uuid[], %s, %s, %s)
                    RETURNING id
                 """, (n["domain_id"], n["topic_id"], h.get("form_code"), h["hypothesis_text"],
                       supersede_ctx_date, [h_id], conf, agent_version, sup_needs_binding))
                 cur.execute(
                     "UPDATE memory.hypotheses SET graph_merge_status='integrated', graph_review_reason=%s WHERE id=%s",
                     (trace_reason, h_id))
                     
            elif act == "link":
                if d.get('needs_review'):
                    # Противоречие или неуверенность → НЕ создаём узел, просто метим на ручную проверку
                    cur.execute(
                        "UPDATE memory.hypotheses SET graph_merge_status='needs_review', graph_review_reason=%s WHERE id=%s",
                        (trace_reason, h_id))
                else:
                    # Обычная связь → создаём узел и связь, гипотеза интегрирована
                    link_ctx_date = d.get("context_date")
                    if not link_ctx_date:
                        try:
                            cur.execute("SELECT created_at::date FROM memory.hypotheses WHERE id = %s", (h_id,))
                            r = cur.fetchone()
                            if r and r[0]:
                                link_ctx_date = r[0]
                        except Exception:
                            pass

                    lnk_needs_binding = (h.get("form_code") == 'fact')
                    cur.execute("""
                        INSERT INTO memory.graph_nodes 
                        (actor_id, domain_id, topic_id, form_code, description, context_date, 
                        source_hypothesis_ids, confidence, agent_version, needs_entity_binding)
                        VALUES (NULL, %s, %s, %s, %s, %s, %s::uuid[], %s, %s, %s)
                        RETURNING id
                    """, (n["domain_id"], n["topic_id"], h.get("form_code"), h["hypothesis_text"],
                        link_ctx_date, [h_id], conf, agent_version, lnk_needs_binding))
                    new_id = str(cur.fetchone()[0])

                    cur.execute(
                        "UPDATE memory.hypotheses SET graph_merge_status='integrated', graph_review_reason=%s WHERE id=%s",
                        (trace_reason, h_id))

                    if d.get('relation'):
                        cur.execute("""
                            INSERT INTO memory.graph_relations 
                            (source_node_id, target_node_id, relation_type, agent_version)
                            VALUES (%s, %s, %s, %s)
                        """, (new_id, n["id"], d['relation'], agent_version))
                     
        conn.commit()
        
        # === Sync новых узлов в Qdrant после commit (best-effort) ===
        if act in ("separate", "supersede", "link", "merge"):
            try:
                from memory_service.graph_node_sync import sync_node_to_qdrant
                if act == "merge":
                    # merge обновляет существующий узел — синхронизируем его
                    sync_node_to_qdrant(str(n["id"]), db_config)
                    logger.debug("Post-commit sync: merged node %s", str(n["id"])[:8])
                else:
                    # separate/supersede/link создают новый узел — ищем его
                    with psycopg2.connect(**db_config) as sync_conn:
                        with sync_conn.cursor() as sync_cur:
                            sync_cur.execute("""
                                SELECT gn.id FROM memory.graph_nodes gn
                                WHERE %s::uuid = ANY(gn.source_hypothesis_ids)
                                AND gn.is_active = TRUE
                                ORDER BY gn.created_at DESC
                                LIMIT 1
                            """, (h_id,))
                            row = sync_cur.fetchone()
                            if row:
                                new_node_id = str(row[0])
                                sync_node_to_qdrant(new_node_id, db_config)
                                logger.debug("Post-commit sync: node %s (from %s)", new_node_id[:8], act)
            except Exception as sync_e:
                logger.warning("Post-commit sync failed for %s (act=%s): %s", h_id[:8], act, sync_e)
        
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


def _create_separate_node(h_id: str, h: dict, n: dict, db_config: dict, ref_node_id: Optional[str] = None) -> None:
    """Создаёт отдельный узел без LLM при несовпадении форм."""
    from memory_service.graph_node_sync import sync_node_to_qdrant
    
    domain_id: Optional[str] = None
    topic_id: Optional[str] = None
    
    # Берём domain/topic из candidate-узла
    if ref_node_id:
        try:
            with psycopg2.connect(**db_config) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT domain_id, topic_id FROM memory.graph_nodes WHERE id=%s",
                        (ref_node_id,)
                    )
                    row = cur.fetchone()
                    if row:
                        domain_id = str(row[0]) if row[0] else None
                        topic_id = str(row[1]) if row[1] else None
        except Exception as e:
            logger.warning("Failed to fetch domain/topic from ref node %s: %s", ref_node_id[:8], e)
    
    # Fallback: берём из h (если есть domain_id как UUID)
    if not domain_id and h.get("domain_id"):
        domain_id = str(h["domain_id"])
    if not topic_id and h.get("topic_id"):
        topic_id = str(h["topic_id"])
    
    form_code = h.get("form_code")
    
    # context_date из created_at гипотезы
    sep_ctx_date = None
    if h.get("created_at"):
        try:
            ca = h["created_at"]
            if hasattr(ca, 'date'):
                sep_ctx_date = ca.date()
            elif isinstance(ca, str) and len(ca) >= 10:
                from datetime import date as _date
                parts = ca[:10].split('-')
                sep_ctx_date = _date(int(parts[0]), int(parts[1]), int(parts[2]))
        except Exception:
            pass
    
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO memory.graph_nodes 
                    (domain_id, topic_id, form_code, description, context_date,
                     source_hypothesis_ids, confidence, agent_version, needs_entity_binding)
                    VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s::uuid[], %s, %s, %s)
                    RETURNING id
                """, (domain_id, topic_id, form_code,
                      h["hypothesis_text"], sep_ctx_date,
                      [h_id], h["confidence"], agent_version,
                      (form_code == 'fact')))
                new_id = str(cur.fetchone()[0])
                cur.execute(
                    "UPDATE memory.hypotheses SET graph_merge_status='integrated', "
                    "graph_review_reason='form_mismatch_separate', candidate_node_id=%s WHERE id=%s",
                    (new_id, h_id))
                conn.commit()
                
                # === Sync в Qdrant после commit ===
                try:
                    sync_node_to_qdrant(new_id, db_config)
                except Exception as sync_e:
                    logger.warning("Post-commit sync failed for %s: %s", new_id[:8], sync_e)
    except Exception as e:
        logger.error("Separate node creation failed for %s: %s", h_id[:8], e)
        _mark_review(h_id, "separate_fail", str(e), db_config)