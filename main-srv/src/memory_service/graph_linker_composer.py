"""
main-srv/src/memory_service/graph_linker_composer.py

Композер оркестратора для задачи graph_relation_linker.
Фоновое построение логических связей (рёбер) между узлами внутри одной темы с использованием LLM (CoT).

Последовательность шагов:
1. graph_route_load: Выборка темы с >= 2 активными узлами.
2. graph_linker_llm: Вызов LLM (prompt: graph_relation_linker) для поиска связей.
3. graph_linker_tx: Транзакция вставки рёбер с жесткой валидацией.

Правила и защиты:
- Связи строятся строго внутри одной темы (topic_id).
- Запрет на создание self-loops (связь узла с самим собой).
- Запрет на создание refines/supersedes/depends_on между узлами разных форм (form_code).
- Фильтрация по MIN_EDGE_CONFIDENCE (0.6) для отсечения галлюцинаций LLM.
- Дедупликация рёбер внутри батча и проверка на существование в БД.
- После создания рёбер помечает узлы для пересуммаризации (needs_summary_update) 
  и кластеризации сущностей (needs_entity_binding).
- Триггерит graph_summarize с учетом интервалов и отсутствия активных верификаций.
"""

version = "1.2.0"
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
from services.datetime_context import build_time_block

logger = logging.getLogger(__name__)

PROMPT_NAME = "graph_relation_linker"
MAX_CTX = 10000
BATCH_NODES = 15
MIN_EDGE_CONFIDENCE = 0.6  # Связи ниже этого порога уверенности модели не создаются


def _trigger_summarize(parent_task_id: str, db_config: dict) -> None:
    """Проверяет наличие узлов без summary и запускает summarize с учётом интервала."""
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                # === ЗАЩИТА 1: Проверка интервала с последнего запуска ===
                cur.execute("""
                    SELECT t.completed_at FROM orchestrator.orchestrator_tasks t
                    JOIN orchestrator.task_types tt ON t.task_type_id = tt.id
                    WHERE tt.type_name = 'graph_summarize'
                      AND t.status = 'completed'::task_status
                    ORDER BY t.completed_at DESC
                    LIMIT 1
                """)
                last_run = cur.fetchone()
                
                if last_run and last_run[0]:
                    cur.execute(
                        "SELECT value_float FROM state.settings WHERE param_name = 'graph_summarize_interval_minutes'"
                    )
                    interval_row = cur.fetchone()
                    interval_min = interval_row[0] if interval_row and interval_row[0] else 15.0
                    
                    if interval_min > 0:
                        from datetime import datetime as _dt, timezone as _tz, timedelta as _td
                        last_completed = last_run[0]
                        if last_completed.tzinfo is None:
                            last_completed = last_completed.replace(tzinfo=_tz.utc)
                        elapsed = _dt.now(_tz.utc) - last_completed
                        if elapsed < _td(minutes=interval_min):
                            logger.debug(
                                "Summarize skipped: interval not elapsed (%.1f < %.1f min)",
                                elapsed.total_seconds() / 60, interval_min
                            )
                            return
                
                # === ЗАЩИТА 2: Активная верификация гипотез? ===
                cur.execute("""
                    SELECT 1 FROM memory.verification_sessions
                    WHERE status = 'active'::memory.verification_session_status
                    LIMIT 1
                """)
                if cur.fetchone():
                    logger.debug("Summarize skipped: verification session active")
                    return
                
                # === ЗАЩИТА 3: Есть ли вообще узлы, требующие summary? ===
                cur.execute("""
                    SELECT 1 FROM memory.graph_nodes
                    WHERE is_active = TRUE
                    AND (summary IS NULL OR needs_summary_update = TRUE)
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
    # === Загружаем описания форм из справочника ===
    forms_map = {}
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT code, description FROM memory.forms WHERE is_active = TRUE")
                forms_map = {r["code"]: r["description"] for r in cur.fetchall()}
    except Exception as e:
        logger.warning("Failed to load forms descriptions: %s", e)

    ctx = json.dumps([
        {
            "id": str(n["id"]),
            "form": n.get("form_code") or "unknown",
            "form_meaning": forms_map.get(n.get("form_code"), "Неизвестная форма"),
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
                    SELECT id, description, context_date, is_active, topic_id, domain_id, 
                        source_hypothesis_ids, form_code
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
        
        # Добавляем время и дату
        system_with_time = p["text"] + build_time_block("general")
        msgs = [{"role": "system", "content": system_with_time}, {"role": "user", "content": ctx}]
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
        if c.startswith("`"):
            s, e = c.find('['), c.rfind(']')
            if s != -1 and e != -1:
                c = c[s:e + 1]
        edges = json.loads(c)
        if not isinstance(edges, list):
            raise ValueError("Not list")

        # Валидация UUID из ответа модели
        valid_ids = {str(n["id"]) for n in nodes}

        with psycopg2.connect(**db_config) as conn:
            conn.autocommit = False
            with conn.cursor() as cur:
                created = 0
                # === ДЕДУПЛИКАЦИЯ ВНУТРИ БАТЧА: LLM может вернуть одну пару дважды ===
                seen_in_batch: set = set()

                for e in edges:
                    s_id = str(e.get("source_id") or "")
                    t_id = str(e.get("target_id") or "")
                    rel = e.get("relation") or ""

                    if not s_id or not t_id or not rel:
                        continue
                    if s_id not in valid_ids or t_id not in valid_ids:
                        continue

                    # Защита: не создавать связь узла с самим собой
                    if s_id == t_id:
                        logger.debug("Self-edge skipped: %s → %s", s_id[:8], t_id[:8])
                        continue

                    # Дедупликация: уже видели в этом батче
                    pair_key = (s_id, t_id, rel)
                    if pair_key in seen_in_batch:
                        logger.debug("Duplicate edge skipped (batch): %s → %s (%s)", s_id[:8], t_id[:8], rel)
                        continue
                    seen_in_batch.add(pair_key)

                    # === ЗАЩИТА: не создаём refines/supersedes между разными формами ===
                    s_form = next((n.get("form_code") for n in nodes if str(n["id"]) == s_id), None)
                    t_form = next((n.get("form_code") for n in nodes if str(n["id"]) == t_id), None)
                    if rel in ("refines", "supersedes", "depends_on") and s_form and t_form and s_form != t_form:
                        logger.debug(
                            "Skipped %s between different forms (%s→%s): %s↔%s",
                            rel, s_form, t_form, s_id[:8], t_id[:8]
                        )
                        continue

                    # Валидация confidence из ответа модели
                    conf = e.get("confidence", 0.7)
                    if not isinstance(conf, (int, float)) or not (0.0 <= conf <= 1.0):
                        conf = 0.8
                    conf = float(conf)
                    
                    # Фильтр: связи с низкой уверенностью — мусор от LLM
                    if conf < MIN_EDGE_CONFIDENCE:
                        logger.debug(
                            "Low confidence edge skipped: %s→%s (%s) conf=%.2f < %.2f",
                            s_id[:8], t_id[:8], rel, conf, MIN_EDGE_CONFIDENCE
                        )
                        continue

                    # Собираем source_hypothesis_ids из обоих узлов связи
                    src_hyp_ids = []
                    for n in nodes:
                        nid = str(n["id"])
                        if nid == s_id or nid == t_id:
                            hyp_ids = n.get("source_hypothesis_ids")
                            if hyp_ids:
                                if isinstance(hyp_ids, str):
                                    clean = hyp_ids.strip("{}")
                                    if clean:
                                        src_hyp_ids.extend([uid.strip() for uid in clean.split(",")])
                                elif isinstance(hyp_ids, (list, tuple)):
                                    src_hyp_ids.extend(hyp_ids)

                    # Дедупликация + жёсткая фильтрация формата UUID
                    src_hyp_ids = list({
                        str(uid) for uid in src_hyp_ids
                        if uid and len(uid) == 36 and uid.count('-') == 4
                    })

                    # === PER-EDGE PRE-CHECK: ребро уже существует в БД? ===
                    cur.execute("""
                        SELECT 1 FROM memory.graph_edges 
                        WHERE source_node_id = %s::uuid 
                          AND target_node_id = %s::uuid 
                          AND relation_type = %s 
                          AND is_active = TRUE
                        LIMIT 1
                    """, (s_id, t_id, rel))
                    if cur.fetchone():
                        logger.debug("Edge already exists (DB): %s→%s (%s)", s_id[:8], t_id[:8], rel)
                        continue
                    
                    # === INSERT БЕЗ ON CONFLICT DO NOTHING ===
                    cur.execute("""
                        INSERT INTO memory.graph_edges 
                        (actor_id, source_node_id, target_node_id, relation_type, 
                         source_hypothesis_ids, confidence, needs_review, agent_version)
                        VALUES (NULL::uuid, %s::uuid, %s::uuid, %s, %s::uuid[], %s, %s::boolean, %s)
                    """, (s_id, t_id, rel, src_hyp_ids if src_hyp_ids else None, 
                          conf, bool(e.get("needs_review", False)), agent_version))
                    created += 1

            conn.commit()

            # Помечаем узлы для пересуммаризации и entity_clustering
            if created > 0:
                involved_ids = set()
                for e in edges:
                    if e.get("source_id") and str(e["source_id"]) in valid_ids:
                        involved_ids.add(str(e["source_id"]))
                    if e.get("target_id") and str(e["target_id"]) in valid_ids:
                        involved_ids.add(str(e["target_id"]))
                
                involved_list = list(involved_ids)
                if involved_list:
                    with conn.cursor() as c:
                        c.execute("""
                            UPDATE memory.graph_nodes 
                            SET needs_summary_update = TRUE, updated_at = NOW()
                            WHERE id = ANY(%s::uuid[]) AND summary IS NOT NULL
                        """, (involved_list,))
                        
                        c.execute("""
                            UPDATE memory.graph_nodes 
                            SET needs_entity_binding = TRUE, updated_at = NOW()
                            WHERE id = ANY(%s::uuid[])
                              AND form_code = 'fact'
                              AND is_active = TRUE
                        """, (involved_list,))
                    
                    conn.commit()

        complete_step_success(step_id, {"edges_created": created})
    except Exception as e:
        complete_step_error(step_id, "graph_linker_composer", str(e))