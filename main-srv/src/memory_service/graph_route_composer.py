"""
main-srv/src/memory_service/graph_route_composer.py

Композер задачи graph_route_and_create.
Детерминированный роутинг подтвержденных (confirmed) гипотез в узлы графа БЕЗ использования LLM.

Последовательность шагов оркестратора:
1. graph_route_load: Загрузка батча гипотез со статусом 'none' и атомарная блокировка (route_locked).
2. graph_route_vectorize: Векторизация текстов гипотез через emb-srv с сохранением метрик.
3. graph_route_create: Детерминированное создание узлов и маршрутизация.

Архитектура и правила маршрутизации:
- Группировка гипотез по (topic_id, domain_id, form_code).
- Поиск кандидатов в Qdrant (cosine similarity >= QDRANT_ROUTE_THRESHOLD).
- Жесткий post-filter кандидатов через PostgreSQL (проверка совпадения domain, topic, form).
- Перекрестная проверка внутри батча: алгоритм Union-Find кластеризует похожие гипотезы.
- Первая гипотеза в кластере (по created_at) становится представительной (insert), 
  остальные помечаются как pending_llm для слияния с ней.
- Гипотезы без совпадений создаются как новые уникальные узлы.
- Post-commit синхронизация новых узлов в Qdrant (best-effort).
- Триггер следующих задач: graph_merge_resolve (для pending) и graph_relation_linker (для новых).
"""

version = "1.2.0"
description = "Graph route & create: deterministic hypothesis routing and unique node insertion"

import logging
import psycopg2
import math
from collections import defaultdict

from typing import Dict, Any, Tuple, Optional, List
from psycopg2.extras import RealDictCursor

from db_manager.db_manager import load_postgres_config
from db_manager.qdrant_manager import search_similar_graph_nodes
from services.emb_service import call_emb_server, EMB_SRV_HOST, EMB_SRV_PORT, EMBEDDING_DIMENSION
from services.service_metrics import (
    create_orchestrator_step, complete_step_success, complete_step_error,
    complete_task_success, complete_task_error, save_emb_metrics
)
from services.tokens_counter import count_tokens_qwen
from version import __version__ as agent_version

logger = logging.getLogger(__name__)

# =============================================================================
# КОНСТАНТЫ
# =============================================================================
QDRANT_ROUTE_THRESHOLD = 0.65 # Косинусная близость для поиска
BATCH_LIMIT = 20
ROUTE_TASK_PRIORITY = 0.4


# =============================================================================
# ХЕЛПЕРЫ
# =============================================================================
def _cosine_similarity(v1: List[float], v2: List[float]) -> float:
    """Cosine similarity между двумя векторами. Возвращает значение в [-1, 1]."""
    if not v1 or not v2 or len(v1) != len(v2):
        return 0.0
    dot = sum(a * b for a, b in zip(v1, v2))
    norm1 = math.sqrt(sum(a * a for a in v1))
    norm2 = math.sqrt(sum(b * b for b in v2))
    if norm1 == 0 or norm2 == 0:
        return 0.0
    return dot / (norm1 * norm2)


def _postfilter_hits(
    raw_hits: list, 
    domain_id: str, 
    topic_id: str, 
    form_code: Optional[str],
    db_config: dict
) -> list:
    """Post-filter Qdrant-кандидатов через PostgreSQL по domain+topic+form."""
    if not raw_hits:
        return []
    postgres_ids = [h["postgres_id"] for h in raw_hits if h.get("postgres_id")]
    if not postgres_ids:
        return []
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                query = """
                    SELECT id, form_code FROM memory.graph_nodes 
                    WHERE id = ANY(%s::uuid[]) 
                    AND domain_id = %s::uuid 
                    AND topic_id = %s::uuid
                    AND is_active = TRUE
                """
                params: list = [postgres_ids, domain_id, topic_id]
                if form_code:
                    query += " AND (form_code = %s OR form_code IS NULL)"
                    params.append(form_code)
                cur.execute(query, params)
                valid_ids = {str(row["id"]) for row in cur.fetchall()}
        filtered = [h for h in raw_hits if h.get("postgres_id") in valid_ids]
        logger.debug(
            "Post-filter: %d/%d candidates (domain=%s, topic=%s, form=%s)",
            len(filtered), len(raw_hits), domain_id[:8], topic_id[:8], form_code or "any"
        )
        return filtered
    except Exception as e:
        logger.warning("Post-filter failed, returning raw hits: %s", e)
        return raw_hits
    

def _mark_hypothesis_failed(hyp_id: str, reason: str, db_config: dict) -> None:
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE memory.hypotheses
                    SET graph_merge_status = 'needs_review'::memory.graph_merge_status,
                        graph_review_reason = %s,
                        updated_at = NOW()
                    WHERE id = %s::uuid
                """, (reason, hyp_id))
                conn.commit()
    except Exception as e:
        logger.error("Status update failed for %s: %s", hyp_id[:8], e)


def _unlock_route_candidates(db_config: dict, hypothesis_ids: list) -> None:
    """Откатывает route_locked → none при ошибке, чтобы следующий запуск подхватил."""
    if not hypothesis_ids:
        return
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE memory.hypotheses
                    SET graph_merge_status = 'none'::memory.graph_merge_status,
                        graph_review_reason = NULL,
                        updated_at = NOW()
                    WHERE id = ANY(%s::uuid[])
                      AND graph_merge_status = 'route_locked'::memory.graph_merge_status
                """, (hypothesis_ids,))
                conn.commit()
                logger.info("Unlocked %d hypotheses back to 'none'", cur.rowcount)
    except Exception as e:
        logger.error("Failed to unlock hypotheses: %s", e)

# =============================================================================
# ОСНОВНОЙ ОБРАБОТЧИК
# =============================================================================
def compose_graph_route_and_create(task_id: str, input_data: Dict[str, Any]) -> None:
    """Выполняет детерминированный роутинг и создание уникальных узлов."""
    db_config = load_postgres_config()

    # === ПОДДЕРЖКА dialogue_id=None (все confirmed гипотезы) ===
    dialogue_id = input_data.get("dialogue_id")

    step_load = create_orchestrator_step(task_id, 1, "graph_route_load", {"dialogue_id": dialogue_id or "all"})
    hypotheses = _load_route_candidates(step_load, dialogue_id, db_config)
    if not hypotheses:
        complete_step_success(step_load, {"loaded": 0})
        complete_task_success(task_id, output_data={"processed": 0, "reason": "no_candidates"})
        return
    complete_step_success(step_load, {"loaded": len(hypotheses)})

    # Запоминаем ID гипотез для отката при ошибке
    hypothesis_ids = [str(h['id']) for h in hypotheses]

    try:
        step_vec = create_orchestrator_step(task_id, 2, "graph_route_vectorize", {"count": len(hypotheses)})
        vectors, emb_metrics = _vectorize_batch(step_vec, hypotheses, db_config)
        complete_step_success(step_vec, {"vectorized": len(vectors)})

        step_create = create_orchestrator_step(task_id, 3, "graph_route_create", {"threshold": QDRANT_ROUTE_THRESHOLD})
        created, pending = _route_and_insert(step_create, hypotheses, vectors, emb_metrics, db_config)
        complete_step_success(step_create, {"created": created, "pending_llm": pending})
        complete_task_success(task_id, output_data={"created": created, "pending_llm": pending})
    except Exception as e:
        # === ОТКАТ: возвращаем route_locked → none ===
        _unlock_route_candidates(db_config, hypothesis_ids)
        complete_task_error(task_id, "graph_route_composer", str(e))
        return

    # === ТРИГГЕРЫ ПАЙПЛАЙНА — ОТДЕЛЬНЫЙ TRY/EXCEPT ===
    # Ошибка планирования следующей задачи НЕ должна ломать завершённую текущую
    try:
        if pending > 0:
            from orchestrator.orchestrator_entry import schedule_graph_merge_resolve
            schedule_graph_merge_resolve(priority=0.3, parent_task_id=task_id)
            logger.info("Pipeline → graph_merge_resolve (pending_llm=%d)", pending)
        if created > 0:
            from orchestrator.orchestrator_entry import schedule_graph_relation_linker
            schedule_graph_relation_linker(priority=0.2, parent_task_id=task_id)
            logger.info("Pipeline → graph_relation_linker (created=%d new nodes)", created)
    except Exception as e:
        logger.error("Pipeline trigger failed (task %s): %s", task_id[:8], e, exc_info=True)
        # НЕ вызываем complete_task_error — задача уже completed!


# =============================================================================
# ХЕЛПЕРЫ: ЗАГРУЗКА И ВЕКТОРИЗАЦИЯ
# =============================================================================
def _load_route_candidates(step_id: str, dialogue_id: Optional[str], db_config: dict) -> list:
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if dialogue_id:
                cur.execute("""
                    SELECT h.id, h.hypothesis_text, h.topic_id, h.confidence, h.knowledge_source, h.domain_code,
                           h.source_message_ids, h.status, h.created_at, h.form_code,
                           d.id AS domain_id
                    FROM memory.hypotheses h
                    INNER JOIN memory.knowledge_domains d ON d.code = h.domain_code
                    WHERE h.dialogue_id = %s::uuid
                      AND h.status = 'confirmed'::memory.hypothesis_status
                      AND h.graph_merge_status = 'none'
                      AND h.topic_id IS NOT NULL
                    ORDER BY h.created_at ASC LIMIT %s FOR UPDATE OF h SKIP LOCKED
                """, (dialogue_id, BATCH_LIMIT))
            else:
                cur.execute("""
                    SELECT h.id, h.hypothesis_text, h.topic_id, h.confidence, h.knowledge_source, h.domain_code,
                           h.source_message_ids, h.status, h.created_at, h.form_code,
                           d.id AS domain_id
                    FROM memory.hypotheses h
                    INNER JOIN memory.knowledge_domains d ON d.code = h.domain_code
                    WHERE h.status = 'confirmed'::memory.hypothesis_status
                      AND h.graph_merge_status = 'none'
                      AND h.topic_id IS NOT NULL
                    ORDER BY h.created_at ASC LIMIT %s FOR UPDATE OF h SKIP LOCKED
                """, (BATCH_LIMIT,))
            
            rows = cur.fetchall()
            
            # === АТОМАРНАЯ БЛОКИРОВКА: route_locked ===
            # graph_merge_resolve ищет только pending_llm — не увидит эти гипотезы.
            # Второй запуск graph_route_and_create тоже не увидит (ищет только 'none').
            if rows:
                ids = [str(r['id']) for r in rows]
                cur.execute("""
                    UPDATE memory.hypotheses
                    SET graph_merge_status = 'route_locked'::memory.graph_merge_status,
                        graph_review_reason = 'route_processing'
                    WHERE id = ANY(%s::uuid[])
                """, (ids,))
            
            return rows


def _vectorize_batch(step_id: str, hypotheses: list, db_config: dict) -> Tuple[list, list]:
    vectors, metrics = [], []
    for h in hypotheses:
        vec, resp = call_emb_server(h["hypothesis_text"])
        if not vec:
            logger.warning("Emb failed for hyp %s: %s", str(h["id"])[:8], resp["params"].get("error"))
            vectors.append(None)
            metrics.append(None)
            continue
            
        vectors.append(vec)
        params = resp.get("params", {})
        sent_at = params.get("sent_at")
        received_at = params.get("received_at")
        
        m_id = save_emb_metrics(
            step_id, 
            f"{EMB_SRV_HOST}:{EMB_SRV_PORT}", 
            resp["model"].get("name", "unknown"),
            {"embedding_dim": EMBEDDING_DIMENSION}, 
            EMBEDDING_DIMENSION,
            count_tokens_qwen(h["hypothesis_text"]), 
            received_at,
            sent_at,
            params.get("duration_sec", 0), 
            False,
            agent_version
        )
        
        metrics.append(m_id)  # <--- ✅ ДОБАВИТЬ ЭТУ СТРОКУ!

        # Обновляем emb_metric_id в шаге (последняя успешная метрика)
        last_m_id = None
        for m in reversed(metrics):
            if m is not None:
                last_m_id = m
                break
                
        if last_m_id:
            try:
                with psycopg2.connect(**db_config) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE orchestrator.orchestrator_steps SET emb_metric_id=%s WHERE id=%s",
                            (last_m_id, step_id))
                        conn.commit()
            except Exception as e:
                logger.debug("Failed to update emb_metric_id on step %s: %s", step_id[:8], e)
                
    return vectors, metrics

# =============================================================================
# ОСНОВНАЯ ЛОГИКА: ПЕРЕКРЁСТНАЯ ПРОВЕРКА ВНУТРИ БАТЧА
# =============================================================================
def _route_and_insert(step_id: str, hypotheses: list, vectors: list, emb_metrics: list, db_config: dict) -> Tuple[int, int]:
    """
    Перекрёстная проверка гипотез:
    1. Группируем по (topic_id, domain_id, form_code).
    2. Для каждой гипотезы ищем max_score среди существующих узлов в Qdrant.
    3. Считаем матрицу попарных cosine similarity внутри батча.
    4. Union-Find кластеризация похожих гипотез (score >= threshold).
    5. Первый по created_at в кластере → insert, остальные → pending_llm к нему.
    6. Все гипотезы без совпадений → insert (отдельные узлы).
    """
    created, pending = 0, 0

    # === ЗАЩИТА: нормализация m_id (может быть tuple из save_emb_metrics) ===
    safe_emb_metrics = []
    for m in emb_metrics:
        if isinstance(m, (tuple, list)) and len(m) > 0:
            safe_emb_metrics.append(str(m[0]) if m[0] else None)
        else:
            safe_emb_metrics.append(m)

    # === 1. ВАЛИДАЦИЯ: собираем все валидные items ===
    valid_items = []
    for i, (h, vec, m_id) in enumerate(zip(hypotheses, vectors, safe_emb_metrics)):
        if vec is None:
            continue
        domain_id = str(h.get("domain_id") or "")
        topic_id = str(h.get("topic_id") or "")
        if not domain_id or not topic_id:
            logger.warning("⏭ Пропуск гипотезы %s: нет domain_id/topic_id", h["id"][:8])
            _mark_hypothesis_failed(h["id"], "missing_domain_or_topic_uuid", db_config)
            continue
        valid_items.append({
            'orig_idx': i,
            'h': h,
            'vec': vec,
            'm_id': m_id,
            'domain_id': domain_id,
            'topic_id': topic_id,
            'form_code': h.get("form_code"),
            'decision': None,
            'candidate_node_id': None,
            'batch_rep_orig_idx': None,
        })

    if not valid_items:
        return 0, 0

    # === 2. ГРУППИРОВКА ПО (topic_id, domain_id, form_code) ===
    by_topic = defaultdict(list)
    for item in valid_items:
        by_topic[(item['topic_id'], item['domain_id'], item['form_code'])].append(item)

    # === 3. ПЕРЕКРЁСТНАЯ ПРОВЕРКА ДЛЯ КАЖДОЙ ГРУППЫ ===
    # ИСПРАВЛЕНО: распаковка кортежа из 3 элементов (topic_id, domain_id, form_code)
    for (topic_id, domain_id, _form_key), items in by_topic.items():
        # Сортируем по created_at ASC — детерминизм: первый = representative
        items.sort(key=lambda x: x['h']['created_at'])

        # 3a. Для каждой гипотезы: max_score среди СУЩЕСТВУЮЩИХ узлов в Qdrant
        for item in items:
            raw_hits = search_similar_graph_nodes(
                vector=item['vec'],
                actor_id=None,
                limit=3,
                score_threshold=QDRANT_ROUTE_THRESHOLD,
                candidate_limit=30
            )
            # Post-filter через PostgreSQL: оставляем только узлы с тем же domain+topic+form
            hits = _postfilter_hits(
                raw_hits, domain_id, topic_id,
                item['h'].get('form_code'), db_config
            )
            if hits:
                item['decision'] = 'pending_existing'
                item['candidate_node_id'] = hits[0]["postgres_id"]
                logger.debug(
                    "Hyp %s → pending_existing (Qdrant hit: node=%s, score=%.3f)",
                    item['h']['id'][:8], hits[0]["postgres_id"][:8], hits[0]["score"]
                )

        # 3b. Cross-batch similarity среди ещё не определённых (не pending_existing)
        undecided_indices = [i for i, it in enumerate(items) if it['decision'] is None]

        if len(undecided_indices) > 1:
            # Union-Find для кластеризации похожих гипотез
            parent = list(range(len(items)))

            def find(x):
                while parent[x] != x:
                    parent[x] = parent[parent[x]]
                    x = parent[x]
                return x

            def union(x, y):
                px, py = find(x), find(y)
                if px != py:
                    parent[px] = py

            for a_pos, i in enumerate(undecided_indices):
                for b_pos, j in enumerate(undecided_indices):
                    if a_pos >= b_pos:
                        continue
                    score = _cosine_similarity(items[i]['vec'], items[j]['vec'])
                    if score >= QDRANT_ROUTE_THRESHOLD:
                        union(i, j)
                        logger.debug(
                            "Batch similarity: hyp %s ↔ hyp %s = %.3f (>= %.2f) → same cluster",
                            items[i]['h']['id'][:8], items[j]['h']['id'][:8], score, QDRANT_ROUTE_THRESHOLD
                        )

            clusters = defaultdict(list)
            for i in undecided_indices:
                clusters[find(i)].append(i)

            for cluster_indices in clusters.values():
                rep_idx = cluster_indices[0]
                items[rep_idx]['decision'] = 'insert'
                for k in cluster_indices[1:]:
                    items[k]['decision'] = 'pending_batch'
                    items[k]['batch_rep_orig_idx'] = items[rep_idx]['orig_idx']
                    logger.debug(
                        "Hyp %s → pending_batch (cluster rep: hyp %s)",
                        items[k]['h']['id'][:8], items[rep_idx]['h']['id'][:8]
                    )

        # 3c. Все оставшиеся без решения → insert
        for item in items:
            if item['decision'] is None:
                item['decision'] = 'insert'

    # === 4. ПРИМЕНЕНИЕ РЕШЕНИЙ ===
    to_insert = []
    for item in valid_items:
        if item['decision'] == 'insert':
            to_insert.append((item['h'], item['vec'], item['m_id'], item['orig_idx']))

    vec_queue = []
    batch_created_map = {}

    if to_insert:
        try:
            with psycopg2.connect(**db_config) as conn:
                conn.autocommit = False
                with conn.cursor() as cur:
                    for h, vec, m_id, orig_idx in to_insert:
                        # === БЕРЁМ context_date ИЗ created_at ГИПОТЕЗЫ ===
                        # Это момент, когда факт был зафиксирован в системе
                        ctx_date = None
                        if h.get("created_at"):
                            try:
                                ca = h["created_at"]
                                # psycopg2 RealDictCursor возвращает datetime или str
                                if hasattr(ca, 'date'):
                                    # datetime → date
                                    ctx_date = ca.date()
                                elif isinstance(ca, str) and len(ca) >= 10:
                                    # "2026-07-13 15:45:33.787 +0300" → date
                                    from datetime import date as _date
                                    parts = ca[:10].split('-')
                                    ctx_date = _date(int(parts[0]), int(parts[1]), int(parts[2]))
                            except Exception:
                                pass

                        # === needs_entity_binding: TRUE для fact-узлов ===
                        # Entity-clustering подхватит их и привяжет к entity-агрегатору
                        needs_binding = (h.get("form_code") == 'fact')
                        cur.execute("""
                            INSERT INTO memory.graph_nodes 
                            (domain_id, topic_id, form_code, description, context_date, 
                            source_hypothesis_ids, confidence, agent_version, needs_entity_binding)
                            VALUES (%s, %s, %s, %s, %s, %s::uuid[], %s, %s, %s) RETURNING id
                        """, (str(h["domain_id"]), str(h["topic_id"]), h.get("form_code"),
                            h["hypothesis_text"], ctx_date, [str(h["id"])], h["confidence"], 
                            agent_version, needs_binding))
                        nid = str(cur.fetchone()[0])
                        cur.execute(
                            "UPDATE memory.hypotheses SET graph_merge_status='integrated', graph_review_reason=NULL WHERE id=%s",
                            (str(h["id"]),))
                        vec_queue.append(nid)
                        batch_created_map[orig_idx] = nid
                        created += 1
                conn.commit()

                for nid in vec_queue:
                    try:
                        from memory_service.graph_node_sync import sync_node_to_qdrant
                        sync_node_to_qdrant(nid, db_config)
                    except Exception as e:
                        logger.warning("Post-commit sync failed for node %s: %s", nid[:8], e)

        except Exception as e:
            logger.error("❌ Ошибка транзакции вставки узлов: %s", e, exc_info=True)
            for h, _, _, _ in to_insert:
                _mark_hypothesis_failed(h["id"], f"insert_tx_error: {str(e)[:50]}", db_config)
            complete_step_error(step_id, "graph_route_composer", f"TX failed: {e}")
            raise

    to_pending = []
    for item in valid_items:
        h = item['h']
        if item['decision'] == 'pending_existing':
            to_pending.append((h, item['candidate_node_id']))
        elif item['decision'] == 'pending_batch':
            rep_node_id = batch_created_map.get(item['batch_rep_orig_idx'])
            if rep_node_id:
                to_pending.append((h, rep_node_id))
                logger.debug(
                    "Hyp %s → pending_llm (batch rep node: %s)",
                    h['id'][:8], rep_node_id[:8]
                )
            else:
                logger.warning("Batch representative node not found for hyp %s — marking failed", h['id'][:8])
                _mark_hypothesis_failed(h["id"], "batch_rep_missing", db_config)

    for h, cand_id in to_pending:
        try:
            with psycopg2.connect(**db_config) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE memory.hypotheses SET graph_merge_status='pending_llm', graph_review_reason='pending_merge', candidate_node_id=%s WHERE id=%s",
                        (cand_id, str(h["id"])))
                    conn.commit()
                    pending += 1
        except Exception as e:
            logger.warning("Failed to mark pending_llm for %s: %s", str(h["id"])[:8], e)

    logger.info(
        "Route complete: created=%d, pending_llm=%d (threshold=%.2f)",
        created, pending, QDRANT_ROUTE_THRESHOLD
    )
    return created, pending