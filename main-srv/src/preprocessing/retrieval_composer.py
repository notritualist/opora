"""
/main-srv/src/preprocessing/retrieval_composer.py

Шаг knowledge_retrieval: гибридный поиск и сборка контекста из графа знаний.

Логика работы и архитектура поиска:
1. Подготовка запросов: использует исходный вопрос или массив подвопросов (multi-vector).
2. Векторизация: вызов embedding-сервиса для каждого запроса с сохранением метрик.
3. Qdrant + Postgres: векторный поиск с пост-фильтрацией по UUID доменов (защита от пересечений).
4. Графовое расширение (Graph Expansion): 
   - Поиск соседей через memory.graph_edges (до MAX_GRAPH_DEPTH уровней).
   - Сущностно-ориентированное расширение: если найдена 'entity', подтягиваются все связанные 'facts' (relation_type='about').
5. Многофакторное ранжирование (Scoring):
   weighted_score = (base_score * confidence) * (1 + pop_bonus) * time_decay
   где pop_bonus учитывает частоту использования узла, а time_decay — давность (до 90 дней).
6. Формирование и обрезка контекста:
   - Форматирование узлов в текст (приоритет summary, если он короче description).
   - Жесткая обрезка (trimming) до MAX_CONTEXT_TOKENS (10000) с заменой на summary при переполнении.
7. Логирование и статистика:
   - Запись полного лога в memory.retrieval_logs.
   - Инкремент счетчика retrieval_count и обновление last_retrieved_at для использованных узлов.

Поддерживает стратегии: 'hybrid', 'hybrid_multi', 'fallback', 'fallback_multi'.
"""
version = "1.4.1"
description = "Knowledge retrieval from graph with hybrid strategy"

import logging
import time
import psycopg2
import math
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from psycopg2.extras import RealDictCursor, Json

from db_manager.db_manager import load_postgres_config
from db_manager.qdrant_manager import search_similar_graph_nodes
from services.service_metrics import (
    create_orchestrator_step,
    complete_step_success,
    complete_step_error,
    save_emb_metrics,
)
from services.emb_service import (
    call_emb_server,
    EMB_SRV_HOST,
    EMB_SRV_PORT,
    EMBEDDING_DIMENSION,
)
from services.tokens_counter import count_tokens_qwen
from version import __version__ as agent_version 

logger = logging.getLogger(__name__)

# =============================================================================
# === НАСТРОЙКИ ПОИСКА (КОНСТАНТЫ ДЛЯ ТЮНИНГА) ===============================
# =============================================================================
MAX_SUB_QUERIES: int = 10
MIN_WEIGHTED_SCORE: float = 0.2
MAX_CONTEXT_TOKENS: int = 10000
MAX_GRAPH_DEPTH: int = 3
SIMILARITY_THRESHOLD: float = 0.45
MAX_NODES_PER_TOPIC: int = 10
MAX_TOTAL_NODES: int = 20
MIN_CONFIDENCE: float = 0.5
EXCLUDE_NEEDS_REVIEW: bool = True
MAX_RETRIEVAL_LATENCY_SEC: float = 10.0

# =============================================================================
# === ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =================================================
# =============================================================================

def _bump_node_retrieval_stats(node_ids: List[str], db_config: dict) -> None:
    if not node_ids:
        return
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE memory.graph_nodes
                    SET 
                        retrieval_count = retrieval_count + 1,
                        last_retrieved_at = NOW(),
                        updated_at = NOW()
                    WHERE id = ANY(%s::uuid[])
                """, (node_ids,))
            conn.commit()
    except Exception as e:
        logger.warning("Failed to bump retrieval stats for nodes: %s", e)


def _load_graph_nodes(
    cur,
    node_ids: List[str],
    exclude_needs_review: bool = True,
    min_confidence: float = 0.0
) -> List[Dict[str, Any]]:
    if not node_ids:
        return []

    sql = """
        SELECT id, description, summary, confidence, domain_id, topic_id,
           needs_review, is_active, retrieval_count, last_retrieved_at
        FROM memory.graph_nodes
        WHERE id = ANY(%s::uuid[])
          AND is_active = TRUE
    """
    params: list = [node_ids]

    if exclude_needs_review:
        sql += " AND needs_review = FALSE"
    if min_confidence > 0:
        sql += " AND confidence >= %s"
        params.append(min_confidence)

    cur.execute(sql, params)
    return [dict(row) for row in cur.fetchall()]


def _expand_via_graph_edges(
    cur,
    seed_node_ids: List[str],
    depth: int,
    min_confidence: float,
) -> Tuple[List[str], List[str]]:
    if depth < 1 or not seed_node_ids:
        return [], []

    cur.execute("""
        SELECT DISTINCT
            CASE
                WHEN source_node_id = ANY(%s::uuid[]) THEN target_node_id
                ELSE source_node_id
            END AS neighbor_id,
            id AS edge_id,
            confidence
        FROM memory.graph_edges
        WHERE (source_node_id = ANY(%s::uuid[]) OR target_node_id = ANY(%s::uuid[]))
          AND is_active = TRUE
          AND confidence >= %s
    """, (seed_node_ids, seed_node_ids, seed_node_ids, min_confidence))

    neighbors: List[str] = []
    edge_ids: List[str] = []
    for row in cur.fetchall():
        neighbor = str(row["neighbor_id"])
        if neighbor not in seed_node_ids and neighbor not in neighbors:
            neighbors.append(neighbor)
        edge_id = str(row["edge_id"])
        if edge_id not in edge_ids:
            edge_ids.append(edge_id)

    if depth > 1 and neighbors:
        deeper_nodes, deeper_edges = _expand_via_graph_edges(
            cur, neighbors, depth - 1, min_confidence
        )
        for n in deeper_nodes:
            if n not in neighbors:
                neighbors.append(n)
        for e in deeper_edges:
            if e not in edge_ids:
                edge_ids.append(e)

    return neighbors, edge_ids


def _format_node_for_context(node: Dict[str, Any]) -> str:
    summary = (node.get("summary") or "").strip()
    description = (node.get("description") or "").strip()

    if summary and len(summary) < len(description) * 0.7:
        text = summary
    else:
        text = description

    if len(text) > 800:
        text = text[:800] + "..."

    return f"[Факт] {text}"


def _trim_context_to_limit(
    candidates: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], int, bool]:
    result: List[Dict[str, Any]] = []
    total_tokens = 0
    trimmed = False

    for cand in candidates:
        text = cand["formatted_text"]
        tok = count_tokens_qwen(text)

        if total_tokens + tok > MAX_CONTEXT_TOKENS:
            trimmed = True
            if cand.get("summary") and len(cand["summary"]) < len(cand.get("description", "")):
                short = f"[Факт] {cand['summary']}"
                short_tok = count_tokens_qwen(short)
                if total_tokens + short_tok <= MAX_CONTEXT_TOKENS:
                    cand["formatted_text"] = short
                    result.append(cand)
                    total_tokens += short_tok
            continue

        result.append(cand)
        total_tokens += tok

    return result, total_tokens, trimmed


def _expand_entities_to_facts(cur, entity_ids: List[str], min_confidence: float) -> Tuple[List[str], List[str]]:
    if not entity_ids:
        return [], []
    
    cur.execute("""
        SELECT source_node_id AS fact_id, id AS edge_id
        FROM memory.graph_edges
        WHERE target_node_id = ANY(%s::uuid[])
          AND relation_type = 'about'
          AND is_active = TRUE
          AND confidence >= %s
    """, (entity_ids, min_confidence))
    
    fact_ids = []
    edge_ids = []
    for row in cur.fetchall():
        fact_ids.append(str(row["fact_id"]))
        edge_ids.append(str(row["edge_id"]))
        
    return list(set(fact_ids)), list(set(edge_ids))


# =============================================================================
# === ГЛАВНАЯ ФУНКЦИЯ ШАГА ====================================================
# =============================================================================

def compose_knowledge_retrieval(
    task_id: str,
    message_id: str,
    routing_context: Dict[str, Any],
    sub_queries: Optional[List[str]] = None,
    step_type_name: str = "knowledge_retrieval",
) -> Dict[str, Any]:
    db_config = load_postgres_config()
    start_time = time.time()

    step_id = create_orchestrator_step(
        task_id=task_id,
        step_number=3,
        step_type_name=step_type_name,
        input_data={"message_id": message_id, "routing": routing_context}
    )

    conn = None
    emb_metric_id: Optional[str] = None
    domains: List[str] = []
    topics: List[str] = []

    try:
        # === 1. Загружаем текст вопроса ===
        conn = psycopg2.connect(**db_config)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT row_text FROM dialogs.row_messages WHERE id = %s",
                (message_id,)
            )
            msg_row = cur.fetchone()
            if not msg_row:
                raise RuntimeError(f"Message {message_id} not found")
            question_text = msg_row["row_text"]

        # === 2. Формируем список запросов для векторизации ===
        queries_to_embed: List[str] = []
        if sub_queries and len(sub_queries) > 0:
            queries_to_embed = sub_queries[:MAX_SUB_QUERIES]
            logger.info("Multi-vector mode: %d sub-queries", len(queries_to_embed))
        else:
            queries_to_embed = [question_text]
            logger.debug("Single-vector mode")

        # === 3. Векторизуем все запросы ===
        query_vectors: List[Tuple[str, List[float]]] = []
        emb_metric_ids: List[str] = []

        for i, query_text in enumerate(queries_to_embed):
            q_vector, emb_response = call_emb_server(query_text)
            error_msg = emb_response.get("params", {}).get("error")

            if error_msg or q_vector is None:
                logger.warning(
                    "Embedding failed for query %d/%d: %s",
                    i + 1, len(queries_to_embed), error_msg or "unknown"
                )
                continue

            model_info = emb_response.get("model", {}) or {}
            params_info = emb_response.get("params", {}) or {}

            received_at = None
            sent_at = None
            if params_info.get("received_at"):
                try:
                    received_at = datetime.fromisoformat(str(params_info["received_at"]))
                except (ValueError, TypeError):
                    pass
            if params_info.get("sent_at"):
                try:
                    sent_at = datetime.fromisoformat(str(params_info["sent_at"]))
                except (ValueError, TypeError):
                    pass

            e_id = save_emb_metrics(
                orchestrator_step_id=step_id,
                host=f"{EMB_SRV_HOST}:{EMB_SRV_PORT}",
                model=model_info.get("name", "unknown"),
                param={"embedding_dim": model_info.get("embedding_dim")},
                vector_dimension=model_info.get("embedding_dim", EMBEDDING_DIMENSION),
                prompt_tokens=count_tokens_qwen(query_text),
                received_at=received_at,
                sent_at=sent_at,
                full_time=float(params_info.get("duration_sec", 0.0)),
                error_status=False,
                agent_version=agent_version
            )
            emb_metric_ids.append(e_id)
            query_vectors.append((query_text, q_vector))

        if not query_vectors:
            raise RuntimeError("All embeddings failed — cannot proceed with retrieval")

        emb_metric_id = emb_metric_ids[0] if emb_metric_ids else None

        # === 4. Определяем стратегию ===
        domains = routing_context.get("domains", []) or []
        topics = routing_context.get("topics", []) or []
        is_fallback = routing_context.get("fallback", False) or (not domains and not topics)
        strategy = "fallback" if is_fallback else "hybrid"
        if sub_queries:
            strategy += "_multi"

        # === 5. Получаем UUID доменов и выполняем поиск ===
        domain_uuids: List[str] = []
        all_hits: List[Dict[str, Any]] = []
        seen_nodes: Dict[str, float] = {}

        with psycopg2.connect(**db_config) as conn2:
            # === ИСПРАВЛЕНИЕ ЗДЕСЬ: добавлен cursor_factory=RealDictCursor ===
            with conn2.cursor(cursor_factory=RealDictCursor) as cur2:
                if is_fallback:
                    logger.warning("Routing fallback — using top 3 active domains without topic restriction")
                    cur2.execute("SELECT id FROM memory.knowledge_domains WHERE is_active = TRUE LIMIT 3")
                    domain_uuids = [str(row["id"]) for row in cur2.fetchall()]
                else:
                    if domains:
                        cur2.execute(
                            "SELECT id, code FROM memory.knowledge_domains WHERE code = ANY(%s) AND is_active = TRUE",
                            (domains,)
                        )
                        domain_map = {row["code"]: str(row["id"]) for row in cur2.fetchall()}
                        domain_uuids = [domain_map[c] for c in domains if c in domain_map]

                # 5.2 Поиск в Qdrant и пост-фильтрация
                for q_text, q_vector in query_vectors:
                    for domain_id in domain_uuids:
                        try:
                            hits = search_similar_graph_nodes(
                                vector=q_vector,
                                actor_id=None,
                                limit=MAX_NODES_PER_TOPIC,
                                score_threshold=SIMILARITY_THRESHOLD,
                                candidate_limit=MAX_NODES_PER_TOPIC * 3
                            )
                            
                            if hits:
                                pg_ids = [h["postgres_id"] for h in hits if h.get("postgres_id")]
                                if pg_ids:
                                    cur2.execute(
                                        "SELECT id FROM memory.graph_nodes WHERE id = ANY(%s::uuid[]) "
                                        "AND domain_id = %s::uuid AND is_active = TRUE",
                                        (pg_ids, domain_id)
                                    )
                                    valid_set = {str(r["id"]) for r in cur2.fetchall()}
                                    hits = [h for h in hits if h.get("postgres_id") in valid_set]
                            
                            for hit in hits:
                                pid = hit.get("postgres_id")
                                score = hit.get("score", 0.0)
                                if pid and (pid not in seen_nodes or score > seen_nodes[pid]):
                                    seen_nodes[pid] = score
                                    hit_copy = dict(hit)
                                    hit_copy["domain_id"] = domain_id
                                    hit_copy["query_text"] = q_text[:100]
                                    all_hits = [h for h in all_hits if h.get("postgres_id") != pid]
                                    all_hits.append(hit_copy)
                        except Exception as e:
                            logger.warning("Qdrant search failed for domain=%s: %s", domain_id[:8], e)

        # === 6. Загружаем полные данные узлов ===
        seed_ids = [h["postgres_id"] for h in all_hits if h.get("postgres_id")]
        edge_ids: List[str] = []

        with psycopg2.connect(**db_config) as conn3:
            with conn3.cursor(cursor_factory=RealDictCursor) as cur3:
                nodes = _load_graph_nodes(
                    cur3, seed_ids,
                    exclude_needs_review=EXCLUDE_NEEDS_REVIEW,
                    min_confidence=MIN_CONFIDENCE
                )
                nodes_by_id = {str(n["id"]): n for n in nodes}

                # === 7. Расширение через граф ===
                if MAX_GRAPH_DEPTH > 0 and seed_ids:
                    extra_ids, extra_edge_ids = _expand_via_graph_edges(
                        cur3, seed_ids, MAX_GRAPH_DEPTH, MIN_CONFIDENCE
                    )
                    if extra_ids:
                        extra_nodes = _load_graph_nodes(
                            cur3, extra_ids,
                            exclude_needs_review=EXCLUDE_NEEDS_REVIEW,
                            min_confidence=MIN_CONFIDENCE
                        )
                        for n in extra_nodes:
                            nodes_by_id[str(n["id"])] = n
                    edge_ids.extend(extra_edge_ids)

                # === 8. Сущностно-ориентированное расширение (Entity -> Facts) ===
                found_entity_ids = [nid for nid, node in nodes_by_id.items() if node.get("form_code") == 'entity']

                if found_entity_ids:
                    extra_fact_ids, entity_edge_ids = _expand_entities_to_facts(cur3, found_entity_ids, MIN_CONFIDENCE)
                    
                    if extra_fact_ids:
                        fact_nodes = _load_graph_nodes(cur3, extra_fact_ids, exclude_needs_review=EXCLUDE_NEEDS_REVIEW, min_confidence=MIN_CONFIDENCE)
                        for n in fact_nodes:
                            nodes_by_id[str(n["id"])] = n
                            
                    edge_ids.extend(entity_edge_ids)

        # === 9. Формируем кандидатов с весами ===
        candidates: List[Dict[str, Any]] = []
        score_by_node = {h["postgres_id"]: h["score"] for h in all_hits}
        now_utc = datetime.now(timezone.utc)

        for node_id, node in nodes_by_id.items():
            base_score = score_by_node.get(node_id, SIMILARITY_THRESHOLD)
            confidence = float(node.get("confidence", 0.5))
            
            count = node.get("retrieval_count", 0) or 0
            last_used = node.get("last_retrieved_at")
            
            pop_bonus = 0.15 * math.log10(count + 1) 
            
            time_decay = 1.0
            if last_used:
                try:
                    if isinstance(last_used, str):
                        last_used = datetime.fromisoformat(last_used)
                    if last_used.tzinfo is None:
                        last_used = last_used.replace(tzinfo=timezone.utc)
                        
                    days_ago = (now_utc - last_used).days
                    time_decay = max(0.3, 1.0 - (days_ago / 90.0)) 
                except Exception:
                    pass
                    
            weighted = (base_score * confidence) * (1 + pop_bonus) * time_decay
            
            if weighted < MIN_WEIGHTED_SCORE:
                continue

            formatted = _format_node_for_context(node)
            candidates.append({
                "node_id": node_id,
                "description": node.get("description"),
                "summary": node.get("summary"),
                "confidence": confidence,
                "weighted_score": weighted,
                "formatted_text": formatted,
            })

        # === 10. Сортировка и ограничение ===
        candidates.sort(key=lambda c: c["weighted_score"], reverse=True)
        candidates = candidates[:MAX_TOTAL_NODES]

        # === 11. Подрезка по токенам ===
        final_candidates, total_tokens, trimmed = _trim_context_to_limit(candidates)
        node_ids_final = [c["node_id"] for c in final_candidates]

        _bump_node_retrieval_stats(node_ids_final, db_config)
        
        raw_content = "\n\n".join(c["formatted_text"] for c in final_candidates)

        # === 12. Сохранение в retrieval_logs ===
        avg_conf = (
            sum(c["confidence"] for c in final_candidates) / len(final_candidates)
            if final_candidates else 0.0
        )
        latency = time.time() - start_time
        
        retrieval_log_id: Optional[str] = None
        with psycopg2.connect(**db_config) as conn4:
            with conn4.cursor() as cur4:
                cur4.execute("""
                    INSERT INTO memory.retrieval_logs (
                        message_id, orchestrator_step_id, routing_context, strategy,
                        filter_domains, filter_topics,
                        node_ids, edge_ids, nodes_count, edges_count,
                        raw_content, total_tokens, trimmed,
                        avg_confidence, latency, sub_queries, agent_version
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s::uuid[], %s::uuid[], %s::uuid[], %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    RETURNING id
                """, (
                    message_id, step_id,
                    Json(routing_context),
                    strategy, domains, topics,
                    node_ids_final, edge_ids,
                    len(final_candidates), len(edge_ids),
                    raw_content, total_tokens, trimmed,
                    avg_conf, latency, sub_queries,
                    agent_version
                ))
                retrieval_log_id = str(cur4.fetchone()[0])

                retrieved_context = {
                    "node_ids": node_ids_final,
                    "edge_ids": edge_ids,
                }
                cur4.execute("""
                    UPDATE dialogs.row_messages
                    SET retrieved_context = %s,
                        retrieval_log_id = %s
                    WHERE id = %s
                """, (Json(retrieved_context), retrieval_log_id, message_id))
            conn4.commit()

        # === 13. Завершаем шаг успешно ===
        complete_step_success(
            step_id=step_id,
            output_data={
                "nodes_count": len(final_candidates),
                "edges_count": len(edge_ids),
                "total_tokens": total_tokens,
                "trimmed": trimmed,
                "avg_confidence": avg_conf,
                "latency": latency,
                "strategy": strategy,
                "retrieval_log_id": retrieval_log_id,
                "sub_queries_count": len(sub_queries) if sub_queries else 0,
                "embeddings_count": len(query_vectors),
            },
            emb_metric_id=emb_metric_id,
        )

        logger.info(
            "Retrieval OK: nodes=%d, tokens=%d/%d, trimmed=%s, strategy=%s",
            len(final_candidates), total_tokens, MAX_CONTEXT_TOKENS, trimmed, strategy
        )

        return {
            "nodes_count": len(final_candidates),
            "edges_count": len(edge_ids),
            "total_tokens": total_tokens,
            "trimmed": trimmed,
            "avg_confidence": avg_conf,
            "latency": latency,
            "strategy": strategy,
            "retrieval_log_id": retrieval_log_id,
            "sub_queries_count": len(sub_queries) if sub_queries else 0,
            "embeddings_count": len(query_vectors),
        }

    except Exception as exc:
        logger.exception("Retrieval step failed: %s", exc)

        try:
            with psycopg2.connect(**db_config) as conn_err:
                with conn_err.cursor() as cur_err:
                    cur_err.execute("""
                        INSERT INTO memory.retrieval_logs (
                            message_id, orchestrator_step_id, routing_context, strategy,
                            filter_domains, filter_topics,
                            node_ids, edge_ids, nodes_count, edges_count,
                            raw_content, total_tokens, trimmed,
                            error_message, latency, sub_queries, agent_version
                        ) VALUES (
                            %s, %s, %s, 'failed', %s, %s::uuid[], '{}'::uuid[], '{}'::uuid[], 0, 0, '', 0, FALSE,
                            %s, %s, %s, %s
                        )
                        RETURNING id
                    """, (
                        message_id, step_id,
                        Json(routing_context),
                        domains, topics,
                        str(exc), time.time() - start_time, sub_queries,
                        agent_version
                    ))
                    fallback_log_id = str(cur_err.fetchone()[0])

                    cur_err.execute("""
                        UPDATE dialogs.row_messages
                        SET retrieved_context = %s,
                            retrieval_log_id = %s
                        WHERE id = %s
                    """, (
                        Json({"node_ids": [], "edge_ids": []}),
                        fallback_log_id,
                        message_id
                    ))
                conn_err.commit()
        except Exception as e2:
            logger.error("Failed to write fallback retrieval: %s", e2)

        complete_step_error(
            step_id=step_id,
            error_module="preprocessing.retrieval_composer",
            error_message=str(exc),
            emb_metric_id=emb_metric_id,
        )
        raise
    finally:
        if conn:
            conn.close()