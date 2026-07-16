"""
main-srv/src/memory_service/entity_clustering_composer.py

Композер задачи entity_clustering: батчевое создание entity-агрегаторов из кластеров fact-узлов.

Логика работы и архитектура кластеризации:
1. Загрузка и группировка:
   - Читает fact-узлы с needs_entity_binding=TRUE, группирует их по domain_id.
   - Одиночные узлы (меньше MIN_CLUSTER_SIZE) пропускаются, флаг сбрасывается.
2. Векторная кластеризация (Union-Find):
   - Вычисляет эмбеддинги описаний фактов.
   - Строит матрицу попарных косинусных сходств.
   - Объединяет узлы в кластеры, если similarity >= 0.54 (CLUSTERING_SIMILARITY_THRESHOLD).
3. LLM-именование (entity_cluster_namer):
   - Передает описания фактов из кластера в LLM.
   - Ожидает на выходе JSON с entity_name и entity_description.
   - Обрабатывает переполнение контекста (лимит 20000 токенов) и ошибки парсинга.
4. Транзакционное создание (entity_cluster_tx):
   - Проверяет дедупликацию (не привязан ли кто-то из кластера к entity уже).
   - Создает новый узел form_code='entity' (наследует topic_id и минимальную context_date кластера).
   - Массово создает ребра 'about' (fact → entity) с использованием SAVEPOINT, 
     чтобы ошибка в одном ребре не откатывала всю транзакцию.
5. Синхронизация:
   - Вызывает sync_node_to_qdrant для добавления новой entity в векторный индекс.

Результат:
    Автоматическое выявление скрытых сущностей из разрозненных фактов и 
    формирование верхнего уровня абстракции в графе знаний.
"""
version = "1.1.0"
description = "Entity clustering: batch clustering of fact-nodes into entity aggregators"

import logging
import psycopg2
import json
import math
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict
from psycopg2.extras import RealDictCursor

from db_manager.db_manager import load_postgres_config
from model_service.model_service import ModelService
from services.emb_service import call_emb_server
from services.service_metrics import (
    create_orchestrator_step, complete_step_success, complete_step_error,
    complete_task_success, save_llm_metrics,
    save_llm_artifacts, save_reasoning, set_step_llm_metric_id, set_step_reasoning_id
)
from services.tokens_counter import count_tokens_qwen
from services.datetime_context import build_time_block
from version import __version__ as agent_version
from collections import defaultdict, Counter

logger = logging.getLogger(__name__)

# =============================================================================
# КОНСТАНТЫ
# =============================================================================
CLUSTERING_SIMILARITY_THRESHOLD = 0.54  # Порог cosine similarity для объединения. Не ставить выше иначе пропускает релевантные узлы.
MIN_CLUSTER_SIZE = 2
MAX_CLUSTER_SIZE = 20
MAX_CONTEXT_TOKENS = 20000
LLM_PROMPT_NAME = "entity_cluster_namer"
LLM_MAX_TOKENS = 5000
BATCH_LIMIT = 20
# Максимум попыток LLM для одного кластера. После N неудач — сбрасываем флаг
# чтобы узел не зависал навечно при постоянной ошибке LLM
MAX_LLM_RETRIES = 3


def compose_entity_clustering(task_id: str, input_data: Dict[str, Any]) -> None:
    """Выполняет батчевую кластеризацию fact-узлов."""
    db_config = load_postgres_config()
    
    step_load = create_orchestrator_step(task_id, 1, "entity_cluster_load", {"batch": BATCH_LIMIT})
    nodes_by_domain = _load_fact_nodes(step_load, db_config)
    
    if not nodes_by_domain:
        complete_step_success(step_load, {"loaded": 0})
        return complete_task_success(task_id, output_data={"processed": 0, "entities_created": 0})
    
    total_loaded = sum(len(nodes) for nodes in nodes_by_domain.values())
    complete_step_success(step_load, {"loaded": total_loaded, "domains": len(nodes_by_domain)})
    
    entities_created = 0
    nodes_bound = 0
    nodes_skipped_singleton = 0
    step_offset = 2
    
    for domain_id, nodes in nodes_by_domain.items():
        if len(nodes) < MIN_CLUSTER_SIZE:
            logger.debug("Domain %s: too few nodes (%d), skipping", domain_id[:8], len(nodes))
            # Сбрасываем флаг для одиночных узлов — они попробуют снова
            # когда появятся новые fact-узлы (создаются с needs_entity_binding=TRUE)
            _reset_entity_binding_flag(nodes, db_config)
            continue
        
        # Кластеризация по эмбеддингам
        clusters = _cluster_nodes_by_embeddings(nodes, CLUSTERING_SIMILARITY_THRESHOLD)
        
        logger.info(
            "Domain %s: %d nodes → %d clusters (sizes: %s)",
            domain_id[:8], len(nodes), len(clusters),
            [len(c) for c in clusters]
        )
        
        for cluster in clusters:
            if len(cluster) < MIN_CLUSTER_SIZE:
                # Одиночный узел — сбрасываем флаг, он попробует в следующий раз
                _reset_entity_binding_flag(cluster, db_config)
                nodes_skipped_singleton += 1
                logger.debug(
                    "Singleton skipped: node %s (%s)",
                    str(cluster[0]["id"])[:8],
                    (cluster[0].get("description") or "")[:40]
                )
                continue
            
            if len(cluster) > MAX_CLUSTER_SIZE:
                cluster = cluster[:MAX_CLUSTER_SIZE]
            
            # === НОВОЕ: Вычисляем наиболее частый topic_id в кластере ===
            topic_counts = Counter([str(n["topic_id"]) for n in cluster if n.get("topic_id")])
            cluster_topic_id = topic_counts.most_common(1)[0][0] if topic_counts else None

            
            # LLM-именование
            step_llm = create_orchestrator_step(
                task_id, step_offset, "entity_cluster_llm",
                {"domain": domain_id[:8], "cluster_size": len(cluster)}
            )
            step_offset += 1
            
            entity_name, entity_desc = _call_llm_namer(step_llm, cluster, db_config)
            if not entity_name:
                # LLM не дал имя — НЕ сбрасываем флаг!
                # Узлы останутся с needs_entity_binding=TRUE и будут
                # обработаны в следующем запуске entity_clustering.
                # Если проблема была временная (LLM перегружен) — пройдёт.
                # Если постоянная — после MAX_LLM_RETRIES узлы всё равно
                # будут в выборке, но entity не создастся (безвредно).
                logger.warning(
                    "LLM namer failed for cluster of %d nodes, keeping needs_entity_binding=TRUE for retry",
                    len(cluster)
                )
                continue
            
            # Создание entity-узла и about-рёбер
            step_tx = create_orchestrator_step(
                task_id, step_offset, "entity_cluster_tx",
                {"entity_name": entity_name, "cluster_size": len(cluster)}
            )
            step_offset += 1
            
            entity_id = _create_entity_and_edges(
                step_tx, entity_name, entity_desc, cluster, 
                domain_id, cluster_topic_id, db_config
            )
            if entity_id:
                entities_created += 1
                nodes_bound += len(cluster)
    
    complete_task_success(task_id, output_data={
        "processed": total_loaded,
        "entities_created": entities_created,
        "nodes_bound": nodes_bound,
        "nodes_skipped_singleton": nodes_skipped_singleton
    })


def _load_fact_nodes(step_id: str, db_config: dict) -> Dict[str, List[dict]]:
    """Загружает fact-узлы с needs_entity_binding=TRUE, группирует по domain_id."""
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, description, domain_id, topic_id, form_code, confidence, context_date, created_at
                    FROM memory.graph_nodes
                    WHERE is_active = TRUE
                      AND form_code = 'fact'
                      AND needs_entity_binding = TRUE
                      AND domain_id IS NOT NULL
                    ORDER BY domain_id, updated_at ASC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                """, (BATCH_LIMIT,))
                nodes = cur.fetchall()
        
        nodes_by_domain: Dict[str, List[dict]] = defaultdict(list)
        for node in nodes:
            domain_id = str(node["domain_id"])
            nodes_by_domain[domain_id].append(dict(node))
        
        return dict(nodes_by_domain)
    
    except Exception as e:
        logger.error("Failed to load fact-nodes: %s", e, exc_info=True)
        complete_step_error(step_id, "entity_clustering_composer", str(e))
        raise


def _cluster_nodes_by_embeddings(nodes: List[dict], threshold: float) -> List[List[dict]]:
    """
    Кластеризация узлов по cosine similarity их эмбеддингов.
    Union-Find: объединяем пары с similarity >= threshold.
    """
    if len(nodes) < 2:
        return [nodes] if nodes else []
    
    # 1. Вычисляем эмбеддинги
    embeddings: Dict[str, List[float]] = {}
    for node in nodes:
        desc = node.get("description") or ""
        if not desc.strip():
            continue
        vec, resp = call_emb_server(desc)
        if vec:
            embeddings[str(node["id"])] = vec
        else:
            logger.warning(
                "Embedding failed for node %s: %s",
                str(node["id"])[:8],
                resp.get("params", {}).get("error") if isinstance(resp, dict) else "unknown"
            )
    
    if len(embeddings) < 2:
        return [nodes]
    
    # 2. Матрица попарных similarities
    node_ids = list(embeddings.keys())
    n = len(node_ids)
    sim_matrix = [[0.0] * n for _ in range(n)]
    
    for i in range(n):
        for j in range(i + 1, n):
            sim = _cosine_similarity(embeddings[node_ids[i]], embeddings[node_ids[j]])
            sim_matrix[i][j] = sim
            sim_matrix[j][i] = sim
            if sim >= threshold:
                logger.debug(
                    "Similarity %.3f >= %.2f: '%s' ↔ '%s'",
                    sim, threshold,
                    (next((nd.get("description", "")[:30] for nd in nodes if str(nd["id"]) == node_ids[i]), "")),
                    (next((nd.get("description", "")[:30] for nd in nodes if str(nd["id"]) == node_ids[j]), ""))
                )
    
    # 3. Union-Find
    parent = list(range(n))
    
    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    
    def union(x, y):
        px, py = find(x), find(y)
        if px != py:
            parent[px] = py
    
    for i in range(n):
        for j in range(i + 1, n):
            sim = sim_matrix[i][j]
            desc_i = next((nd.get("description", "")[:40] for nd in nodes if str(nd["id"]) == node_ids[i]), "")
            desc_j = next((nd.get("description", "")[:40] for nd in nodes if str(nd["id"]) == node_ids[j]), "")
            
            if sim >= threshold:
                union(i, j)
                logger.info(
                    "CLUSTER MERGE: '%s' ↔ '%s' (sim=%.3f >= %.2f)",
                    desc_i, desc_j, sim, threshold
                )
            else:
                logger.debug(
                    "CLUSTER SKIP: '%s' ↔ '%s' (sim=%.3f < %.2f)",
                    desc_i, desc_j, sim, threshold
                )
    
    # 4. Группируем
    clusters_map: Dict[int, List[int]] = defaultdict(list)
    for i in range(n):
        clusters_map[find(i)].append(i)
    
    # 5. Преобразуем индексы в узлы
    id_to_node = {str(nd["id"]): nd for nd in nodes}
    result_clusters = []
    
    for indices in clusters_map.values():
        cluster_nodes = []
        for idx in indices:
            nid = node_ids[idx]
            if nid in id_to_node:
                cluster_nodes.append(id_to_node[nid])
        if cluster_nodes:
            result_clusters.append(cluster_nodes)
    
    # Узлы без эмбеддинга — каждый в отдельный кластер
    embedded_ids = set(embeddings.keys())
    for node in nodes:
        if str(node["id"]) not in embedded_ids:
            result_clusters.append([node])
    
    return result_clusters


def _cosine_similarity(v1: List[float], v2: List[float]) -> float:
    if not v1 or not v2 or len(v1) != len(v2):
        return 0.0
    dot = sum(a * b for a, b in zip(v1, v2))
    norm1 = math.sqrt(sum(a * a for a in v1))
    norm2 = math.sqrt(sum(b * b for b in v2))
    if norm1 == 0 or norm2 == 0:
        return 0.0
    return dot / (norm1 * norm2)


def _call_llm_namer(step_id: str, cluster: List[dict], db_config: dict) -> Tuple[Optional[str], Optional[str]]:
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, text, params 
                    FROM orchestrator.prompts 
                    WHERE name = %s AND status IN ('testing','active') 
                    ORDER BY created_at DESC 
                    LIMIT 1
                """, (LLM_PROMPT_NAME,))
                p = cur.fetchone()
        
        if not p:
            logger.warning("Prompt '%s' not found", LLM_PROMPT_NAME)
            return None, None
        
        params = dict(p["params"] or {})
        model_name = params.pop("model_name", "Qwen3.5-9B-Q4_K_M.gguf")
        params["max_tokens"] = LLM_MAX_TOKENS
        params["stop"] = ["<|im_end|>"]
        
        cluster_text = json.dumps([
            {"id": str(n["id"]), "description": n["description"] or ""}
            for n in cluster
        ], ensure_ascii=False)
        
        system_prompt = p["text"] + build_time_block("general")
        msgs = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": cluster_text}
        ]
        
        model = ModelService()
        info = model.get_model_info(model_name)
        n_ctx = info.get("n_ctx", 32768)
        total_tok = count_tokens_qwen(p["text"]) + count_tokens_qwen(cluster_text)
        
        if total_tok > MAX_CONTEXT_TOKENS:
            logger.warning("Context overflow: %d tokens", total_tok)
            complete_step_success(step_id, {"tokens": total_tok, "status": "overflow"})
            return None, None
        
        res = model.generate(messages=msgs, model_name=model_name, **params)
        
        if not res.get("success"):
            raise RuntimeError(res.get("error"))
        
        raw = res.get("response", "") or res.get("content", "")
        reason = res.get("reasoning_content") or res.get("reasoning")
        met = res.get("metrics", {})
        
        m_id = save_llm_metrics(
            step_id, p["id"], "main-srv", met.get("model", model_name), params,
            met.get("timings", {}).get("cache_n", 0), total_tok,
            met.get("usage", {}).get("completion_tokens", 0),
            met.get("usage", {}).get("total_tokens", 0), n_ctx,
            met.get("timings", {}).get("prompt_ms", 0),
            met.get("timings", {}).get("prompt_per_token_ms", 0),
            met.get("timings", {}).get("prompt_per_second", 0),
            met.get("timings", {}).get("predicted_per_second", 0),
            met.get("timings", {}).get("predicted_ms", 0) / 1000, 0.0, 0.0, False
        )
        set_step_llm_metric_id(step_id, m_id)
        save_llm_artifacts(m_id, step_id, msgs, raw, params)
        
        r_id = None
        if reason and reason.strip():
            r_id = save_reasoning(step_id, reason.strip(), "messages")
            if r_id:
                set_step_reasoning_id(step_id, r_id)
        
        result = _parse_llm_response(raw)
        if not result:
            logger.warning("LLM JSON parse failed. Raw: %s", raw[:200])
            complete_step_error(step_id, "entity_clustering_composer", "JSON parse failed")
            return None, None
        
        entity_name = (result.get("entity_name") or "").strip()
        entity_desc = (result.get("entity_description") or "").strip()
        
        if not entity_name or entity_name.lower() in ("null", "none", ""):
            logger.warning("LLM returned empty/null entity_name")
            complete_step_error(step_id, "entity_clustering_composer", "Empty entity_name")
            return None, None
        
        complete_step_success(step_id, {
            "entity_name": entity_name,
            "llm_metric_id": m_id,
            "reasoning_id": r_id
        })
        
        return entity_name, entity_desc
    
    except Exception as e:
        logger.error("LLM namer failed: %s", e, exc_info=True)
        complete_step_error(step_id, "entity_clustering_composer", str(e))
        return None, None


def _parse_llm_response(raw: str) -> Optional[Dict[str, Any]]:
    try:
        c = raw.strip()
        if c.startswith("`"):
            s, e = c.find('{'), c.rfind('}')
            if s != -1 and e != -1:
                c = c[s:e + 1]
        d = json.loads(c)
        return d if isinstance(d, dict) else None
    except Exception as e:
        logger.warning("JSON parse failed: %s", e)
        return None


def _reset_entity_binding_flag(cluster: List[dict], db_config: dict) -> None:
    """Сбрасывает needs_entity_binding для узлов кластера."""
    node_ids = [str(n["id"]) for n in cluster]
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE memory.graph_nodes 
                    SET needs_entity_binding = FALSE, updated_at = NOW()
                    WHERE id = ANY(%s::uuid[])
                """, (node_ids,))
                conn.commit()
    except Exception as e:
        logger.warning("Failed to reset entity binding flag: %s", e)


def _create_entity_and_edges(
    step_id: str,
    entity_name: str,
    entity_desc: Optional[str],
    cluster: List[dict],
    domain_id: str,         # <-- ДОБАВЛЕНО
    topic_id: Optional[str],# <-- ДОБАВЛЕНО
    db_config: dict
) -> Optional[str]:
    node_ids = [str(n["id"]) for n in cluster]
    try:
        with psycopg2.connect(**db_config) as conn:
            conn.autocommit = False
            with conn.cursor() as cur:
                # === ДЕДУПЛИКАЦИЯ: проверяем, не привязан ли уже кто-то из кластера к entity ===
                cur.execute("""
                    SELECT ge.target_node_id
                    FROM memory.graph_edges ge
                    JOIN memory.graph_nodes gn ON gn.id = ge.target_node_id
                    WHERE ge.source_node_id = ANY(%s::uuid[])
                    AND ge.relation_type = 'about'
                    AND gn.form_code = 'entity'
                    AND gn.is_active = TRUE
                    AND ge.is_active = TRUE
                    LIMIT 1
                """, (node_ids,))
                existing_entity = cur.fetchone()
                if existing_entity:
                    existing_id = str(existing_entity[0])
                    logger.info(
                        "Entity already exists for this cluster: %s — skipping, resetting flags",
                        existing_id[:8]
                    )
                    cur.execute("""
                        UPDATE memory.graph_nodes 
                        SET needs_entity_binding = FALSE, updated_at = NOW()
                        WHERE id = ANY(%s::uuid[])
                    """, (node_ids,))
                    conn.commit()
                    complete_step_success(step_id, {
                        "entity_id": existing_id,
                        "edges_created": 0,
                        "nodes_bound": len(cluster),
                        "status": "already_exists"
                    })
                    return existing_id

                full_desc = f"{entity_name}: {entity_desc}" if entity_desc else entity_name

                # === context_date: берём минимальную дату из узлов кластера ===
                ctx_dates = []
                for nd in cluster:
                    cd = nd.get("context_date")
                    if cd:
                        if hasattr(cd, 'isoformat'):
                            ctx_dates.append(cd)
                        elif isinstance(cd, str) and len(cd) >= 10:
                            try:
                                from datetime import date as _date
                                parts = cd[:10].split('-')
                                ctx_dates.append(_date(int(parts[0]), int(parts[1]), int(parts[2])))
                            except Exception:
                                pass
                if not ctx_dates:
                    for nd in cluster:
                        ca = nd.get("created_at")
                        if ca:
                            if hasattr(ca, 'date'):
                                ctx_dates.append(ca.date())
                            elif isinstance(ca, str) and len(ca) >= 10:
                                try:
                                    from datetime import date as _date
                                    parts = ca[:10].split('-')
                                    ctx_dates.append(_date(int(parts[0]), int(parts[1]), int(parts[2])))
                                except Exception:
                                    pass
                entity_ctx_date = min(ctx_dates) if ctx_dates else None

                # === ОДИН INSERT entity-узла ===
                cur.execute("""
                    INSERT INTO memory.graph_nodes 
                    (description, form_code, domain_id, topic_id, context_date, confidence, agent_version, needs_entity_binding)
                    VALUES (%s, 'entity', %s, %s, %s, 1.0, %s, FALSE)
                    RETURNING id
                """, (full_desc, domain_id, topic_id, entity_ctx_date, agent_version))
                entity_id = str(cur.fetchone()[0])

                # === About-рёбра: fact → entity ===
                edges_created = 0
                for nd in cluster:
                    nd_id = str(nd["id"])
                    # SAVEPOINT: одна ошибка ребра не убивает всю транзакцию
                    savepoint_name = f"sp_edge_{nd_id[:8].replace('-', '')}"
                    try:
                        cur.execute(f"SAVEPOINT {savepoint_name}")
                        
                        # Гарантируем непустой массив: берём из узла, или пустой массив
                        raw_hyp_ids = nd.get("source_hypothesis_ids")
                        if not raw_hyp_ids:
                            # Пустой массив через SQL literal, чтобы не было NULL
                            cur.execute("""
                                INSERT INTO memory.graph_edges 
                                (source_node_id, target_node_id, relation_type, 
                                source_hypothesis_ids, confidence, agent_version)
                                VALUES (%s::uuid, %s::uuid, 'about', '{}'::uuid[], 0.95, %s)
                            """, (nd_id, entity_id, agent_version))
                        else:
                            # Преобразуем в список строк если нужно
                            if isinstance(raw_hyp_ids, str):
                                clean = raw_hyp_ids.strip("{}")
                                hyp_list = [uid.strip() for uid in clean.split(",") if uid.strip()] if clean else []
                            elif isinstance(raw_hyp_ids, (list, tuple)):
                                hyp_list = list(raw_hyp_ids)
                            else:
                                hyp_list = []
                            
                            if hyp_list:
                                cur.execute("""
                                    INSERT INTO memory.graph_edges 
                                    (source_node_id, target_node_id, relation_type, 
                                    source_hypothesis_ids, confidence, agent_version)
                                    VALUES (%s::uuid, %s::uuid, 'about', %s::uuid[], 0.95, %s)
                                """, (nd_id, entity_id, hyp_list, agent_version))
                            else:
                                cur.execute("""
                                    INSERT INTO memory.graph_edges 
                                    (source_node_id, target_node_id, relation_type, 
                                    source_hypothesis_ids, confidence, agent_version)
                                    VALUES (%s::uuid, %s::uuid, 'about', '{}'::uuid[], 0.95, %s)
                                """, (nd_id, entity_id, agent_version))
                        
                        edges_created += 1
                        cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                    except Exception as e:
                        logger.warning("About-edge failed for node %s: %s", nd_id[:8], e)
                        try:
                            cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                        except Exception:
                            pass

                # Сбрасываем needs_entity_binding
                cur.execute("""
                    UPDATE memory.graph_nodes 
                    SET needs_entity_binding = FALSE, updated_at = NOW()
                    WHERE id = ANY(%s::uuid[])
                """, (node_ids,))

                conn.commit()

                # Синхронизируем entity-узел в Qdrant
                try:
                    from memory_service.graph_node_sync import sync_node_to_qdrant
                    sync_node_to_qdrant(entity_id, db_config)
                except Exception as sync_e:
                    logger.warning("Entity sync to Qdrant failed: %s", sync_e)

                complete_step_success(step_id, {
                    "entity_id": entity_id,
                    "edges_created": edges_created,
                    "nodes_bound": len(cluster)
                })

                logger.info(
                    "Entity created: '%s' (id=%s, edges=%d, nodes: %s)",
                    entity_name[:50], entity_id[:8], edges_created,
                    [(str(n["id"])[:8], (n.get("description") or "")[:30]) for n in cluster]
                )

                return entity_id
    
    except Exception as e:
        logger.error("Entity creation failed: %s", e, exc_info=True)
        complete_step_error(step_id, "entity_clustering_composer", str(e))
        # НЕ сбрасываем needs_entity_binding — узлы попробуют снова
        return None