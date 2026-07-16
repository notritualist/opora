"""
main-srv/src/memory_service/entity_binding_composer.py

Композер задачи entity_binding: инкрементальная привязка новых fact-узлов к существующим entity.

Логика работы и архитектура привязки:
1. Загрузка (entity_binding_load): 
   - Читает батч (до 50 шт.) новых fact-узлов с флагом needs_entity_binding=TRUE.
2. Векторизация и поиск (entity_binding_match):
   - Вызывает embedding-сервис для описания факта.
   - Ищет TOP-5 ближайших узлов в Qdrant.
   - Пост-фильтрация в PostgreSQL: отбрасывает результаты, если они не являются entity-узлами.
3. Принятие решения и транзакция (entity_binding_tx):
   - Если score >= 0.75 (AUTO_BIND_THRESHOLD): автоматическая привязка (создание ребра 'about').
   - Если score >= 0.60 (REVIEW_BIND_THRESHOLD): привязка с установкой needs_review=TRUE.
   - Если score < 0.60: узел пропускается (остается для батчевой кластеризации).
4. Обновление состояния:
   - Сбрасывает флаг needs_entity_binding=FALSE у успешно привязанных фактов.
   - Проверяет дубликаты ребер перед INSERT (ON CONFLICT DO NOTHING).

Результат:
    Граф знаний оперативно обогащается связями фактов с глобальными сущностями 
    без ожидания тяжелого процесса кластеризации.
"""
version = "1.1.0"
description = "Entity binding: incremental binding of new fact-nodes to existing entities"

import logging
import psycopg2
from typing import Dict, Any, List
from psycopg2.extras import RealDictCursor

from db_manager.db_manager import load_postgres_config
from db_manager.qdrant_manager import search_similar_graph_nodes
from services.emb_service import call_emb_server
from services.service_metrics import (
    create_orchestrator_step, complete_step_success, complete_step_error,
    complete_task_success
)

from version import __version__ as agent_version

logger = logging.getLogger(__name__)

# =============================================================================
# КОНСТАНТЫ (настраиваемые параметры)
# =============================================================================
# Порог косинусной близости qdrant для автоматической привязки
# 0.75 = очень похожие (автопривязка), 0.7 = умеренно похожие (review), <0.6 = пропуск
AUTO_BIND_THRESHOLD = 0.75
REVIEW_BIND_THRESHOLD = 0.6

# Максимальное количество узлов для обработки за один запуск
BATCH_LIMIT = 50

# Количество ближайших entity-узлов для поиска
TOP_K_ENTITIES = 5


def compose_entity_binding(task_id: str, input_data: Dict[str, Any]) -> None:
    """Выполняет инкрементальную привязку fact-узлов к entity."""
    db_config = load_postgres_config()
    
    # === ШАГ 1: Загрузка узлов ===
    step_load = create_orchestrator_step(task_id, 1, "entity_binding_load", {"batch": BATCH_LIMIT})
    nodes = _load_new_fact_nodes(step_load, db_config)
    
    if not nodes:
        complete_step_success(step_load, {"loaded": 0})
        return complete_task_success(task_id, output_data={"processed": 0, "bound": 0})
    
    complete_step_success(step_load, {"loaded": len(nodes)})
    
    # === ШАГ 2-3: Поиск и привязка (для каждого узла) ===
    nodes_bound = 0
    step_offset = 2
    
    for node in nodes:
        # Векторизация описания
        vec, emb_resp = call_emb_server(node["description"] or "")
        if not vec:
            logger.warning("Embedding failed for node %s", node["id"][:8])
            continue
        
        # Поиск ближайших entity
        step_match = create_orchestrator_step(
            task_id, step_offset, "entity_binding_match",
            {"node": node["id"][:8]}
        )
        step_offset += 1
        
        entity_hits = _find_nearest_entities(step_match, vec, db_config)
        
        if not entity_hits:
            continue
        
        # Проверяем пороги
        best_hit = entity_hits[0]
        score = best_hit.get("score", 0.0)
        
        if score >= AUTO_BIND_THRESHOLD:
            # Автоматическая привязка
            step_tx = create_orchestrator_step(
                task_id, step_offset, "entity_binding_tx",
                {"node": node["id"][:8], "entity": best_hit["postgres_id"][:8], "score": score}
            )
            step_offset += 1
            
            if _create_about_edge(step_tx, node, best_hit, needs_review=False, db_config=db_config):
                nodes_bound += 1
        
        elif score >= REVIEW_BIND_THRESHOLD:
            # Привязка с пометкой needs_review
            step_tx = create_orchestrator_step(
                task_id, step_offset, "entity_binding_tx",
                {"node": node["id"][:8], "entity": best_hit["postgres_id"][:8], "score": score}
            )
            step_offset += 1
            
            _create_about_edge(step_tx, node, best_hit, needs_review=True, db_config=db_config)
        
        else:
            # Оставляем для батчевой кластеризации
            logger.debug(
                "Node %s: best score %.3f < %.3f, leaving for clustering",
                node["id"][:8], score, REVIEW_BIND_THRESHOLD
            )
    
    complete_task_success(task_id, output_data={
        "processed": len(nodes),
        "bound": nodes_bound
    })


def _load_new_fact_nodes(step_id: str, db_config: dict) -> List[dict]:
    """Загружает новые fact-узлы с needs_entity_binding=TRUE."""
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, description, domain_id, topic_id, form_code
                    FROM memory.graph_nodes
                    WHERE is_active = TRUE
                      AND form_code = 'fact'
                      AND needs_entity_binding = TRUE
                    ORDER BY created_at DESC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                """, (BATCH_LIMIT,))
                return [dict(r) for r in cur.fetchall()]
    
    except Exception as e:
        logger.error("Failed to load fact-nodes: %s", e, exc_info=True)
        complete_step_error(step_id, "entity_binding_composer", str(e))
        raise


def _find_nearest_entities(step_id: str, vector: list, db_config: dict) -> List[Dict[str, Any]]:
    """Ищет ближайшие entity-узлы через Qdrant."""
    try:
        # Поиск без фильтрации (post-filter через PostgreSQL)
        raw_hits = search_similar_graph_nodes(
            vector=vector,
            actor_id=None,
            limit=TOP_K_ENTITIES,
            score_threshold=REVIEW_BIND_THRESHOLD,
            candidate_limit=TOP_K_ENTITIES * 3
        )
        
        if not raw_hits:
            complete_step_success(step_id, {"found": 0})
            return []
        
        # Post-filter: оставляем только entity-узлы
        pg_ids = [h["postgres_id"] for h in raw_hits if h.get("postgres_id")]
        if not pg_ids:
            complete_step_success(step_id, {"found": 0})
            return []
        
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, form_code 
                    FROM memory.graph_nodes 
                    WHERE id = ANY(%s::uuid[]) 
                      AND form_code = 'entity'
                      AND is_active = TRUE
                """, (pg_ids,))
                entity_ids = {str(row["id"]) for row in cur.fetchall()}
        
        entity_hits = [h for h in raw_hits if h.get("postgres_id") in entity_ids]
        
        complete_step_success(step_id, {"found": len(entity_hits)})
        
        return entity_hits
    
    except Exception as e:
        logger.error("Entity search failed: %s", e, exc_info=True)
        complete_step_error(step_id, "entity_binding_composer", str(e))
        return []


def _create_about_edge(
    step_id: str,
    fact_node: dict,
    entity_hit: Dict[str, Any],
    needs_review: bool,
    db_config: dict
) -> bool:
    """Создаёт about-ребро и сбрасывает needs_entity_binding."""
    try:
        fact_id = str(fact_node["id"])
        entity_id = entity_hit["postgres_id"]
        score = entity_hit.get("score", 0.0)
        
        with psycopg2.connect(**db_config) as conn:
            conn.autocommit = False
            with conn.cursor() as cur:
                # Pre-check: about-ребро уже существует?
                cur.execute("""
                    SELECT 1 FROM memory.graph_edges 
                    WHERE source_node_id = %s::uuid 
                    AND target_node_id = %s::uuid 
                    AND relation_type = 'about'
                    AND is_active = TRUE
                    LIMIT 1
                """, (fact_id, entity_id))
                if cur.fetchone():
                    logger.debug("About-edge already exists: %s→%s", fact_id[:8], entity_id[:8])
                    return True  # Уже есть — считаем успехом
                
                
                # Создаём ребро
                src_hyp_array = fact_node.get("source_hypothesis_ids") or None
                cur.execute("""
                    INSERT INTO memory.graph_edges 
                    (source_node_id, target_node_id, relation_type, 
                     source_hypothesis_ids, confidence, needs_review, agent_version)
                    VALUES (%s::uuid, %s::uuid, 'about', %s::uuid[], %s, %s::boolean, %s)
                    ON CONFLICT (source_node_id, target_node_id, relation_type) DO NOTHING
                """, (
                    fact_id, entity_id,
                    src_hyp_array,
                    score, bool(needs_review), agent_version
                ))
                
                # Сбрасываем needs_entity_binding
                cur.execute("""
                    UPDATE memory.graph_nodes 
                    SET needs_entity_binding = FALSE, updated_at = NOW()
                    WHERE id = %s
                """, (fact_id,))
                
                conn.commit()
        
        complete_step_success(step_id, {
            "fact_id": fact_id,
            "entity_id": entity_id,
            "score": score,
            "needs_review": needs_review
        })
        
        logger.info(
            "Bound node %s → entity %s (score=%.3f, review=%s)",
            fact_id[:8], entity_id[:8], score, needs_review
        )
        
        return True
    
    except Exception as e:
        logger.error("Edge creation failed: %s", e, exc_info=True)
        complete_step_error(step_id, "entity_binding_composer", str(e))
        return False