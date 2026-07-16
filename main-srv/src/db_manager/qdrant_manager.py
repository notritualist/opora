"""
main-srv/src/db_manager/qdrant_manager.py

Менеджер работы с векторной БД Qdrant для псевдографа знаний.
Архитектура:
- Единая коллекция 'opora_db'
- Payload: graph_nodes (postgres_id, actor_id, topic_id, domain_id)
- Операции: upsert (создание/обновление), поиск с жёсткой фильтрацией по UUID домена и темы
- Все обновления описаний узлов требуют явного вызова upsert_graph_node_vector
- Типизация условий фильтрации приведена к list для совместимости с qdrant-client и Pylance
"""
version = "1.2.0"
description = "Менеджер Qdrant для псевдографа с фильтрацией по domain_id/topic_id и upsert-логикой"

import logging
import uuid
from typing import List, Dict, Any, Optional, cast
from qdrant_client import QdrantClient
from qdrant_client.models import (
    PointStruct, Filter, FieldCondition, MatchValue,
    IsEmptyCondition, PayloadField, MinShould, ScoredPoint
)
from db_manager.db_manager import load_qdrant_config
from db_manager.qdrant_schema import build_graph_node_payload

logger = logging.getLogger(__name__)
_qdrant_client: Optional[QdrantClient] = None
_qdrant_collection: str = "opora_db"

def get_qdrant_client() -> QdrantClient:
    """Ленивая инициализация клиента Qdrant."""
    global _qdrant_client
    if _qdrant_client is None:
        config = load_qdrant_config()
        _qdrant_client = QdrantClient(
            host=config.get("host", "localhost"),
            port=config.get("port", 6333),
            timeout=30
        )
        logger.info("Qdrant клиент инициализирован: %s:%s", config.get("host"), config.get("port"))
    return _qdrant_client

def upsert_graph_node_vector(
    vector: List[float],
    postgres_node_id: str,
    actor_id: Optional[str] = None,
    qdrant_point_id: Optional[str] = None
) -> str:
    """
    Сохраняет или обновляет вектор узла графа в Qdrant.
    Payload минимальный: type, postgres_id, actor_id.
    """
    try:
        client = get_qdrant_client()
        point_id = qdrant_point_id or str(uuid.uuid4())
        payload = build_graph_node_payload(
            postgres_node_id=postgres_node_id,
            actor_id=actor_id
        )
        client.upsert(
            collection_name=_qdrant_collection,
            points=[PointStruct(id=point_id, vector=vector, payload=payload)]
        )
        logger.debug("Qdrant upsert: node=%s → point=%s", postgres_node_id[:8], point_id[:8])
        return point_id
    except Exception as e:
        logger.error("❌ Qdrant upsert failed for node %s: %s", postgres_node_id[:8], str(e), exc_info=True)
        raise

def search_similar_graph_nodes(
    vector: List[float],
    actor_id: Optional[str] = None,
    limit: int = 5,
    score_threshold: float = 0.70,
    candidate_limit: int = 50
) -> List[Dict[str, Any]]:
    """
    Поиск похожих узлов графа БЕЗ фильтрации по domain/topic в Qdrant.
    Возвращает все похожие точки. Post-filter по domain/topic/form 
    выполняется в вызывающем коде через PostgreSQL.
    
    Args:
        vector: Вектор запроса
        actor_id: UUID владельца (опционально)
        limit: Максимум результатов (применяется после Qdrant, до post-filter)
        score_threshold: Порог схожести
        candidate_limit: Сколько кандидатов брать из Qdrant для post-filter
    
    Returns:
        Список словарей с point_id, postgres_id, score
    """
    try:
        client = get_qdrant_client()
        
        must_conditions: list = [
            FieldCondition(key="type", match=MatchValue(value="graph_nodes")),
        ]
        
        if actor_id:
            custom_filter = Filter(
                must=must_conditions,
                min_should=MinShould(
                    conditions=[
                        FieldCondition(key="actor_id", match=MatchValue(value=actor_id)),
                        IsEmptyCondition(is_empty=PayloadField(key="actor_id"))
                    ],
                    min_count=1
                )
            )
        else:
            must_conditions.append(IsEmptyCondition(is_empty=PayloadField(key="actor_id")))
            custom_filter = Filter(must=must_conditions)
        
        response = client.query_points(
            collection_name=_qdrant_collection,
            query=vector,
            query_filter=custom_filter,
            limit=candidate_limit,
            score_threshold=score_threshold,
            with_payload=True,
            with_vectors=False
        )
        
        hits = []
        raw_points = response.points if hasattr(response, 'points') else response
        for raw_hit in raw_points:
            if isinstance(raw_hit, ScoredPoint):
                point_id = raw_hit.id
                score = raw_hit.score
                payload = raw_hit.payload or {}
            elif isinstance(raw_hit, (tuple, list)):
                point_id = raw_hit[0] if len(raw_hit) > 0 else None
                score = raw_hit[1] if len(raw_hit) > 1 else 0.0
                payload = raw_hit[2] if len(raw_hit) > 2 and isinstance(raw_hit[2], dict) else {}
            else:
                # Явный cast для Pylance: qdrant-client иногда возвращает Any
                sp = cast(ScoredPoint, raw_hit)
                point_id = sp.id
                score = sp.score
                payload = sp.payload or {}
            hits.append({
                "point_id": point_id,
                "postgres_id": payload.get("postgres_id"),
                "score": score
            })
        logger.debug("Qdrant search: found=%d candidates (pre-filter)", len(hits))
        return hits[:limit]
    except Exception as e:
        logger.error("❌ Qdrant search failed: %s", str(e), exc_info=True)
        return []

def delete_graph_node_point(qdrant_point_id: str) -> bool:
    """Удаляет точку узла из Qdrant."""
    try:
        client = get_qdrant_client()
        client.delete(collection_name=_qdrant_collection, points_selector=[qdrant_point_id])
        logger.debug("Qdrant point deleted: %s", qdrant_point_id[:8])
        return True
    except Exception as e:
        logger.error("❌ Qdrant delete failed %s: %s", qdrant_point_id[:8], str(e), exc_info=True)
        return False