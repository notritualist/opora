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
version = "1.1.0"
description = "Менеджер Qdrant для псевдографа с фильтрацией по domain_id/topic_id и upsert-логикой"

import logging
import uuid
from typing import List, Dict, Any, Optional
from qdrant_client import QdrantClient
from qdrant_client.models import (
    PointStruct, Filter, FieldCondition, MatchValue,
    IsEmptyCondition, PayloadField, MinShould
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
    domain_id: str,
    topic_id: str,
    actor_id: Optional[str] = None,
    qdrant_point_id: Optional[str] = None
) -> str:
    """
    Сохраняет или обновляет вектор узла графа в Qdrant.
    Если qdrant_point_id передан → обновляет существующую точку.
    Если None → создаёт новую.
    Args:
        vector: Вектор эмбеддинга
        postgres_node_id: UUID узла в PostgreSQL
        domain_id: UUID домена (обязателен)
        topic_id: UUID темы (обязателен)
        actor_id: UUID владельца
        qdrant_point_id: Существующий ID точки в Qdrant (для обновлений)
    Returns:
        ID точки в Qdrant
    """
    if not domain_id or not topic_id:
        raise ValueError("domain_id и topic_id обязательны для upsert_graph_node_vector")
        
    try:
        client = get_qdrant_client()
        point_id = qdrant_point_id or str(uuid.uuid4())
        payload = build_graph_node_payload(
            postgres_node_id=postgres_node_id,
            actor_id=actor_id,
            topic_id=topic_id,
            domain_id=domain_id
        )
        client.upsert(
            collection_name=_qdrant_collection,
            points=[PointStruct(id=point_id, vector=vector, payload=payload)]
        )
        logger.debug("Qdrant upsert: node=%s → point=%s (domain=%s, topic=%s)", postgres_node_id[:8], point_id[:8], domain_id[:8], topic_id[:8])
        return point_id
    except Exception as e:
        logger.error("❌ Qdrant upsert failed for node %s: %s", postgres_node_id[:8], str(e), exc_info=True)
        raise

def search_similar_graph_nodes(
    vector: List[float],
    domain_id: str,
    topic_id: str,
    actor_id: Optional[str] = None,
    limit: int = 5,
    score_threshold: float = 0.72
) -> List[Dict[str, Any]]:
    """
    Поиск похожих узлов графа. СТРОГО внутри одного домена и темы.
    Args:
        vector: Вектор запроса
        domain_id: UUID домена (обязательный фильтр)
        topic_id: UUID темы (обязательный фильтр)
        actor_id: UUID владельца (опционально, ищет личные + глобальные)
        limit: Максимум результатов
        score_threshold: Порог схожести
    Returns:
        Список словарей с point_id, postgres_id, score
    """
    if not domain_id or not topic_id:
        raise ValueError("domain_id и topic_id обязательны для search_similar_graph_nodes")
        
    try:
        client = get_qdrant_client()
        # Явная аннотация list снимает ошибку инвариантности Pylance при смешивании Condition-подтипов
        must_conditions: list = [
            FieldCondition(key="type", match=MatchValue(value="graph_nodes")),
            FieldCondition(key="domain_id", match=MatchValue(value=domain_id)),
            FieldCondition(key="topic_id", match=MatchValue(value=topic_id))
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
            limit=limit,
            score_threshold=score_threshold,
            with_payload=True,
            with_vectors=False
        )
        hits = []
        for hit in response.points:
            payload = hit.payload or {}
            hits.append({
                "point_id": hit.id,
                "postgres_id": payload.get("postgres_id"),
                "score": hit.score
            })
        logger.debug("Qdrant search: domain=%s, topic=%s, found=%d", domain_id[:8], topic_id[:8], len(hits))
        return hits
    except Exception as e:
        logger.error("❌ Qdrant search failed (domain=%s, topic=%s): %s", domain_id[:8], topic_id[:8], str(e), exc_info=True)
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