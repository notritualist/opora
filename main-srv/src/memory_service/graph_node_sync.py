"""
main-srv/src/memory_service/graph_node_sync.py

Сервис post-commit синхронизации узлов графа с векторной БД Qdrant.
Вызывается композерами (merge, summarize) ПОСЛЕ успешного COMMIT транзакции в PostgreSQL.

Логика работы:
1. Чтение актуальных данных узла (description, actor_id, topic_id, domain_id, qdrant_point_id) из БД.
2. Векторизация description через внешний emb-srv.
3. Upsert вектора в Qdrant (обновление существующей точки или создание новой).
4. Обновление qdrant_point_id в memory.graph_nodes (если он изменился).

Архитектурные принципы:
- Не блокирует основные транзакции БД (вызывается после commit).
- Fail-safe: при ошибке векторизации или Qdrant логирует WARNING. 
  Узел остается в БД, а следующая успешная синхронизация перезапишет точку.
"""

version = "1.2.0"
description = "Post-commit sync service for graph nodes → Qdrant"

import logging
import psycopg2
from typing import Optional, Dict, Any
from psycopg2.extras import RealDictCursor

from db_manager.db_manager import load_postgres_config
from db_manager.qdrant_manager import upsert_graph_node_vector
from services.emb_service import call_emb_server

logger = logging.getLogger(__name__)


def sync_node_to_qdrant(node_id: str, db_config: Optional[Dict[str, Any]] = None) -> bool:
    """
    Синхронизирует узел графа с Qdrant после изменения description.
    Читает description, actor_id, topic_id, domain_id из БД.
    Args:
        node_id: UUID узла в memory.graph_nodes
        db_config: параметры подключения (если None, загружаются автоматически)
    Returns:
        True при успехе, False при ошибке
    """
    if db_config is None:
        db_config = load_postgres_config()
        
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Запрашиваем domain_id напрямую из таблицы узлов
                cur.execute("""
                    SELECT description, actor_id, topic_id, domain_id, qdrant_point_id
                    FROM memory.graph_nodes WHERE id = %s AND is_active = TRUE
                """, (node_id,))
                row = cur.fetchone()
                if not row:
                    logger.warning("Sync skipped: node %s not found or inactive", node_id[:8])
                    return False
                    
                desc = row["description"] or ""
                if not desc.strip():
                    logger.warning("Sync skipped: node %s has empty description", node_id[:8])
                    return False

                # Векторизация
                vec, resp = call_emb_server(desc)
                if not vec:
                    logger.warning("Sync failed: emb-srv error for node %s: %s", node_id[:8], resp["params"].get("error"))
                    return False
                    
                # Upsert в Qdrant (тип гарантированно str после guard-проверки)
                qdrant_id = upsert_graph_node_vector(
                    vector=vec,
                    postgres_node_id=node_id,
                    actor_id=str(row["actor_id"]) if row["actor_id"] else None,
                    qdrant_point_id=row["qdrant_point_id"]
                )
                    
                # Обновление ссылки в БД
                if qdrant_id != row["qdrant_point_id"]:
                    cur.execute("UPDATE memory.graph_nodes SET qdrant_point_id=%s WHERE id=%s", (qdrant_id, node_id))
                    conn.commit()
                    
                logger.debug("Node %s synced to Qdrant: point=%s", node_id[:8], qdrant_id[:8])
                return True
                
    except Exception as e:
        logger.error("❌ Sync failed for node %s: %s", node_id[:8], str(e), exc_info=True)
        return False