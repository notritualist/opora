"""
main-srv/src/db_manager/qdrant_schema.py

Схема плейлоада коллекции Qdrant opora_db для псевдографа знаний.
ТИП ДАННЫХ В КОЛЛЕКЦИИ:
┌──────────────┬──────────────────────────────────────────────────┐
│ Тип          │ Плейлоад структура                               │
├──────────────┼──────────────────────────────────────────────────┤
│ graph_nodes  │ {                                                │
│              │    "type": "graph_nodes",                        │
│              │    "postgres_id": "uuid",                        │
│              │    "actor_id": "uuid|null",                      │
│              │    "topic_id": "uuid",                           │
│              │    "domain_id": "uuid"                           │
│              │ }                                                │
└──────────────┴──────────────────────────────────────────────────┘
Правила:
- Текстовые коды доменов/тем в Qdrant НЕ хранятся.
- Фильтрация и маршрутизация идут строго по domain_id и topic_id (UUID).
- Все семантические изменения описаний требуют явного upsert.
"""

__version__ = "1.1.0"
__description__ = "Схема плейлоада коллекции Qdrant opora_db для графа знаний"

from typing import Optional, Dict, Any


def build_graph_node_payload(
    postgres_node_id: str,
    actor_id: Optional[str] = None,
    topic_id: Optional[str] = None,
    domain_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Формирует плейлоад для узла графа знаний.
    Args:
        postgres_node_id: UUID узла в PostgreSQL (memory.graph_nodes.id)
        actor_id: UUID владельца (None для глобальных узлов)
        topic_id: UUID темы (обязателен для структурной фильтрации)
        domain_id: UUID домена (обязателен для маршрутизации и изоляции)
    Returns:
        Словарь с плейлоадом для Qdrant
    """
    payload: Dict[str, Any] = {
        "type": "graph_nodes",
        "postgres_id": postgres_node_id
    }
    if actor_id:
        payload["actor_id"] = actor_id
    if topic_id:
        payload["topic_id"] = topic_id
    if domain_id:
        payload["domain_id"] = domain_id
    return payload