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
│              │    "actor_id": "uuid|null"                       │
│              │ }                                                │
└──────────────┴──────────────────────────────────────────────────┘
Правила:
- Текстовые коды доменов/тем в Qdrant НЕ хранятся.
- Фильтрация по domain_id/topic_id/form_code выполняется через PostgreSQL (post-filter),
что устраняет необходимость синхронизации payload при изменении маршрутизирующих атрибутов.
"""

__version__ = "1.2.0"
__description__ = "Схема плейлоада коллекции Qdrant opora_db для графа знаний"

from typing import Optional, Dict, Any


def build_graph_node_payload(
    postgres_node_id: str,
    actor_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Формирует минимальный плейлоад для узла графа знаний.
    
    Содержит только type, postgres_id, actor_id.
    Domain/topic/form — не хранятся, фильтрация идёт через PostgreSQL.
    
    Args:
        postgres_node_id: UUID узла в PostgreSQL (memory.graph_nodes.id)
        actor_id: UUID владельца (None для глобальных узлов)
    
    Returns:
        Словарь с плейлоадом для Qdrant
    """
    payload: Dict[str, Any] = {
        "type": "graph_nodes",
        "postgres_id": postgres_node_id
    }
    if actor_id:
        payload["actor_id"] = actor_id
    return payload