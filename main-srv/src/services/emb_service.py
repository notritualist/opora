"""
main-srv/src/services/emb_service.py

Сервис управления векторизацией узлов графа знаний и взаимодействия с emb-srv.

Основные возможности:
1. Клиент emb-srv:
   - Загрузка конфигурации из emb_srv_config.yaml (с fallback на localhost).
   - HTTP-запросы к эндпоинту /embed с валидацией размерности вектора (EMBEDDING_DIMENSION).
   - Обработка таймаутов и сетевых ошибок.
2. Подготовка данных:
   - Извлечение текста для векторизации (приоритет: description → display_name + entity_slug).
   - Проверка длины текста в токенах (лимит EMB_SRV_MAX_CONTEXT).
3. Обработчик задачи (vectorize_graph_node):
   - Пошаговое выполнение: валидация → вызов emb-srv → сохранение метрик → upsert в Qdrant.
   - Обновление ссылок emb_metric_id и qdrant_point_id в memory.graph_nodes.
4. Создание задач оркестратора (create_graph_node_vectorize_task).
"""

__version__ = "1.4.0"
__description__ = "Сервис векторизации узлов графа знаний (memory.graph_nodes)"

import logging
import requests
import time
import yaml
from pathlib import Path
from typing import Optional, Dict, Any, Tuple
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor, Json

# Локальные импорты
from db_manager.db_manager import load_postgres_config
from services.service_metrics import (
    mark_task_running,
    complete_task_success,
    complete_task_error,
    create_orchestrator_step,
    complete_step_success,
    complete_step_error,
    save_emb_metrics
)
from services.tokens_counter import count_tokens_qwen
from version import __version__ as agent_version
from db_manager.qdrant_manager import upsert_graph_node_vector

logger = logging.getLogger(__name__)


# =============================================================================
# ЗАГРУЗКА КОНФИГУРАЦИИ EMB-SRV
# =============================================================================

def _load_emb_srv_config(config_path: str | None = None) -> Dict[str, Any]:
    """Загружает конфигурацию сервера эмбеддингов из YAML файла."""
    if config_path is None:
        config_file_path = Path(__file__).parent.parent.parent / "configs" / "emb_srv_config.yaml"
    else:
        config_file_path = Path(config_path)

    logger.debug("Загрузка конфигурации emb-srv из: %s", config_file_path)

    if not config_file_path.exists():
        error_msg = f"Файл конфигурации emb-srv не найден: {config_file_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    try:
        with config_file_path.open('r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)

        logger.info(
            "Конфигурация emb-srv загружена: %s:%s",
            config_data['server']['host'], config_data['server']['port']
        )
        return config_data

    except Exception as e:
        logger.error("Ошибка загрузки конфигурации emb-srv: %s", e)
        raise


try:
    _EMB_SRV_CONFIG = _load_emb_srv_config()
    EMB_SRV_HOST: str = _EMB_SRV_CONFIG["server"]["host"]
    EMB_SRV_PORT: int = _EMB_SRV_CONFIG["server"]["port"]
    EMB_SRV_URL: str = f"http://{EMB_SRV_HOST}:{EMB_SRV_PORT}/embed"
    logger.info("emb-srv конфигурация загружена: %s:%s", EMB_SRV_HOST, EMB_SRV_PORT)
except Exception as e:
    logger.error("❌ Ошибка загрузки конфигурации emb-srv: %s", e)
    EMB_SRV_HOST = "localhost"
    EMB_SRV_PORT = 8000
    EMB_SRV_URL = f"http://{EMB_SRV_HOST}:{EMB_SRV_PORT}/embed"


# =============================================================================
# КОНСТАНТЫ
# =============================================================================

GRAPH_NODE_VECTORIZE_PRIORITY: float = 0.2    # Приоритет задачи векторизации узла графа
EMB_SRV_TIMEOUT: int = 30                      # Таймаут HTTP-запроса к emb-srv (сек)
EMB_SRV_MAX_CONTEXT: int = 16384               # Максимум токенов для emb-srv
EMBEDDING_DIMENSION: int = 2560                # Ожидаемая размерность вектора


# =============================================================================
# КЛИЕНТ EMB-SRV
# =============================================================================

def call_emb_server(text: str) -> Tuple[Optional[list], Dict[str, Any]]:
    """
    Выполняет HTTP-запрос к emb-srv для генерации эмбеддинга.

    Args:
        text: Текст для векторизации

    Returns:
        Tuple[vector | None, response_metadata]:
            - vector: список float или None при ошибке
            - response_metadata: dict с model, params (duration_sec, error)
    """
    start_time = time.time()

    try:
        payload = {"text": text}
        logger.debug(
            "Отправка запроса на векторизацию (%d симв., %d токенов)",
            len(text), count_tokens_qwen(text)
        )

        response = requests.post(EMB_SRV_URL, json=payload, timeout=EMB_SRV_TIMEOUT)
        duration = time.time() - start_time

        if response.status_code != 200:
            error_msg = f"emb-srv вернул статус {response.status_code}: {response.text}"
            logger.error(error_msg)
            return None, {
                "model": {},
                "params": {
                    "received_at": None,
                    "sent_at": None,
                    "duration_sec": duration,
                    "error": error_msg
                }
            }

        result = response.json()
        vector = result.get("vector")
        model_info = result.get("model", {})
        params_info = result.get("params", {})

        if vector is not None:
            actual_dim = len(vector)
            if actual_dim != EMBEDDING_DIMENSION:
                error_msg = f"Неверная размерность вектора: {actual_dim} (ожидалось {EMBEDDING_DIMENSION})"
                logger.error(error_msg)
                return None, {
                    "model": model_info,
                    "params": {
                        **params_info,
                        "duration_sec": duration,
                        "error": error_msg
                    }
                }

            logger.debug("Вектор получен: %d элементов, время: %.3f сек", actual_dim, duration)

        return vector, {
            "model": model_info,
            "params": {
                **params_info,
                "duration_sec": duration
            }
        }

    except requests.exceptions.Timeout:
        error_msg = f"Таймаут запроса к emb-srv (>{EMB_SRV_TIMEOUT} сек)"
        logger.error(error_msg)
        return None, {
            "model": {},
            "params": {
                "received_at": None,
                "sent_at": None,
                "duration_sec": time.time() - start_time,
                "error": error_msg
            }
        }

    except requests.exceptions.RequestException as e:
        error_msg = f"Ошибка подключения к emb-srv ({EMB_SRV_HOST}:{EMB_SRV_PORT}): {str(e)}"
        logger.error(error_msg, exc_info=True)
        return None, {
            "model": {},
            "params": {
                "received_at": None,
                "sent_at": None,
                "duration_sec": time.time() - start_time,
                "error": error_msg
            }
        }

    except Exception as e:
        error_msg = f"Неожиданная ошибка при векторизации: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return None, {
            "model": {},
            "params": {
                "received_at": None,
                "sent_at": None,
                "duration_sec": time.time() - start_time,
                "error": error_msg
            }
        }


# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================

def get_graph_node_text_for_embedding(
    node_id: str,
    db_config: Dict[str, Any]
) -> Optional[str]:
    """
    Извлекает текст узла графа для векторизации.

    Векторизуется поле description (полное описание сущности).
    Если description пустой — используется display_name + entity_slug.

    Args:
        node_id: UUID узла в memory.graph_nodes
        db_config: параметры подключения к PostgreSQL

    Returns:
        str | None: текст для векторизации или None, если узел не найден
    """
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT description, display_name, entity_slug
                    FROM memory.graph_nodes
                    WHERE id = %s AND is_active = TRUE
                """, (node_id,))

                row = cur.fetchone()
                if not row:
                    logger.error("Узел графа %s не найден в БД", node_id[:8])
                    return None

                # Приоритет: description → display_name + entity_slug
                text = row["description"]
                if not text and row["display_name"]:
                    text = f"{row['display_name']}: {row['entity_slug']}"
                elif not text:
                    text = row["entity_slug"]

                if not text:
                    logger.error("Узел графа %s не содержит текста для векторизации", node_id[:8])
                    return None

                logger.debug("Текст узла графа получен: %d симв.", len(text))
                return text

    except Exception as e:
        logger.error("Ошибка чтения узла графа %s: %s", node_id[:8], str(e), exc_info=True)
        return None


def get_graph_node_metadata(
    node_id: str,
    db_config: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT actor_id, entity_slug, domain_code, domain_id,
                           display_name, confidence, usage_count, updated_at, topic_id
                    FROM memory.graph_nodes
                    WHERE id = %s AND is_active = TRUE
                """, (node_id,))

                row = cur.fetchone()
                if not row:
                    return None

                return {
                    "actor_id": str(row["actor_id"]) if row["actor_id"] else None,
                    "entity_slug": row["entity_slug"],
                    "domain_code": row["domain_code"],
                    "domain_id": str(row["domain_id"]) if row.get("domain_id") else None,
                    "topic_id": str(row["topic_id"]) if row.get("topic_id") else None,
                    "display_name": row["display_name"],
                    "confidence": float(row["confidence"]),
                    "usage_count": int(row["usage_count"]),
                    "updated_at": row["updated_at"]
                }
    except Exception as e:
        logger.error("Ошибка чтения метаданных узла графа %s: %s", node_id[:8], str(e), exc_info=True)
        return None


def validate_text_length(text: str, max_tokens: int = EMB_SRV_MAX_CONTEXT) -> bool:
    """
    Проверяет, что текст не превышает лимит токенов сервера эмбеддингов.

    Args:
        text: Текст для проверки
        max_tokens: Максимальное количество токенов сервера

    Returns:
        bool: True если текст в пределах лимита
    """
    token_count = count_tokens_qwen(text)

    if token_count > max_tokens:
        logger.warning(
            "⚠️ Текст превышает лимит токенов эмбеддинг-сервера: %d > %d",
            token_count, max_tokens
        )
        return False

    logger.debug("Текст в пределах лимита: %d токенов (макс: %d)", token_count, max_tokens)
    return True


# =============================================================================
# ФУНКЦИИ СОЗДАНИЯ ЗАДАЧ ОРКЕСТРАТОРА
# =============================================================================

def create_graph_node_vectorize_task(
    node_id: str,
    priority: float = GRAPH_NODE_VECTORIZE_PRIORITY
) -> Optional[str]:
    """
    Создаёт задачу оркестратора для векторизации узла графа знаний.

    Вызывается из graph_service после создания/обновления узла в memory.graph_nodes.

    Args:
        node_id: UUID узла в memory.graph_nodes
        priority: Приоритет задачи (по умолчанию 0.2 — фоновая)

    Returns:
        str | None: UUID созданной задачи или None при ошибке
    """
    db_config: Dict[str, Any] = load_postgres_config()

    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id FROM orchestrator.task_types
                    WHERE type_name = 'graph_node_vectorize'
                """)
                task_type = cur.fetchone()

                if not task_type:
                    logger.error("Тип задачи 'graph_node_vectorize' не найден в БД")
                    return None

                input_data: Dict[str, str] = {
                    "node_id": node_id
                }

                cur.execute("""
                    INSERT INTO orchestrator.orchestrator_tasks (
                        task_type_id,
                        input_data,
                        priority,
                        status,
                        agent_version,
                        created_at
                    ) VALUES (
                        %s,
                        %s,
                        %s,
                        'pending'::task_status,
                        %s,
                        NOW()
                    )
                    RETURNING id
                """, (
                    task_type["id"],
                    Json(input_data),
                    priority,
                    agent_version
                ))

                conn.commit()
                task_id: str = str(cur.fetchone()["id"])
                logger.info(
                    "Задача векторизации узла графа создана: %s (node=%s, приоритет=%s)",
                    task_id[:8], node_id[:8], priority
                )
                return task_id

    except Exception as e:
        logger.error("❌ Ошибка создания задачи векторизации узла графа: %s", str(e), exc_info=True)
        return None


# =============================================================================
# ОСНОВНАЯ ФУНКЦИЯ ОБРАБОТКИ ЗАДАЧИ
# =============================================================================

def vectorize_graph_node(task_id: str, input_data: Dict[str, Any]) -> None:
    """
    Обработчик задачи векторизации узла графа знаний.

    Последовательность:
    1. Пометить задачу как running
    2. Извлечь текст узла из memory.graph_nodes (description)
    3. Проверить длину текста
    4. Создать шаг оркестратора (graph_node_vectorize)
    5. Вызвать emb-srv
    6. Сохранить метрики эмбеддинга в metrics.emb_internal
    7. Обновить emb_metric_id в memory.graph_nodes
    8. Сохранить вектор в Qdrant через upsert_graph_node_vector
    9. Обновить qdrant_point_id в memory.graph_nodes
    10. Завершить шаг и задачу

    Args:
        task_id: UUID задачи оркестратора
        input_data: {"node_id": "uuid"}
    """
    db_config: Dict[str, Any] = load_postgres_config()

    mark_task_running(task_id)
    logger.info("Задача векторизации узла графа %s помечена как running", task_id[:8])

    # === 1. Валидация входных данных ===
    node_id: Optional[str] = input_data.get("node_id")
    if not node_id:
        error = "Отсутствует node_id в input_data"
        logger.error(error)
        complete_task_error(task_id, error_module="emb_service", error_message=error)
        return

    # === 2. Получение текста узла ===
    text = get_graph_node_text_for_embedding(node_id, db_config)
    if not text:
        error = f"Не удалось получить текст узла графа {node_id} для векторизации"
        logger.error(error)
        complete_task_error(task_id, error_module="emb_service", error_message=error)
        return

    # === 3. Проверка длины ===
    if not validate_text_length(text, EMB_SRV_MAX_CONTEXT):
        error = f"Текст узла графа {node_id} превышает лимит токенов {EMB_SRV_MAX_CONTEXT}"
        logger.error(error)
        complete_task_error(task_id, error_module="emb_service", error_message=error)
        return

    # === 4. Создание шага оркестратора ===
    step_input: Dict[str, Any] = {
        "node_id": node_id,
        "text_length": len(text),
        "token_count": count_tokens_qwen(text)
    }

    step_id: str = create_orchestrator_step(
        task_id=task_id,
        step_number=1,
        step_type_name="graph_node_vectorize",
        input_data=step_input
    )
    logger.info("Шаг векторизации узла графа %s создан", step_id[:8])

    # === 5. Вызов emb-srv ===
    vector, emb_response = call_emb_server(text)
    error_msg = emb_response["params"].get("error")

    if error_msg or vector is None:
        logger.error("❌ Ошибка векторизации: %s", error_msg or "Неизвестная ошибка")
        complete_step_error(
            step_id,
            error_module="emb_service",
            error_message=error_msg or "Неизвестная ошибка векторизации"
        )
        complete_task_error(
            task_id,
            error_module="emb_service",
            error_message=error_msg or "Неизвестная ошибка векторизации"
        )
        return

    # === 6. Сохранение метрик эмбеддинга ===
    model_info = emb_response["model"]
    params_info = emb_response["params"]

    emb_metric_id: str = save_emb_metrics(
        orchestrator_step_id=step_id,
        host=f"{EMB_SRV_HOST}:{EMB_SRV_PORT}",
        model=model_info.get("name", "unknown"),
        param={
            "embedding_dim": model_info.get("embedding_dim"),
            "n_ctx": model_info.get("n_ctx")
        },
        vector_dimension=model_info.get("embedding_dim", EMBEDDING_DIMENSION),
        prompt_tokens=count_tokens_qwen(text),
        received_at=datetime.fromisoformat(params_info["received_at"]) if params_info.get("received_at") else None,
        sent_at=datetime.fromisoformat(params_info["sent_at"]) if params_info.get("sent_at") else None,
        full_time=params_info.get("duration_sec", 0.0),
        error_status=False,
        agent_version=agent_version 
    )
    logger.debug("Метрики эмбеддинга сохранены: %s", emb_metric_id[:8])

    # === 7. Обновление emb_metric_id в memory.graph_nodes ===
    try:
        with psycopg2.connect(**db_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE memory.graph_nodes
                    SET emb_metric_id = %s
                    WHERE id = %s
                """, (emb_metric_id, node_id))
                conn.commit()
                logger.info(
                    "Векторизация узла завершена: node=%s, метрика=%s",
                    node_id[:8], emb_metric_id[:8]
                )

    except Exception as e:
        error = f"Ошибка обновления узла графа {node_id}: {str(e)}"
        logger.error(error, exc_info=True)
        complete_step_error(step_id, error_module="emb_service", error_message=error)
        complete_task_error(task_id, error_module="emb_service", error_message=error)
        return

    # === 8. Сохранение вектора в Qdrant ===
    qdrant_point_id: Optional[str] = None
    if vector is not None:
        metadata = get_graph_node_metadata(node_id, db_config)

        if metadata:
            try:
                qdrant_point_id = upsert_graph_node_vector(
                    vector=vector,
                    postgres_node_id=node_id,
                    actor_id=metadata.get("actor_id")
                )
            except Exception as e:
                logger.error("❌ Ошибка upsert в Qdrant для узла %s: %s", node_id[:8], str(e), exc_info=True)

            if qdrant_point_id:
                # === 9. Обновление qdrant_point_id в memory.graph_nodes ===
                try:
                    with psycopg2.connect(**db_config) as conn:
                        with conn.cursor() as cur:
                            cur.execute("""
                                UPDATE memory.graph_nodes
                                SET qdrant_point_id = %s
                                WHERE id = %s
                            """, (qdrant_point_id, node_id))
                            conn.commit()
                            logger.info(
                                "Qdrant point привязан к узлу графа: %s → %s",
                                node_id[:8], qdrant_point_id[:8]
                            )
                except Exception as e:
                    logger.error(
                        "Ошибка обновления qdrant_point_id для узла %s: %s",
                        node_id[:8], str(e), exc_info=True
                    )

    # === 10. Завершение ===
    step_output: Dict[str, Any] = {
        "node_id": node_id,
        "emb_metric_id": emb_metric_id,
        "qdrant_point_id": qdrant_point_id,
        "vector_dimension": EMBEDDING_DIMENSION,
        "text_length": len(text),
        "token_count": count_tokens_qwen(text),
        "duration_sec": params_info.get("duration_sec", 0.0)
    }

    complete_step_success(step_id, output_data=step_output)
    complete_task_success(task_id, output_data=step_output)
    logger.info("Задача векторизации узла графа %s завершена успешно", task_id[:8])