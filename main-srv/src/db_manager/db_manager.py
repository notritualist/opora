"""/main-srv/src/db_manager/db_manager.py"""

__version__ = "1.1.0"
__description__ = "Main module for database (PostgreSQL) operations"


import yaml
import logging
from pathlib import Path
from .migrations.pg_migration_manager import PGMigrationManager


# Логгер для этого модуля
logger = logging.getLogger(__name__)


# ============================================================================
# === PostgreSQL ===
# ============================================================================

def load_postgres_config(config_path: str | None = None) -> dict:
    """
    Загружает конфигурацию базы данных Postgres из файла
    Аргументы:
        config_path: Путь к файлу конфигурации (необязательный)
    Возвращает:
        dict: Словарь с конфигурацией PostgreSQL
    Вызывает исключения:
        FileNotFoundError: Если файл конфигурации не найден
        Exception: При ошибке разбора YAML
    """
    # Определяем путь к конфигу
    if config_path is None:
        config_file_path = Path(__file__).parent.parent.parent / "configs" / "postgres_db_config.yaml"
   
    else:
        config_file_path = Path(config_path)
    
    logger.debug(f"Loading Postgres DB configuration from: {config_file_path}")
    
     # Проверка существования файла
    if not config_file_path.exists():
        error_msg = f"Postgres configuration file not found: {config_file_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    # Загрузка и парсинг YAML
    try:
        with config_file_path.open('r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
        
        logger.info(f"Postgres DB configuration successfully loaded from: {config_file_path}")
        
        return config_data["database"]
    
    except Exception as e:
        logger.error(f"Error loading Postgres DB configuration: {e}")
        raise


# === PostgreSQL ===
def ensure_postgres_schema_ready(postgres_config: dict | None = None) -> bool:
    """
    Обеспечивает актуальность схемы базы данных PostgreSQL (применены все миграции)
    Аргументы:
        postgres_config: конфигурация PostgreSQL (необязательно, будет загружена автоматически)
    Возвращает:
        bool: True, если схема актуальна; False, если возникли ошибки
    """
    try:
        if postgres_config is None:
            postgres_config = load_postgres_config()
        
        # Путь к миграциям относительно этого файла
        migrations_path = Path(__file__).parent / "migrations"
        migration_manager = PGMigrationManager(str(migrations_path))
        logger.info("Checking Postgres DB schema up-to-date status...")
        
        result = migration_manager.ensure_schema_ready(postgres_config)

        if result:
            logger.info("Postgres DB migration schema check completed successfully")
        else:
            logger.error("Postgres DB migration schema check completed with errors")
        
        return result
    
    except Exception as e:
        logger.error(f"Critical error during DB migration schema check: {e}", exc_info=True)
        return False
    

# ============================================================================
# === Qdrant ===
# ============================================================================

def load_qdrant_config(config_path: str | None = None) -> dict:
    """Загружает конфигурацию векторной БД Qdrant из файла"""
    if config_path is None:
        config_file_path = Path(__file__).parent.parent.parent / "configs" / "qdrant_db_config.yaml"
    else:
        config_file_path = Path(config_path)
    
    logger.debug(f"Loading Qdrant database configuration from: {config_file_path}")
    
    if not config_file_path.exists():
        error_msg = f"Qdrant configuration file not found: {config_file_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    try:
        with config_file_path.open('r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
        logger.info(f"Qdrant database configuration successfully loaded")
        return config_data
    except Exception as e:
        logger.error(f"Error loading Qdrant database configuration: {e}")
        raise


def ensure_qdrant_collections(qdrant_config: dict | None = None) -> bool:
    """Проверяет подключение к Qdrant и создаёт коллекцию если не существует"""
    try:
        if qdrant_config is None:
            qdrant_config = load_qdrant_config()
        
        from qdrant_client import QdrantClient
        from qdrant_client.models import (
            Distance, VectorParams, HnswConfigDiff,
            OptimizersConfigDiff, ScalarQuantization,
            ScalarQuantizationConfig, ScalarType, WalConfigDiff
        )
        
        client = QdrantClient(
            host=qdrant_config.get("host", "localhost"),
            port=qdrant_config.get("port", 6335),
            timeout=30
        )
        
        # Проверка подключения
        client.get_collections()
        logger.info("Successfully connected to the Qdrant database")
        
        collection_name = "opora_db"
        
        # Если коллекция уже есть — выходим
        if client.collection_exists(collection_name):
            logger.info(f"Collection '{collection_name}' already exists, exiting")
            return True
        
        # Создание коллекции (параметры из create_qdrant_collection.py)
        logger.info(f"Creating collection '{collection_name}'...")
        
        client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(
                size=2560,
                distance=Distance.COSINE,
                on_disk=False
            ),
            hnsw_config=HnswConfigDiff(
                m=16,
                ef_construct=100,
                full_scan_threshold=10000,
                max_indexing_threads=0,
                on_disk=False
            ),
            optimizers_config=OptimizersConfigDiff(
                deleted_threshold=0.2,
                vacuum_min_vector_number=1000,
                default_segment_number=2,
                memmap_threshold=50000,
                indexing_threshold=10000,
                flush_interval_sec=5,
                max_optimization_threads=2,
            ),
            wal_config=WalConfigDiff(
                wal_capacity_mb=1024,
                wal_segments_ahead=0
            ),
            quantization_config=ScalarQuantization(
                scalar=ScalarQuantizationConfig(
                    type=ScalarType.INT8,
                    quantile=0.99,
                    always_ram=True
                )
            ),
            on_disk_payload=True,
            replication_factor=1,
            shard_number=1
        )
        
        logger.info(f"Collection '{collection_name}' successfully created")
        return True
        
    except Exception as e:
        logger.error(f"Error while working with Qdrant database: {e}", exc_info=True)
        return False
