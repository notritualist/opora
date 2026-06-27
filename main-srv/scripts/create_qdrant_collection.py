"""
Создание коллекции opora_db в Qdrant для векторов размерностью 2560
С использованием оптимизаций: квантование INT8, хранение полезной нагрузки на диске (on_disk_payload) и настройки для данных высокой размерности.
"""

from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance, 
    VectorParams, 
    HnswConfigDiff,
    OptimizersConfigDiff,
    ScalarQuantization,
    ScalarQuantizationConfig,
    ScalarType,
    WalConfigDiff
)

def create_opora_db_collection():
    """Создание коллекции для памяти агента с оптимизацией под 2560d векторы"""
    
    # Подключение к локальному Qdrant
    client = QdrantClient("localhost", port=6335)
    
    # Проверка существования коллекции
    collection_name = "opora_db"
    
    # Если коллекция существует, спросим что делать
    if client.collection_exists(collection_name):
        print(f"Collection '{collection_name}' already exists")
        response = input("Delete and recreate? (y/n):")
        if response.lower() == 'y':
            client.delete_collection(collection_name)
            print(f"Collection deleted")
        else:
            print("Operation cancelled")
            return
    
    # Оптимальная конфигурация для 2560d векторов
    client.create_collection(
        collection_name=collection_name,
        vectors_config=VectorParams(
            size=2560,                          # Размерность векторов
            distance=Distance.COSINE,            # COSINE лучше для high-dimensional
            on_disk=False                        # Вектора в памяти (с учетом квантования)
        ),
        hnsw_config=HnswConfigDiff(
            m=16,                                # Для 2560d норм, можно и 32 если данных много
            ef_construct=100,                     # Качество построения индекса
            full_scan_threshold=10000,            # Полный скан до построения индекса
            max_indexing_threads=0,                # Auto
            on_disk=False                           # HNSW граф в памяти для скорости
        ),
        optimizers_config=OptimizersConfigDiff(
            deleted_threshold=0.2,                  # Порог удаления для оптимизации
            vacuum_min_vector_number=1000,           # Минимум векторов для оптимизации
            default_segment_number=2,                 # Начальное количество сегментов
            memmap_threshold=50000,                   # Увеличено! Только после 50к на диск
            indexing_threshold=10000,                  # HNSW после 10к точек
            flush_interval_sec=5,                       # Сброс на диск каждые 5 сек
            max_optimization_threads=2,                  # Ограничим потоки оптимизации
        ),
        wal_config=WalConfigDiff(
            wal_capacity_mb=1024,                      # Размер WAL
            wal_segments_ahead=0                         # Количество сегментов вперед
        ),
        quantization_config=ScalarQuantization(
            scalar=ScalarQuantizationConfig(
                type=ScalarType.INT8,                    # Квантование в int8
                quantile=0.99,                             # 99% квантиль для точности
                always_ram=True                             # Всегда держать сжатые вектора в RAM
            )
        ),
        on_disk_payload=True,                            # Payload на диск, вектора в RAM
        replication_factor=1,                              # Без репликации для теста
        shard_number=1                                      # Один шард
    )
    
    print(f"Collection '{collection_name}' successfully created")
    print(f"\nCONFIGURATION:")
    print(f"   Vectors: {2560}d, COSINE")
    print(f"   HNSW: m={16}, ef_construct={100}")
    print(f"   Payload: on disk (RAM savings)")
    print(f"   Memmap threshold: 50000 vectors")
    print(f"   Indexing: after 10000 vectors")
    
    # Детальная информация о квантовании
    print(f"\nQUANTIZATION (memory savings):")
    print(f"   Type: INT8 (4x compression)")
    print(f"   Original: 2560 * 4 bytes = 10 KB/vector")
    print(f"   Compressed: 2560 * 1 byte = 2.5 KB/vector")
    print(f"   Savings: 75% RAM")
    print(f"   always_ram: Yes (compressed vectors always in RAM)")
    
    # Проверка состояния коллекции
    info = client.get_collection(collection_name)
    print(f"\nFINAL CONFIGURATION FROM QDRANT:")
    print(f"   Status: {info.status}")
    print(f"   Number of segments: {info.segments_count}")
    print(f"   Quantization: {info.config.quantization_config is not None}")
    
    # Советы по использованию
    print(f"\nUSAGE TIPS:")
    print("   When searching, add oversampling for rescoring:")
    print("      search(..., limit=100, oversampling=10.0) → returns top-10 with reranking")
    print("   Monitor metrics: indexed_vectors_count should increase")
    print("   RAM usage: N * 2.5 KB for compressed vectors")

def recreate_with_custom_settings():
    """Функция для пересоздания с кастомными настройками"""
    client = QdrantClient("localhost", port=6335)
    collection_name = "opora_db"
    
    # Удаляем если есть
    if client.collection_exists(collection_name):
        client.delete_collection(collection_name)
        print("Old collection deleted")
    
    # Здесь можно вызвать create с другими параметрами
    create_opora_db_collection()

if __name__ == "__main__":
    create_opora_db_collection()
