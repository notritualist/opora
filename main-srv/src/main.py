"""
/main-srv/src/main.py

Главный модуль запуска агента.
Последовательность инициализации:
1. Настройка логирования
2. Проверка схемы Postgres
3. Проверка коллекций Qdrant
4. Очистка зависших сессий и диалогов
5. Запуск оркестратора
6. Инициализация менеджера жизненного цикла (LifecycleManager)
7. Запуск консольного интерфейса с интеграцией жизненного цикла
"""

__version__ = "1.1.0"
__description__ = "Main launch module of agent"

import sys
import logging
from pathlib import Path
from version import __version__ as agent_version # Версия проекта
from db_manager.db_manager import load_postgres_config, ensure_postgres_schema_ready, load_qdrant_config, ensure_qdrant_collections
from interfaces.console_interface import run_console_interface
from orchestrator.orchestrator import start_orchestrator
from session_services.session_manager import SessionManager
from services.lifecycle_manager import LifecycleManager


def setup_logging():
    """
    Настройка глобального логирования с фильтрацией.
    Создаёт логгер с двумя handlers:
    - Файловый: DEBUG и выше в logs/agent_full.log
    - Консольный: WARNING и выше в stdout
    """
    project_root = Path(__file__).parent.parent
    log_dir = project_root / "logs"
    log_dir.mkdir(exist_ok=True)
    
    # Создаем логгер
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    # Подавляем DEBUG-сообщения от HTTP-библиотек
    # При желании можно оставить DEBUG для httpx, если нужны полные сводки запросов:
    # logging.getLogger("httpx").setLevel(logging.DEBUG)
    
    logging.getLogger("httpcore").setLevel(logging.INFO)
    logging.getLogger("httpx").setLevel(logging.INFO)

    # Форматтер
    formatter = logging.Formatter('[%(asctime)s] %(levelname)-8s | %(name)-15s | %(message)s')
    
    # 1. Файловый handler - пишет ВСЁ (DEBUG и выше)
    file_handler = logging.FileHandler(log_dir / "agent_full.log", encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    # 2. Консольный handler - вывод в консоль только WARNING и выше
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(formatter)
    
    # Удаляем старые handlers
    logger.handlers.clear()
    
    # Добавляем новые
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logging.getLogger(__name__)

def main():
    """
    Точка входа проекта.
    Последовательность:
    1. Логгирование старта агента
    2. Загрузка и проверка схемы БД Postgres
    3. Загрузка и проверка коллекции Qdrant
    4. Очистка зависших после рестарта сессий пользователей
    5. Запуск цикла оркестратора
    6. Инициализация LifecycleManager
    7. Запуск консольного интерфейса с передачей lifecycle_mgr
    """
    # Инициализация логгирования
    success = False
    logger = setup_logging()

    try:
        # 1. Пишем старт сессии в лог
        logger.info(f"Launching agent version {agent_version}")

        # 2. Убеждаемся, что схема БД Postgres актуальна (миграции применены)
        postgres_config = load_postgres_config()
        if not ensure_postgres_schema_ready(postgres_config):
            logger.critical(f"Postgres database schema initialization failed")
            return 1
        success = True

        # 3. Убеждаемся, что коллекция Qdrant существует и доступна
        qdrant_config = load_qdrant_config()
        if not ensure_qdrant_collections(qdrant_config):
            logger.critical(f"Failed to initialize Qdrant vector database")
            return 1
        
        success = True 

        # 4. Очистка зависших до рестарта сессий
        SessionManager.close_dangling_sessions(postgres_config)
                
        # 5. Запуск цикла оркестратора
        start_orchestrator()

        # 6. Инициализация LifecycleManager
        lifecycle_mgr = LifecycleManager(postgres_config)

        # 7. Запуск консольного интерфейса с передачей конфига БД и версии агента
        run_console_interface(postgres_config, agent_version, lifecycle_mgr)

                
    except Exception as e:
        logger.critical(f"Critical startup error {e}", exc_info=True)
        return 1
    
    finally:
        if success:
            logger.info("Session completed successfully")
        else:
            logger.critical("Session terminated with error")
            return 1
        
    return 0

if __name__ == "__main__":
    exit(main())