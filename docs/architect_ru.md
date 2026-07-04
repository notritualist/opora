# Структура проекта

agent/
├── pyproject.toml               # Python проект: зависимости, версия
├── .gitignore                   # Игнорируемые файлы
├── .gitmodules                  # Импортированные модули
│
├── main-srv/                    # Основной сервер
│    ├── .venv/                  # Виртуальное окружение Python
│    ├── configs/
│    │   ├── docker-compose.yaml  # Docker Compose для PostgreSQL и Qdrant
│    │   ├── postgresql.conf      # Конфигурация PostgreSQL
│    │   ├── pg_hba.conf          # Правила аутентентификации PostgreSQL
│    │   ├── qdrant_config.yaml   # Конфигурация Qdrant
│    │   ├── postgres_db_config.yaml  # Конфигурация PostgreSQL (подключение к БД)
│    │   └── model_routing.yaml   # Конфигурация роутинга LLM моделей
│    │
│    ├── llama.cpp/              # Субмодуль llama.cpp (форк)
│    │   ├── CMakeLists.txt
│    │   ├── Makefile
│    │   ├── build/               # Собранные бинарники (игнорируется git)
│    │   └── ...                  # Исходники llama.cpp
│    │
│    ├── logs/                   # Логи работы агента
│    │   └── opora_full.log        # Полный лог (DEBUG+)
│    │
│    ├── models/                 # LLM модели (игнорируется git)
│    │   ├── qwen3_5/
│    │   │   └── Qwen3.5-9B-Q4_K_M.gguf
│    │   └── qwen3_5-tokenizer/
│    │       └── tokenizer.json   # Токенизатор для Qwen3.5
│    │
│    ├── requirements.txt        # Файл зависимостей .venv (main-srv)
│    │
│    ├── scripts/
│    │   ├── start-db.sh          # Скрипт запуска всех БД
│    │   └── start_llama-server.sh # Скрипт запуска llama-server с моделью Qwen3.5
│    │
│    └── src/                     # Исходный код Python
│        ├── __init__.py
│        ├── main.py              # Точка входа (запуск агента)
│        ├── version.py           # Глобальная версия из pyproject.toml
│        │
│        ├── db_manager/          # Управление БД
│        │   ├── __init__.py
│        │   ├── db_manager.py    # Подключение к PostgreSQL (использует postgres_db_config.yaml)
│        │   └── migrations/      # Миграции Postgres
│        │       ├── __init__.py
│        │       ├── pg_migration_manager.py         # Менеджер применения миграций БД
│        │       ├── V001_initial.sql                # Начальная схема (основные таблицы агента)
│        │       └── V002_verification.sql           # Подсистема верификации гипотез
│        │
│        ├── dialog_services/     # Управление жизненным циклом диалогов
│        │   ├── __init__.py
│        │   └── dialogue_manager.py  # Менеджер диалогов (создание/закрытие, таймауты)
│        │
│        ├── interfaces/          # Интерфейсы
│        │   ├── __init__.py
│        │   └── console_interface.py  # Консольный UI
│        │
│        ├── memory_service/      # Подсистема долговременной памяти агента
│        │   ├── __init__.py
│        │   ├── hypothesis_service.py   # Единый модуль работы с гипотезами
│        │   ├── memory_composer.py      # Выполнение задачи извлечения гипотез
│        │   ├── verification_service.py # Управление сессиями верификации гипотез
│        │   └── verification_composer.py # Выполнение задач верификации гипотез
│        │
│        ├── model_service/       # Абстракция доступа к LLM с роутингом
│        │   ├── __init__.py
│        │   ├── model_service.py        # Роутер: выбор провайдера по model_name (использует model_routing.yaml)
│        │   └── providers/              # Реализации провайдеров LLM
│        │       ├── __init__.py
│        │       ├── base.py                 # Абстрактный интерфейс LLMProvider
│        │       ├── local_llama.py          # Провайдер для локального llama-server
│        │       └── external_dashscope.py   # Провайдер для DashScope API (заглушка)
│        │
│        ├── orchestrator/        # Ядро оркестрации задач
│        │   ├── __init__.py
│        │   ├── orchestrator_entry.py   # Точка входа: создание задач из внешних событий
│        │   ├── orchestrator.py         # Фоновый цикл: выбор и диспетчеризация задач
│        │   └── response_composer.py    # Генерация финального ответа через ModelService
│        │
│        │
│        ├── services/            # Вспомогательные сервисы
│        │   ├── __init__.py
│        │   ├── lifecycle_manager.py  # Глобальный менеджер жизненного цикла агента
│        │   ├── service_metrics.py    # Обновление статусов задач/шагов, метрики
│        │   └── tokens_counter.py     # Подсчёт токенов для моделей Qwen
│        │
│        └── session_services/    # Управление сессиями
│            ├── __init__.py
│            └── session_manager.py      # Менеджер сессий и привязки actor_id
│
└── docs/                        # Документация
    └── ...