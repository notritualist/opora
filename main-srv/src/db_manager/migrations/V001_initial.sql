-- =============================================
-- Migration: 001_initial.sql
-- Version: V001
-- Description: Создание базовых таблиц PostgreSQL для работы системы.
-- ВАЖНО! Не меняйте порядок создания. В противном случае связи зависимостей для ссылок на таблицы (через REFERENCES) будут нарушены!
-- =============================================

-- При очистке схем и пересозаднии БД в Dbeaver вручную удалить datatypes в таблице public!
-- Сначала удали ENUM в Postgre, если уже применялась такая миграция, при пересозаднии БД в Dbeaver вручную!

-- Инициальзация расширения PGVECTOR (задел на будущее)
CREATE EXTENSION IF NOT EXISTS vector;
COMMENT ON EXTENSION vector IS 'pgvector for vector similarity search and halfvec type';


-- Блок 1: Создание схем БД.
CREATE SCHEMA IF NOT EXISTS users;
CREATE SCHEMA IF NOT EXISTS dialogs;
CREATE SCHEMA IF NOT EXISTS orchestrator;
CREATE SCHEMA IF NOT EXISTS metrics;
CREATE SCHEMA IF NOT EXISTS common;
CREATE SCHEMA IF NOT EXISTS public;
CREATE SCHEMA IF NOT EXISTS state;
COMMENT ON SCHEMA state IS 'Схема для хранения конфигурационных параметров системы';


-- Блок 2: Пользовательские типы (ENUM) — ДО всех таблиц.
-- Создание ENUM для типа пользователей
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'public.actor_type') THEN
        CREATE TYPE public.actor_type AS ENUM (
            'system',     -- Сама система AGI
            'owner',      -- Владелец системы
            'user',       -- Пользователь системы с ограничениями по цензуре
            'ai_agent'    -- Любой внешний AI-агент
        );
    END IF;
END $$;
COMMENT ON TYPE public.actor_type IS 'Типы участников диалога с AGI системой: 
system — Сама система AGI, 
owner – Владелец системы, 
user – Пользователь системы с ограничениями доверия системы, 
ai_agent – Любой внешний AI агент';


-- Создание ENUM для пола пользователей
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'public.gender_type') THEN
        CREATE TYPE public.gender_type AS ENUM ('male', 'female', 'unknown');
    END IF;
END $$;
COMMENT ON TYPE public.gender_type IS 'Пол пользователя: male - мужской, female - женский, unknown - для AI агентов.';


-- Создание ENUM для типов источников сессий
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'public.external_source') THEN
        CREATE TYPE public.external_source AS ENUM (
            'console',        -- Консоль сервера
            'console_voice',  -- Голосовая консоль сервера
            'telegram',       -- Telegram мессенджер
            'api_rest'        -- Вход REST API агента
        );
    END IF;
END $$;
COMMENT ON TYPE public.external_source IS 'Типы внешних источников данных для идентификации участников диалогов: 
console – Консоль сервера, 
console_voice – Голосовая консоль сервера, 
telegram – Telegram мессенджер, 
api_rest – Вход REST API агента';


-- Создание ENUM для типа промпта (internal/external)
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'public.prompt_type') THEN
        CREATE TYPE public.prompt_type AS ENUM (
            'internal',   -- для внутреннего использования
            'external'    -- для внешних систем ИИ
        );
    END IF;
END $$;
COMMENT ON TYPE public.prompt_type IS 'Тип промпта: internal – для внутреннего использования, external - для внешних систем';


-- Создание ENUM для статуса промпта
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'public.prompt_status') THEN
        CREATE TYPE public.prompt_status AS ENUM (
            'testing',    -- тестирование
            'active',     -- активен
            'archived'    -- архивирован
        );
    END IF;
END $$;
COMMENT ON TYPE public.prompt_status IS 'Статус промпта: testing - тестирование, active - активен, archived - архивирован';


-- Cоздание ENUM для статуса сессии
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'public.session_status') THEN
        CREATE TYPE public.session_status AS ENUM (
            'active',     -- Активная сессия, диалог продолжается
            'completed'   -- Завершенная сессия
        );
    END IF;
END $$;
COMMENT ON TYPE public.session_status IS 'Статус сессии диалога: active - активная, completed - завершенная';


-- Создание ENUM для статуса задач и шагов оркестратора
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'public.task_status') THEN
        CREATE TYPE public.task_status AS ENUM (
            'pending',   -- Ожидает выполнения
            'running',   -- Выполняется
            'completed', -- Успешно завершена
            'failed'     -- Завершилась с ошибкой
        );
    END IF;
END $$;
COMMENT ON TYPE public.task_status IS 'Статус выполнения задачи оркестратора: pending - ожидает, running - выполняется, completed - успешно завершена, failed - ошибка';


-- Создание ENUM для типа рассуждений
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'public.reasoning_content_type') THEN
        CREATE TYPE public.reasoning_content_type AS ENUM ('messages', 'reflection', 'second_reflection');
    END IF;
END $$;
COMMENT ON TYPE public.reasoning_content_type IS 'Тип рассуждений: messages - анализ ссобщений диалога, 
reflection - рефлексия над производными первого порядка, second_reflection - рефлексия над производными последующих порядков';

-- Создание ENUM для причин завершения сессии
DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'public.session_close_reason') THEN
    CREATE TYPE public.session_close_reason AS ENUM (
        'user_exit',      -- Корректный выход (Ctrl+D / EOF)
        'user_command',   -- Команда выхода (exit / выход)
        'system_restart', -- Зависшая сессия при рестарте сервера
        'loop_error',     -- Ошибка в цикле диалога
        'critical_error', -- Критическая ошибка агента
        'unknown'         -- Причина не определена
    );
END IF;
END $$;
COMMENT ON TYPE public.session_close_reason IS 'Причины завершения сессии диалога';

-- Создание ENUM для для статусов диалогов
DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'dialog_status') THEN
    CREATE TYPE public.dialog_status AS ENUM ('active', 'completed');
END IF;
END $$;
COMMENT ON TYPE dialog_status IS 'Статусы диалогов: active - активен, completed - завершен';

-- Создание ENUM для для причин завершения диалогов (изолирован от сессий)
DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'dialog_close_reason') THEN
    CREATE TYPE public.dialog_close_reason AS ENUM (
        'user_new_dialogue',  -- Явный запрос пользователя (Ctrl+N)
        'inactivity_timeout', -- Автоматическое закрытие по таймауту
        'session_end',        -- Завершение вместе с родительской сессией
        'system_restart'      -- Зависший диалог при рестарте сервера
    );
END IF;
END $$;
COMMENT ON TYPE dialog_close_reason IS 'Причины завершения диалогов: 
user_new_dialogue - явный запрос пользователя (Ctrl+N)
inactivity_timeout - автоматическое закрытие по таймауту
session_end - завершение вместе с родительской сессией
system_restart - зависший диалог при рестарте сервера';

-- Создание ENUM для типов выключения (shutdown)
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'shutdown_type') THEN
        CREATE TYPE state.shutdown_type AS ENUM (
            'maintenance',       -- Плановое обслуживание оборудования
            'crash',             -- Аварийное завершение
            'forced_shutdown',   -- Принудительное выключение
            'user_absence',      -- Длительное отсутствие пользователя
            'agent_modification' -- Доработка и тестирование агента
        );
    END IF;
END $$;
COMMENT ON TYPE state.shutdown_type IS 'Тип причины отключения/простоя агента';


-- Создание ENUM для глобальных состояний агента
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'agent_state_type') THEN
        CREATE TYPE state.agent_state_type AS ENUM (
            'off',     -- Агент выключен (сервера не работают)
            'sleep',   -- Нет диалогов более X минут (сон)
            'active'   -- В диалоге (активность менее X минут)
        );
    END IF;
END $$;
COMMENT ON TYPE state.agent_state_type IS 'Макросостояния: off – не запущен, sleep – сон, active – в диалоге';


-- Создание ENUM для причин смены состояния (lifecycle)
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'lifecycle_change_reason') THEN
        CREATE TYPE state.lifecycle_change_reason AS ENUM (
            'inactivity_timeout',   -- Долгое бездействие → sleep
            'shutdown_command',     -- Команда выключения → off
            'startup',              -- Запуск сервера → off → active/sleep?
            'crash_recovery',       -- Восстановление после креша
            'user_wake_up',         -- Пробуждение из sleep сообщеним пользователя
            'agent_wake_up'         -- Пробуждение из sleep инициацией сообщения агентом  
        );
    END IF;
END $$;
COMMENT ON TYPE state.lifecycle_change_reason IS 'Причина перехода между состояниями off/sleep/active';

-- Блок 3: Общие функции
-- Общая функция для обновления полей updated_at
CREATE OR REPLACE FUNCTION common.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION common.update_updated_at_column IS 'Триггерная функция для автоматического обновления колонки updated_at';


-- Блок 4: Таблицы БД
-- 4.1 Таблица участников диалогов (actors).
CREATE TABLE IF NOT EXISTS users.actors (
    id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    type actor_type NOT NULL,
    name TEXT,
    gender gender_type,
    login TEXT UNIQUE,
    password_hash TEXT,
    email TEXT UNIQUE,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb, -- Метаданные и настройки (лимиты и прочее на будущее)
    access BOOLEAN NOT NULL DEFAULT true,
    verified BOOLEAN NOT NULL DEFAULT false,
    agent_version TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ
);

-- Подробные комментарии к таблице
COMMENT ON TABLE users.actors IS 'Таблица участников диалогов (пользователи и сама Кая)';

-- Комментарии к колонкам
COMMENT ON COLUMN users.actors.id IS 'Уникальный идентификатор участника диалога (UUID)';
COMMENT ON COLUMN users.actors.type IS 'Тип участника диалога: system - Сама система AGI, owner – Владелец системы, 
user – Пользователь системы с ограничениями по правам, ai_agent – Внешний AI агент';
COMMENT ON COLUMN users.actors.name IS 'Человекочитаемое имя (задается вручную, либо автоматически устанавливается при выявлении системой)';
COMMENT ON COLUMN users.actors.gender IS 'Пол участника: male - мужской, female - женский, unknown - неопределенный для AI агентов.';
COMMENT ON COLUMN users.actors.login IS 'Уникальный логин пользователя для входа в систему';
COMMENT ON COLUMN users.actors.password_hash IS 'Хэш пароля пользователя (рекомендуется использовать bcrypt или argon2)';
COMMENT ON COLUMN users.actors.email IS 'Уникальный адрес электронной почты пользователя';
COMMENT ON COLUMN users.actors.metadata IS 'Структурированные дополнительные данные: настройки лимитов диалогов и прочее на будущее';
COMMENT ON COLUMN users.actors.access IS 'Разрешен доступ к диалогу с системой: true - доступ разрешен, false - доступ заблокирован';
COMMENT ON COLUMN users.actors.verified IS 'Прошел ли пользователь верефикацию: true - верифицирован, false - ожидает подтверждения';
COMMENT ON COLUMN users.actors.agent_version IS 'Версия агента глобально из pyproject.toml, на момент создания/обновления записи';
COMMENT ON COLUMN users.actors.created_at IS 'Дата и время создания записи пользователя';
COMMENT ON COLUMN users.actors.updated_at IS 'Дата и время последнего обновления записи пользователя';

-- Индексы для оптимизации запросов
CREATE INDEX idx_actors_type ON users.actors (type);
CREATE INDEX idx_actors_login ON users.actors (login);
CREATE INDEX idx_actors_email ON users.actors (email);
CREATE INDEX idx_actors_gender ON users.actors (gender);
CREATE INDEX idx_actors_access ON users.actors (access);
CREATE INDEX idx_actors_verified ON users.actors (verified);

-- GIN индекс для поиска по JSONB полю metadata
CREATE INDEX idx_actors_metadata ON users.actors USING gin (metadata);

-- Заполнение начальных системных акторов
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM users.actors WHERE type = 'system') THEN
        INSERT INTO users.actors (type, metadata, access, verified, agent_version, created_at) VALUES
        ('system'::public.actor_type, '{}'::jsonb, true, true, '1.1.0', now());
        INSERT INTO users.actors (type, metadata, access, verified, agent_version, created_at) VALUES
        ('owner'::public.actor_type, '{}'::jsonb, true, true, '1.1.0', now());
    END IF;
END $$;

-- Подключение триггера updatet_at
DROP TRIGGER IF EXISTS trg_actors_update_updated_at ON users.actors;
CREATE TRIGGER trg_actors_update_updated_at
    BEFORE UPDATE ON users.actors
    FOR EACH ROW
    EXECUTE FUNCTION common.update_updated_at_column();


-- 4.2 Таблица внешних идентификаторов участников диалогов — ссылается на users.actors
CREATE TABLE IF NOT EXISTS users.actors_external_ids (
    id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    actor_id UUID NOT NULL REFERENCES users.actors(id) ON DELETE CASCADE,
    source external_source NOT NULL,
    source_id TEXT NOT NULL,
    authorized BOOLEAN NOT NULL DEFAULT false,
    agent_version TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ,
    -- Уникальность: один source_id на источник (защита от дублей)
    CONSTRAINT unique_source_source_id UNIQUE (source, source_id)
);

-- Подробные комментарии к таблице
COMMENT ON TABLE users.actors_external_ids IS 'Таблица внешних идентификаторов участников диалогов. Связывает внутренних участников (actors) 
с их идентификаторами во внешних системах.';

-- Комментарии к колонкам
COMMENT ON COLUMN users.actors_external_ids.id IS 'Уникальный идентификатор записи (UUID)';
COMMENT ON COLUMN users.actors_external_ids.actor_id IS 'Ссылка на участника диалога из таблицы users.actors. При удалении участника все его внешние 
идентификаторы удаляются каскадно (CASCADE)';
COMMENT ON COLUMN users.actors_external_ids.source IS 'Тип внешнего источника данных: console – Консоль сервера, console_voice – Голосовая консоль, 
telegram – Telegram мессенджер, api_rest – Входной REST API системы';
COMMENT ON COLUMN users.actors_external_ids.source_id IS 'Уникальный идентификатор во внешней системе (например "telegram:123456789", 
"root@1-srv", "api_key_abc123")';
COMMENT ON COLUMN users.actors_external_ids.authorized IS 'Авторизован ли данный идентификатор системой: true - авторизован и может использоваться, 
false - ожидает подтверждения или заблокирован';
COMMENT ON COLUMN users.actors_external_ids.agent_version IS 'Версия агента глобально из pyproject.toml, на момент создания/обновления записи';
COMMENT ON COLUMN users.actors_external_ids.created_at IS 'Дата и время создания записи внешнего идентификатора';
COMMENT ON COLUMN users.actors_external_ids.updated_at IS 'Дата и время последнего обновления записи внешнего идентификатора';

-- Индексы для оптимизации запросов
CREATE INDEX idx_actors_external_ids_actor_id ON users.actors_external_ids (actor_id);
CREATE INDEX idx_actors_external_ids_source ON users.actors_external_ids (source);
CREATE INDEX idx_actors_external_ids_authorized ON users.actors_external_ids (authorized);

-- Подключение триггера updatet_at
DROP TRIGGER IF EXISTS trg_actors_external_ids_update_updated_at ON users.actors_external_ids;
CREATE TRIGGER trg_actors_external_ids_update_updated_at
    BEFORE UPDATE ON users.actors_external_ids
    FOR EACH ROW
    EXECUTE FUNCTION common.update_updated_at_column();


-- 4.3 Таблица назначений промптов.
CREATE TABLE IF NOT EXISTS orchestrator.prompt_destinations (
    id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    agent_version TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Подробные комментарии к таблице
COMMENT ON TABLE orchestrator.prompt_destinations IS 'Справочник назначений промптов';

-- Комментарии к колонкам
COMMENT ON COLUMN orchestrator.prompt_destinations.id IS 'Уникальный идентификатор назначения';
COMMENT ON COLUMN orchestrator.prompt_destinations.name IS 'Наименование назначения (system - системный промпт, generative - для генерации ответов, и т.д.)';
COMMENT ON COLUMN orchestrator.prompt_destinations.description IS 'Описание назначения и особенностей использования';
COMMENT ON COLUMN orchestrator.prompt_destinations.agent_version IS 'Версия агента глобально из pyproject.toml, на момент создания/обновления записи';
COMMENT ON COLUMN orchestrator.prompt_destinations.created_at IS 'Дата и время создания записи';

-- Индексы для оптимизации запросов
CREATE INDEX idx_prompt_destinations_actor_name ON orchestrator.prompt_destinations (name);
CREATE INDEX idx_prompt_destinations_description ON orchestrator.prompt_destinations (description);

-- Базовое заполнение таблицы назначений
INSERT INTO orchestrator.prompt_destinations (name, description, agent_version, created_at) VALUES
    ('system', 'Системные промпты личности агента', '1.1.0', now()),
    ('generative', 'Промпты для генерации ответов', '1.1.0', now()),
    ('internal_logic', 'Промпты внутренней логики агента', '1.1.0', now()),
    ('external_api', 'Промпты для взаимодействия с внешними API', '1.1.0', now())

ON CONFLICT (name) DO NOTHING;


-- 4.4 Таблица сессий диалогов между пользователями и агентом
CREATE TABLE IF NOT EXISTS dialogs.sessions (
    id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    title TEXT,
    actor_id UUID NOT NULL REFERENCES users.actors(id) ON DELETE CASCADE,
    actor_external_id UUID REFERENCES users.actors_external_ids(id) ON DELETE SET NULL,
    status session_status NOT NULL DEFAULT 'active',
    reason session_close_reason,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ,
    closed_at TIMESTAMPTZ,
    sleep_duration INTERVAL,
    agent_version TEXT NOT NULL    
);

-- Подробные комментарии к таблице
COMMENT ON TABLE dialogs.sessions IS 'Таблица для хранения сессий диалогов между пользователями и агентом. 
Сессия объединяет последовательность сообщений в рамках одного непрерывного взаимодействия.';

-- Комментарии к колонкам
COMMENT ON COLUMN dialogs.sessions.id IS 'Уникальный идентификатор сессии диалога (UUID)';
COMMENT ON COLUMN dialogs.sessions.title IS 'Краткое название сессии (может генерироваться автоматически на основе первого сообщения или темы диалога)';
COMMENT ON COLUMN dialogs.sessions.actor_id IS 'ID участника диалога (пользователь или агент), с которым ведется сессия. Ссылка на users.actors. 
При удалении актора все его сессии удаляются каскадно.';
COMMENT ON COLUMN dialogs.sessions.actor_external_id IS 'ID внешнего источника подключения пользователя (например, конкретный Telegram аккаунт). 
Ссылка на users.actors_external_ids. При удалении внешнего ID устанавливается NULL.';
COMMENT ON COLUMN dialogs.sessions.status IS 'Статус сессии: active - активная (диалог продолжается), completed - завершенная (диалог окончен)';
COMMENT ON COLUMN dialogs.sessions.reason IS 'Причина завершения сессии: user_exit (корректный выход Ctrl+D), user_command (команда exit/выход), system_restart 
(зависшая сессия при перезапуске), loop_error (ошибка в цикле диалога), critical_error (критическая ошибка), unknown (неизвестно)';
COMMENT ON COLUMN dialogs.sessions.created_at IS 'Дата и время начала сессии (создания записи)';
COMMENT ON COLUMN dialogs.sessions.updated_at IS 'Метка времени последнего сообщения в диалоге (обновляется при каждом новом сообщении)';
COMMENT ON COLUMN dialogs.sessions.closed_at IS 'Дата и время завершения сессии (устанавливается при переходе в статус completed)';
COMMENT ON COLUMN dialogs.sessions.sleep_duration IS 'Длительность сна/простоя перед началом сессии';
COMMENT ON COLUMN dialogs.sessions.agent_version IS 'Версия агента глобально из pyproject.toml, на момент создания сессии';

-- Индексы для оптимизации запросов
CREATE INDEX idx_sessions_actor_id ON dialogs.sessions (actor_id);
CREATE INDEX idx_sessions_actor_external_id ON dialogs.sessions (actor_external_id); --Уникальное. Макс. 50 символов.
CREATE INDEX idx_sessions_status ON dialogs.sessions (status);
CREATE INDEX idx_sessions_created_at ON dialogs.sessions (created_at);
CREATE INDEX idx_sessions_updated_at ON dialogs.sessions (updated_at);
CREATE INDEX idx_sessions_closed_at ON dialogs.sessions (closed_at);

-- Индекс для поиска активных сессий с сортировкой по последнему использованию
CREATE INDEX idx_sessions_active_updated_at ON dialogs.sessions (status, updated_at DESC) WHERE status = 'active';

-- Подключение триггера updatet_at
DROP TRIGGER IF EXISTS trg_sessions_update_updated_at ON dialogs.sessions;
CREATE TRIGGER trg_sessions_update_updated_at
    BEFORE UPDATE ON dialogs.sessions
    FOR EACH ROW
    EXECUTE FUNCTION common.update_updated_at_column();


-- 4.5 Таблица типов задач оркестратора
CREATE TABLE IF NOT EXISTS orchestrator.task_types (
    id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    type_name VARCHAR(50) NOT NULL UNIQUE,
    description TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Подробные комментарии к таблице
COMMENT ON TABLE orchestrator.task_types IS 'Справочник типов задач оркестратора.';

-- Комментарии к колонкам
COMMENT ON COLUMN orchestrator.task_types.type_name IS 'Системное имя типа задачи (например: user_answer). Уникальное. Макс. 50 символов.';
COMMENT ON COLUMN orchestrator.task_types.description IS 'Человекочитаемое описание типа задачи.';
COMMENT ON COLUMN orchestrator.task_types.created_at IS 'Дата и время создания записи в локальном времени.';

-- Индексы для оптимизации запросов
CREATE INDEX idx_task_types_type_name ON orchestrator.task_types (type_name);
CREATE INDEX idx_task_types_type_description ON orchestrator.task_types (description);

-- Вставка предопределённых типов
INSERT INTO orchestrator.task_types (type_name, description)
VALUES 
    ('user_question_preprocessing', 'Преданализ вопроса пользователя'),
    ('user_answer_generation', 'Генерация финального ответа пользователю'),
    ('user_question_vectorize',     'Векторизация вопроса пользователя'),
    ('user_answer_vectorize',       'Векторизация ответа пользователю'),
    ('reasoning_vectorize',         'Векторизация цепочки рассуждений (reasoning / COT)'),
    ('prompts_vectorize', 'Векторизация промптов')    
ON CONFLICT (type_name) DO NOTHING;


-- 4.6 Таблица типов шагов оркестратора
CREATE TABLE IF NOT EXISTS orchestrator.step_types (
    id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    step_name VARCHAR(50) NOT NULL UNIQUE,
    description TEXT NOT NULL,
    agent_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Подробные комментарии к таблице
COMMENT ON TABLE orchestrator.step_types IS 'Справочник типов шагов оркестратора.';

-- Комментарии к колонкам
COMMENT ON COLUMN orchestrator.step_types.id IS 'Уникальный идентификатор типа шага (UUID).';
COMMENT ON COLUMN orchestrator.step_types.step_name IS 'Системное имя типа шага (например: user_question_preprocessing). Уникальное. Макс. 50 символов.';
COMMENT ON COLUMN orchestrator.step_types.description IS 'Человекочитаемое название типа шага.';
COMMENT ON COLUMN orchestrator.step_types.agent_version IS 'Версия агента глобально из pyproject.toml, на момент создания/обновления записи';
COMMENT ON COLUMN orchestrator.step_types.created_at IS 'Метка времени создания записи.';

-- Индексы для оптимизации запросов
CREATE INDEX idx_step_types_name ON orchestrator.step_types (step_name);
CREATE INDEX idx_step_types_description ON orchestrator.step_types (description);

-- Вставка предопределённых типов
INSERT INTO orchestrator.step_types (step_name, description, agent_version) 
VALUES
    ('user_question_preprocessing', 'Предразбор вопроса пользователя', '1.1.0'),
    ('user_answer_generation',      'Генерация финального ответа пользователю', '1.1.0'),
    ('user_question_vectorize',     'Векторизация вопроса пользователя', '1.1.0'),
    ('user_answer_vectorize',       'Векторизация ответа пользователю', '1.1.0'),
    ('reasoning_vectorize',         'Векторизация цепочки рассуждений (reasoning / COT)', '1.1.0'),
    ('prompts_vectorize',           'Векторизация промпта', '1.1.0')
ON CONFLICT (step_name) DO NOTHING;


-- 4.7 Таблица задач оркестратора
CREATE TABLE IF NOT EXISTS orchestrator.orchestrator_tasks (
    id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    task_type_id UUID NOT NULL REFERENCES orchestrator.task_types(id) ON DELETE RESTRICT,
    parent_task_id UUID REFERENCES orchestrator.orchestrator_tasks(id),
    input_data JSONB,
    output_data JSONB,
    priority DECIMAL(3,2) CHECK (priority >= 0.00 AND priority <= 1.00),
    status task_status NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    run_latency FLOAT,
    total_latency FLOAT,
    error_module TEXT,
    error_message TEXT,
    error_timestamp TIMESTAMPTZ,
    agent_version TEXT NOT NULL
);

-- Подробные комментарии к таблице
COMMENT ON TABLE orchestrator.orchestrator_tasks IS 'Динамическая таблица задач оркестратора.';

-- Комментарии к колонкам
COMMENT ON COLUMN orchestrator.orchestrator_tasks.id IS 'Уникальный идентификатор задачи (UUID)';
COMMENT ON COLUMN orchestrator.orchestrator_tasks.task_type_id IS 'Ссылка на тип задачи из справочника task_types.';
COMMENT ON COLUMN orchestrator.orchestrator_tasks.parent_task_id IS 'Ссылка на родительскую задачу.';
COMMENT ON COLUMN orchestrator.orchestrator_tasks.input_data IS 'Входные данные для выполнения задачи в формате JSONB';
COMMENT ON COLUMN orchestrator.orchestrator_tasks.output_data IS 'Результат выполнения задачи в формате JSONB';
COMMENT ON COLUMN orchestrator.orchestrator_tasks.priority IS 'Приоритет задачи от 0.00 (низкий) до 1.00 (высокий)';
COMMENT ON COLUMN orchestrator.orchestrator_tasks.status IS 'Статус выполнения задачи: pending - ожидает, running - выполняется, completed - успешно завершена, failed - ошибка';
COMMENT ON COLUMN orchestrator.orchestrator_tasks.created_at IS 'Время создания записи задачи';
COMMENT ON COLUMN orchestrator.orchestrator_tasks.started_at IS 'Время начала выполнения задачи';
COMMENT ON COLUMN orchestrator.orchestrator_tasks.completed_at IS 'Время завершения выполнения задачи';
COMMENT ON COLUMN orchestrator.orchestrator_tasks.total_latency IS 'Общее время выполнения задачи (completed_at - created_at) в секундах';
COMMENT ON COLUMN orchestrator.orchestrator_tasks.run_latency IS 'Время исполнения задачи (completed_at - started_at) в секундах';
COMMENT ON COLUMN orchestrator.orchestrator_tasks.error_module IS 'Модуль, в котором произошла ошибка';
COMMENT ON COLUMN orchestrator.orchestrator_tasks.error_message IS 'Текст ошибки';
COMMENT ON COLUMN orchestrator.orchestrator_tasks.error_timestamp IS 'Время фиксации ошибки';
COMMENT ON COLUMN orchestrator.orchestrator_tasks.agent_version IS 'Версия агента глобально из pyproject.toml, на момент создания/обновления записи';

-- Индексы для оптимизации запросов
CREATE INDEX IF NOT EXISTS idx_orchestrator_tasks_task_type_id ON orchestrator.orchestrator_tasks(task_type_id);
CREATE INDEX IF NOT EXISTS idx_orchestrator_tasks_parent_task_id ON orchestrator.orchestrator_tasks(parent_task_id);
CREATE INDEX IF NOT EXISTS idx_orchestrator_tasks_status ON orchestrator.orchestrator_tasks(status);
CREATE INDEX IF NOT EXISTS idx_orchestrator_tasks_created_at ON orchestrator.orchestrator_tasks(created_at);
CREATE INDEX IF NOT EXISTS idx_orchestrator_tasks_started_at ON orchestrator.orchestrator_tasks(started_at);
CREATE INDEX IF NOT EXISTS idx_orchestrator_tasks_completed_at ON orchestrator.orchestrator_tasks(completed_at);
CREATE INDEX IF NOT EXISTS idx_orchestrator_tasks_total_latency ON orchestrator.orchestrator_tasks (total_latency);
CREATE INDEX IF NOT EXISTS idx_orchestrator_tasks_run_latency ON orchestrator.orchestrator_tasks (run_latency);
CREATE INDEX IF NOT EXISTS idx_orchestrator_tasks_priority ON orchestrator.orchestrator_tasks (priority);

-- GIN индекс для JSONB полей
CREATE INDEX IF NOT EXISTS idx_orchestrator_tasks_input_data ON orchestrator.orchestrator_tasks USING gin (input_data);
CREATE INDEX IF NOT EXISTS idx_orchestrator_tasks_output_data ON orchestrator.orchestrator_tasks USING gin (output_data);


-- 4.8 Таблица шагов оркестратора.
CREATE TABLE IF NOT EXISTS orchestrator.orchestrator_steps (
    id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    parent_step_id UUID REFERENCES orchestrator.orchestrator_steps(id) ON DELETE RESTRICT,
    task_type_name TEXT, 
    task_id UUID NOT NULL REFERENCES orchestrator.orchestrator_tasks(id) ON DELETE RESTRICT,
    step_name TEXT, 
    step_number INTEGER NOT NULL,
    step_type_id UUID NOT NULL REFERENCES orchestrator.step_types(id) ON DELETE RESTRICT,
    status task_status NOT NULL DEFAULT 'pending',
    input_data JSONB,
    output_data JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    latency FLOAT,
    llm_metric_id UUID, -- Связь будет добавлена позже
    emb_metric_id UUID, -- Связь будет добавлена позже
    error_module TEXT,
    error_message TEXT,
    error_timestamp TIMESTAMPTZ,
    agent_version TEXT NOT NULL
);

-- Подробные комментарии к таблице
COMMENT ON TABLE orchestrator.orchestrator_steps IS 'Лог выполнения шагов оркестратора';

-- Комментарии к колонкам
COMMENT ON COLUMN orchestrator.orchestrator_steps.id IS 'Уникальный идентификатор шага (UUID)';
COMMENT ON COLUMN orchestrator.orchestrator_steps.parent_step_id IS 'Родительский шаг';
COMMENT ON COLUMN orchestrator.orchestrator_steps.task_type_name IS 'Название типа задачи (из orchestrator.task_types.type_name). Заполняется автоматически';
COMMENT ON COLUMN orchestrator.orchestrator_steps.task_id IS 'Ссылка на задачу.';
COMMENT ON COLUMN orchestrator.orchestrator_steps.step_name IS 'Системное имя типа шага (из orchestrator.step_types.step_name). Заполняется автоматически';
COMMENT ON COLUMN orchestrator.orchestrator_steps.step_number IS 'Порядковый номер шага внутри задачи';
COMMENT ON COLUMN orchestrator.orchestrator_steps.step_type_id IS 'Ссылка на тип шага';
COMMENT ON COLUMN orchestrator.orchestrator_steps.status IS 'Статус выполнения шага';
COMMENT ON COLUMN orchestrator.orchestrator_steps.input_data IS 'Входные данные шага';
COMMENT ON COLUMN orchestrator.orchestrator_steps.output_data IS 'Выходные данные шага';
COMMENT ON COLUMN orchestrator.orchestrator_steps.created_at IS 'Время создания шага';
COMMENT ON COLUMN orchestrator.orchestrator_steps.completed_at IS 'Время завершения шага';
COMMENT ON COLUMN orchestrator.orchestrator_steps.latency IS 'Задержка выполнения шага в секундах';
COMMENT ON COLUMN orchestrator.orchestrator_steps.llm_metric_id IS 'Ссылка на метрики LLM, использованной для обработки';
COMMENT ON COLUMN orchestrator.orchestrator_steps.emb_metric_id IS 'Ссылка на метрики модели эмбендингов, использованной для обработки';
COMMENT ON COLUMN orchestrator.orchestrator_steps.error_message IS 'Текст ошибки';
COMMENT ON COLUMN orchestrator.orchestrator_steps.error_module IS 'Модуль, в котором произошла ошибка';
COMMENT ON COLUMN orchestrator.orchestrator_steps.error_timestamp IS 'Время фиксации ошибки';
COMMENT ON COLUMN orchestrator.orchestrator_steps.agent_version IS 'Версия агента глобально из pyproject.toml, на момент создания/обновления записи';

-- Индексы для оптимизации запросов
CREATE INDEX idx_orchestrator_steps_task_id ON orchestrator.orchestrator_steps (task_id);
CREATE INDEX idx_orchestrator_steps_step_type_id ON orchestrator.orchestrator_steps (step_type_id);
CREATE INDEX idx_orchestrator_steps_status ON orchestrator.orchestrator_steps (status);
CREATE INDEX idx_orchestrator_steps_parent_id ON orchestrator.orchestrator_steps (parent_step_id);
CREATE INDEX idx_orchestrator_steps_created_at ON orchestrator.orchestrator_steps (created_at);
CREATE INDEX idx_orchestrator_steps_completed_at ON orchestrator.orchestrator_steps (completed_at);
CREATE INDEX idx_orchestrator_steps_llm_metric_id ON orchestrator.orchestrator_steps(llm_metric_id);
CREATE INDEX idx_orchestrator_steps_emb_metric_id ON orchestrator.orchestrator_steps(emb_metric_id);

-- GIN индекс для JSONB полей
CREATE INDEX idx_orchestrator_steps_input_data ON orchestrator.orchestrator_steps USING gin (input_data);
CREATE INDEX idx_orchestrator_steps_output_data ON orchestrator.orchestrator_steps USING gin (output_data);

-- Уникальность: один шаг с номером N в рамках одной задачи
CREATE UNIQUE INDEX idx_orchestrator_steps_task_step_unique 
ON orchestrator.orchestrator_steps (task_id, step_number);

-- Триггерная функция для заполнения step_name и task_type_name
CREATE OR REPLACE FUNCTION orchestrator.populate_step_enriched_fields()
RETURNS TRIGGER AS $$
BEGIN
    -- Заполняем step_name из step_types
    IF NEW.step_type_id IS NOT NULL THEN
        SELECT st.step_name
        INTO NEW.step_name
        FROM orchestrator.step_types st
        WHERE st.id = NEW.step_type_id;
    END IF;

    -- Заполняем task_type_name из task_types (через orchestrator_tasks)
    IF NEW.task_id IS NOT NULL THEN
        SELECT tt.type_name
        INTO NEW.task_type_name
        FROM orchestrator.orchestrator_tasks ot
        JOIN orchestrator.task_types tt ON ot.task_type_id = tt.id
        WHERE ot.id = NEW.task_id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Удаляем и создаём триггер
DROP TRIGGER IF EXISTS trg_step_populate_enriched ON orchestrator.orchestrator_steps;
CREATE TRIGGER trg_step_populate_enriched
BEFORE INSERT ON orchestrator.orchestrator_steps
FOR EACH ROW
EXECUTE FUNCTION orchestrator.populate_step_enriched_fields();


-- 4.9 Таблица рассуждений агента (reasonings / Chain of tought)
CREATE TABLE IF NOT EXISTS orchestrator.reasonings (
    -- Основные идентификаторы
    id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    orchestrator_step_id UUID REFERENCES orchestrator.orchestrator_steps(id) ON DELETE SET NULL,
    reasoning_content TEXT NOT NULL,
    reasoning_content_type reasoning_content_type NOT NULL,
    agent_version TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Подробные комментарии к таблице
COMMENT ON TABLE orchestrator.reasonings IS 'Таблица рассуждений (reasonings) оркестратора. Содержит внутренние мыслительные процессы агента, цепочки рассуждений и саморефлексию.';

-- Комментарии к колонкам
COMMENT ON COLUMN orchestrator.reasonings.id IS 'Уникальный идентификатор рассуждения (UUID)';
COMMENT ON COLUMN orchestrator.reasonings.orchestrator_step_id IS 'Ссылка на шаг оркестратора (задачу), в рамках которого было сгенерировано рассуждение. Идентификатор шага внутри цепочки рассуждений.';
COMMENT ON COLUMN orchestrator.reasonings.reasoning_content IS 'Выходной блок /think модели - результат рассуждения агента. Содержит внутренний монолог, логические цепочки, размышления.';
COMMENT ON COLUMN orchestrator.reasonings.reasoning_content_type IS 'Тип источника контекста: messages - на сообщения диалога, reflection - саморефлексия агента.';
COMMENT ON COLUMN orchestrator.reasonings.agent_version IS 'Версия агента глобально из pyproject.toml, на момент создания/обновления записи';
COMMENT ON COLUMN orchestrator.reasonings.timestamp IS 'Дата и время создания записи рассуждения';

-- Индексы для оптимизации запросов
CREATE INDEX idx_reasonings_orchestrator_step_fk ON orchestrator.reasonings (orchestrator_step_id);
CREATE INDEX idx_reasonings_reasoning_content_type ON orchestrator.reasonings (reasoning_content_type);
CREATE INDEX idx_reasonings_agent_version ON orchestrator.reasonings (agent_version);

-- GIN индекс для полнотекстового поиска по содержанию рассуждений
CREATE INDEX idx_reasonings_reasoning_content ON orchestrator.reasonings USING gin(to_tsvector('russian', reasoning_content))
    WHERE reasoning_content IS NOT NULL;


-- 4.10 Таблица метрик внутренних эмбеддингов
CREATE TABLE IF NOT EXISTS metrics.emb_internal (
    id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    orchestrator_step_id UUID REFERENCES orchestrator.orchestrator_steps(id) ON DELETE RESTRICT,
    orchestrator_step_name TEXT,
    host TEXT,
    model TEXT NOT NULL,
    param JSONB,
    vector_dimension INTEGER,
    prompt_tokens INTEGER, --Количество токенов в промпте (входные данные)
    out_time TIMESTAMPTZ,  -- время отправки запроса на emb-сервер
    in_time TIMESTAMPTZ,   -- время получения ответа от emb-сервера
    full_time FLOAT, -- общее время генерации
    error_status BOOLEAN NOT NULL DEFAULT false,
    error_message TEXT,
    error_time TIMESTAMPTZ,
    agent_version TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Подробные комментарии к таблице
COMMENT ON TABLE metrics.emb_internal IS 'Таблица метрик внутренних эмбеддингов. Содержит технические параметры и результаты генерации 
векторов для анализа производительности и отладки.';

-- Комментарии к колонкам
COMMENT ON COLUMN metrics.emb_internal.id IS 'Уникальный идентификатор операции эмбеддинга (UUID)';
COMMENT ON COLUMN metrics.emb_internal.host IS 'Имя сервера/хоста, на котором выполнялась генерация эмбеддинга';
COMMENT ON COLUMN metrics.emb_internal.orchestrator_step_id IS 'Ссылка на шаг оркестратора, инициировавший запрос эмбеддинга. Позволяет связать метрики с конкретным шагом обработки.';
COMMENT ON COLUMN metrics.emb_internal.orchestrator_step_name IS 'Описание типа шага оркестратора (из orchestrator.step_types.step_name). Заполняется автоматически триггером.';
COMMENT ON COLUMN metrics.emb_internal.model IS 'Название примененной модели эмбеддингов (например: "text-embedding-3-small", "intfloat/multilingual-e5-large")';
COMMENT ON COLUMN metrics.emb_internal.param IS 'Параметры генерации эмбеддинга в формате JSON (размерность, нормализация, pooling и др.)';
COMMENT ON COLUMN metrics.emb_internal.vector_dimension IS 'Размерность полученного эмбеддинга (например 1024, 2560, 4096). Зависит от модели и настроек.';
COMMENT ON COLUMN metrics.emb_internal.prompt_tokens IS 'Количество токенов в промпте (входные данные для векторизации)';
COMMENT ON COLUMN metrics.emb_internal.out_time IS 'Время отправки запроса на emb-сервер (начало операции)';
COMMENT ON COLUMN metrics.emb_internal.in_time IS 'Время получения ответа от emb-сервера (окончание операции)';
COMMENT ON COLUMN metrics.emb_internal.full_time IS 'Общее время генерации эмбеддинга в секундах (разница между in_time и out_time)';
COMMENT ON COLUMN metrics.emb_internal.error_status IS 'Флаг наличия ошибок при генерации: true - была ошибка, false - успешно';
COMMENT ON COLUMN metrics.emb_internal.error_message IS 'Текстовое описание ошибки генерации (заполняется при error_status = true)';
COMMENT ON COLUMN metrics.emb_internal.error_time IS 'Метка времени фиксации ошибки';
COMMENT ON COLUMN metrics.emb_internal.agent_version IS 'Версия агента глобально из pyproject.toml, на момент создания/обновления записи';
COMMENT ON COLUMN metrics.emb_internal.timestamp IS 'Метка времени создания записи метрики';

-- Индексы для оптимизации запросов
CREATE INDEX idx_emb_internal_id ON metrics.emb_internal (id);
CREATE INDEX idx_emb_internal_orchestrator_step ON metrics.emb_internal (orchestrator_step_id);
CREATE INDEX idx_emb_internal_model ON metrics.emb_internal (model);
CREATE INDEX idx_emb_internal_host ON metrics.emb_internal (host);
CREATE INDEX idx_emb_internal_error_status ON metrics.emb_internal (error_status);
CREATE INDEX idx_emb_internal_timestamp ON metrics.emb_internal (timestamp);
CREATE INDEX idx_emb_internal_full_time ON metrics.emb_internal (full_time);
CREATE INDEX idx_emb_internal_prompt_tokens ON metrics.emb_internal (prompt_tokens);
CREATE INDEX idx_emb_internal_agent_version ON metrics.emb_internal (agent_version);

-- Триггер для автоматического заполнения orchestrator_step_name
CREATE OR REPLACE FUNCTION metrics.populate_emb_step_name()
RETURNS TRIGGER AS $$
BEGIN
    -- Заполняем только если orchestrator_step_id задан
    IF NEW.orchestrator_step_id IS NOT NULL THEN
        SELECT st.step_name
        INTO NEW.orchestrator_step_name
        FROM orchestrator.orchestrator_steps os
        JOIN orchestrator.step_types st ON os.step_type_id = st.id
        WHERE os.id = NEW.orchestrator_step_id;
        -- Если JOIN не дал результат, NEW.orchestrator_step_name станет NULL — это допустимо
    ELSE
        NEW.orchestrator_step_name := NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION metrics.populate_emb_step_name IS 'Триггерная функция для автоматического заполнения orchestrator_step_name из справочника step_types';

-- Триггер
DROP TRIGGER IF EXISTS trg_emb_populate_step_name ON metrics.emb_internal;
CREATE TRIGGER trg_emb_populate_step_name
BEFORE INSERT ON metrics.emb_internal
FOR EACH ROW
EXECUTE FUNCTION metrics.populate_emb_step_name();

-- Добавляем ссылку FK на поле созданной ранее таблицы orchestrator_steps
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_steps_emb_metric'
          AND conrelid = 'orchestrator.orchestrator_steps'::regclass
    ) THEN
        ALTER TABLE orchestrator.orchestrator_steps
            ADD CONSTRAINT fk_steps_emb_metric
            FOREIGN KEY (emb_metric_id)
            REFERENCES metrics.emb_internal(id)
            ON DELETE SET NULL;
    END IF;
END $$;


-- 4.11 Таблица промптов
CREATE TABLE IF NOT EXISTS orchestrator.prompts (
    id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    version TEXT NOT NULL,  -- SemVer формат
    name TEXT NOT NULL,
    description TEXT,
    type prompt_type NOT NULL,
    destination_id UUID NOT NULL REFERENCES orchestrator.prompt_destinations(id),
    text TEXT NOT NULL,
    params JSONB NOT NULL DEFAULT '{}'::jsonb,
    prompt_effectiveness JSONB NOT NULL DEFAULT '{}'::jsonb,
    status prompt_status NOT NULL DEFAULT 'testing',
    created_by UUID NOT NULL REFERENCES users.actors(id),
    change_reason TEXT,
    qdrant_point_id TEXT,
    emb_metric_id UUID REFERENCES metrics.emb_internal(id) ON DELETE RESTRICT,
    qdrant_timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ,
    agent_version TEXT NOT NULL,
    -- Уникальность: имя + версия (одна версия промпта с таким именем)
    CONSTRAINT unique_prompt_name_version UNIQUE (name, version)
);

-- Подробные комментарии к таблице
COMMENT ON TABLE orchestrator.prompts IS 'Таблица промптов системы с поддержкой версионирования и метаданными для саморефлексии';

-- Комментарии к колонкам
COMMENT ON COLUMN orchestrator.prompts.id IS 'Уникальный идентификатор промпта (UUID)';
COMMENT ON COLUMN orchestrator.prompts.version IS 'Версия промпта в формате SemVer (например: "1.0.0", "2.1.3")';
COMMENT ON COLUMN orchestrator.prompts.name IS 'Человекочитаемое название промпта, описывающее его назначение (например: "greeting_message", "code_review")';
COMMENT ON COLUMN orchestrator.prompts.description IS 'Подробное описание назначения и особенностей использования промпта';
COMMENT ON COLUMN orchestrator.prompts.type IS 'Тип промпта: internal – для внутреннего использования, external - для внешних систем';
COMMENT ON COLUMN orchestrator.prompts.destination_id IS 'Ссылка на назначение промпта (system - системный, generative - для генеративных моделей, 
api_external - для внешних API)';
COMMENT ON COLUMN orchestrator.prompts.text IS 'Текст промпта с поддержкой шаблонов и переменных';
COMMENT ON COLUMN orchestrator.prompts.params IS 'Динамические параметры модели для генерации в формате JSON. Пример: { "model_name": "Qwen3-8B", 
"temperature": 0.6, "top_p": 0.95, "max_tokens": 4096 }';
COMMENT ON COLUMN orchestrator.prompts.prompt_effectiveness IS 'Заполняется саморефлексией агента на основании статистики применения. 
Содержит метрики эффективности, успешности, затрат';
COMMENT ON COLUMN orchestrator.prompts.status IS 'Статус промпта: testing - тестирование, active - активен, archived - архивирован';
COMMENT ON COLUMN orchestrator.prompts.created_by IS 'Создатель промпта (ссылка на users.actors): owner - владелец системы, system - сама система';
COMMENT ON COLUMN orchestrator.prompts.change_reason IS 'Описание причины создания новой версии промпта (что изменено и почему)';
COMMENT ON COLUMN orchestrator.prompts.qdrant_point_id IS 'Общий идентификатор точки в векторной базе Qdrant для данного промпта (для семантического поиска 
схожести всего промпта а не его чанков)';
COMMENT ON COLUMN orchestrator.prompts.emb_metric_id IS 'Ссылка на метрики формирования эмбеддинга. Позволяет анализировать качество векторизации.';
COMMENT ON COLUMN orchestrator.prompts.qdrant_timestamp IS 'Время сохранения вектора в Qdrant и присвоения qdrant_point_id.';
COMMENT ON COLUMN orchestrator.prompts.created_at IS 'Дата и время создания промпта';
COMMENT ON COLUMN orchestrator.prompts.updated_at IS 'Дата и время последнего обновления промпта';
COMMENT ON COLUMN orchestrator.prompts.agent_version IS 'Версия агента глобально из pyproject.toml, на момент создания/обновления записи';

-- Индексы для оптимизации запросов
CREATE INDEX idx_prompts_name ON orchestrator.prompts (name);
CREATE INDEX idx_prompts_version ON orchestrator.prompts (version);
CREATE INDEX idx_prompts_type ON orchestrator.prompts (type);
CREATE INDEX idx_prompts_destination ON orchestrator.prompts (destination_id);
CREATE INDEX idx_prompts_status ON orchestrator.prompts (status);
CREATE INDEX idx_prompts_created_by ON orchestrator.prompts (created_by);
CREATE INDEX idx_prompts_created_at ON orchestrator.prompts (created_at);
CREATE INDEX idx_prompts_qdrant_point ON orchestrator.prompts (qdrant_point_id);
CREATE INDEX idx_prompts_agent_version ON orchestrator.prompts (agent_version);

-- GIN индексы для JSONB полей
CREATE INDEX idx_prompts_params ON orchestrator.prompts USING gin (params);
CREATE INDEX idx_prompts_effectiveness ON orchestrator.prompts USING gin (prompt_effectiveness);

-- Добавляем первый системный промпт
DO $$
DECLARE
    v_destination_id UUID;
    v_creator_id UUID;
    v_prompt_name TEXT := 'agent_core_identity';
    v_prompt_version TEXT := '1.1.0';
BEGIN
    -- Получаем ID назначения 'system'
    SELECT id INTO v_destination_id 
    FROM orchestrator.prompt_destinations 
    WHERE name = 'system';
    
    -- Получаем ID создателя агента
    SELECT id INTO v_creator_id 
    FROM users.actors 
    WHERE type = 'owner' 
    LIMIT 1;
    
    -- Проверяем, что все необходимые ID получены
    IF v_destination_id IS NULL THEN
        RAISE EXCEPTION 'Назначение "system" не найдено в таблице prompt_destinations';
    END IF;
    
    IF v_creator_id IS NULL THEN
        RAISE EXCEPTION 'Создатель с типом "owner" не найден в таблице actors';
    END IF;
    
    -- Проверяем, существует ли уже такой промпт (name + version)
    IF NOT EXISTS (
        SELECT 1 FROM orchestrator.prompts 
        WHERE name = v_prompt_name 
        AND version = v_prompt_version
    ) THEN
        -- Вставляем системный промпт
        INSERT INTO orchestrator.prompts (
            version,
            name,
            description,
            type,
            destination_id,
            text,
            params,
            prompt_effectiveness,
            status,
            created_by,
            agent_version,
            created_at
        ) VALUES (
            v_prompt_version,
            v_prompt_name,
            'Базовый системный промпт агента',
            'internal'::public.prompt_type,
            v_destination_id,
            E'<Правила>\n' ||
            E'Ты — универсальный ассистент-эксперт по практическим жизненным вопросам.\n' ||
            E'Отвечай кратко, по существу, максимально полезно.\n' ||
            E'Сначала давай прямой ответ, затем, если требуются необходимые пояснения.\n' ||
            E'Если точной информации нет, скажи «Точных данных у меня нет, могу только предположить» и чётко обозначь, что это допущение.\n' ||
            E'Никогда не выдумывай факты, цифры, ссылки ради связности текста. Если не хватает данных для полноценного точного ответа - запроси у собеседника.\n' ||
            E'Для списков используй маркированные перечисления без эмодзи и смайликов.\n' ||
            E'Если вопрос подразумевает план действий — перечисляй конкретные шаги.\n' ||
            E'Отвечай от женского лица, использую женские грамматические формы (я, моя, сказала и т.п.).\n' ||
            E'</Правила>\n',
            '{
                "model_name": "Qwen3.5-9B-Q4_K_M.gguf",
                "temperature": 0.9,
                "top_p": 1.0,
                "top_k": 40,
                "min_p": 0,
                "max_tokens": 32768,
                "presence_penalty": 2.0,
                "repetition_penalty": 1.0,
                "stop": ["<|im_end|>"],
                "chat_template_kwargs": {"enable_thinking": false}
            }'::jsonb,
            '{}'::jsonb,
            'testing'::public.prompt_status,
            v_creator_id,
            '1.1.0',
            now()
        );
        
        RAISE NOTICE 'Системный промпт % версии % успешно создан', v_prompt_name, v_prompt_version;
    ELSE
        RAISE NOTICE 'Системный промпт % v% уже существует, пропускаем создание', v_prompt_name, v_prompt_version;
    END IF;
END $$;

-- Подключение триггера updatet_at
DROP TRIGGER IF EXISTS trg_prompts_update_updated_at ON orchestrator.prompts;
CREATE TRIGGER trg_prompts_update_updated_at
    BEFORE UPDATE ON orchestrator.prompts
    FOR EACH ROW
    EXECUTE FUNCTION common.update_updated_at_column();


-- 4.12 Таблица метрик внутренних LLM запросов
CREATE TABLE IF NOT EXISTS metrics.llm_internal (
    id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    host TEXT,
    orchestrator_step_id UUID REFERENCES orchestrator.orchestrator_steps(id) ON DELETE RESTRICT,
    orchestrator_step_name TEXT,
    prompt_id UUID REFERENCES orchestrator.prompts(id) ON DELETE RESTRICT,
    param JSONB,
    model TEXT NOT NULL,
    cache_n INTEGER, 
    prompt_tokens INTEGER,
    completion_tokens INTEGER,  
    total_tokens INTEGER,
    host_nctx INTEGER,
    prompt_ms FLOAT, 
    prompt_per_token_ms FLOAT, 
    prompt_per_second FLOAT, 
    predicted_per_second FLOAT,
    resp_time FLOAT,
    net_latency FLOAT,
    full_time FLOAT,
    error_status BOOLEAN NOT NULL DEFAULT false,
    error_message TEXT,
    error_time TIMESTAMPTZ,
    agent_version TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Подробные комментарии к таблице
COMMENT ON TABLE metrics.llm_internal IS 'Таблица метрик внутренних LLM запросов. Содержит технические параметры генерации.';

-- Комментарии к колонкам
COMMENT ON COLUMN metrics.llm_internal.id IS 'Уникальный идентификатор операции LLM.';
COMMENT ON COLUMN metrics.llm_internal.orchestrator_step_id IS 'Внешний ключ на шаг оркестратора. Ссылка на процесс, инициировавший LLM запрос.';
COMMENT ON COLUMN metrics.llm_internal.orchestrator_step_name IS 'Описание типа шага оркестратора (из orchestrator.step_types.step_name).';
COMMENT ON COLUMN metrics.llm_internal.prompt_id IS 'Внешний ключ на использованный промпт.';
COMMENT ON COLUMN metrics.llm_internal.param IS 'Параметры генерации в формате JSON (temperature, top_p, top_k, max_tokens и др)';
COMMENT ON COLUMN metrics.llm_internal.model IS 'Название примененной модели';
COMMENT ON COLUMN metrics.llm_internal.cache_n IS 'Сколько токенов из запроса (промпта) было взято из кэша. 0 - кэш не использовался';
COMMENT ON COLUMN metrics.llm_internal.prompt_tokens IS 'Количество токенов в промпте (входные данные)';
COMMENT ON COLUMN metrics.llm_internal.completion_tokens IS 'Количество токенов, сгенерированных моделью в ответе';
COMMENT ON COLUMN metrics.llm_internal.total_tokens IS 'Общее количество токенов, обработанных за этот запрос';
COMMENT ON COLUMN metrics.llm_internal.host_nctx IS 'Размер контекста (n_ctx), настроенный на хосте для модели';
COMMENT ON COLUMN metrics.llm_internal.prompt_ms IS 'Время в миллисекундах на обработку промпта';
COMMENT ON COLUMN metrics.llm_internal.prompt_per_token_ms IS 'Среднее время обработки одного токена промпта в миллисекундах';
COMMENT ON COLUMN metrics.llm_internal.prompt_per_second IS 'Средняя скорость обработки токенов промпта в секунду';
COMMENT ON COLUMN metrics.llm_internal.predicted_per_second IS 'Средняя скорость генерации токенов ответа в секунду';
COMMENT ON COLUMN metrics.llm_internal.resp_time IS 'Общее время генерации ответа в секундах';
COMMENT ON COLUMN metrics.llm_internal.net_latency IS 'Задержка сети при выполнении запроса (секунды)';
COMMENT ON COLUMN metrics.llm_internal.full_time IS 'Общее время выполнения запроса от клиента до сервера (секунды)';
COMMENT ON COLUMN metrics.llm_internal.error_status IS 'Флаг наличия ошибок при генерации';
COMMENT ON COLUMN metrics.llm_internal.error_message IS 'Текстовое описание ошибки генерации';
COMMENT ON COLUMN metrics.llm_internal.error_time IS 'Метка времени фиксации ошибки';
COMMENT ON COLUMN metrics.llm_internal.agent_version IS 'Версия агента глобально из pyproject.toml, на момент создания/обновления записи';
COMMENT ON COLUMN metrics.llm_internal.timestamp IS 'Метка времени создания записи метрики';

-- Индексы для оптимизации запросов
CREATE INDEX idx_llm_internal_id ON metrics.llm_internal (id);
CREATE INDEX idx_llm_internal_orchestrator_step ON metrics.llm_internal (orchestrator_step_id);
CREATE INDEX idx_llm_internal_prompt_id ON metrics.llm_internal (prompt_id);
CREATE INDEX idx_llm_internal_model ON metrics.llm_internal (model);
CREATE INDEX idx_llm_internal_host ON metrics.llm_internal (host);
CREATE INDEX idx_llm_internal_predicted_per_second ON metrics.llm_internal (predicted_per_second);
CREATE INDEX idx_llm_internal_error_status ON metrics.llm_internal (error_status);
CREATE INDEX idx_llm_internal_agent_version ON metrics.llm_internal (agent_version);
CREATE INDEX idx_llm_internal_timestamp ON metrics.llm_internal (timestamp);


-- Триггер для автоматического заполнения orchestrator_step_name
CREATE OR REPLACE FUNCTION metrics.populate_llm_step_name()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.orchestrator_step_id IS NOT NULL THEN
        SELECT st.step_name
        INTO NEW.orchestrator_step_name
        FROM orchestrator.orchestrator_steps os
        JOIN orchestrator.step_types st ON os.step_type_id = st.id
        WHERE os.id = NEW.orchestrator_step_id;
    ELSE
        NEW.orchestrator_step_name := NULL;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Идемпотентное создание триггера
DROP TRIGGER IF EXISTS trg_llm_populate_step_name ON metrics.llm_internal;
CREATE TRIGGER trg_llm_populate_step_name
BEFORE INSERT ON metrics.llm_internal
FOR EACH ROW
EXECUTE FUNCTION metrics.populate_llm_step_name();

-- Добавляем ссылку FK на поле созданной ранее таблицы orchestrator_steps
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_steps_llm_metric'
          AND conrelid = 'orchestrator.orchestrator_steps'::regclass
    ) THEN
        ALTER TABLE orchestrator.orchestrator_steps
            ADD CONSTRAINT fk_steps_llm_metric
            FOREIGN KEY (llm_metric_id)
            REFERENCES metrics.llm_internal(id)
            ON DELETE SET NULL;
    END IF;
END $$;


-- 4.13 Создание таблицы диалогов
CREATE TABLE IF NOT EXISTS dialogs.dialogues (
    id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    session_id UUID NOT NULL REFERENCES dialogs.sessions(id) ON DELETE CASCADE,
    actor_id UUID NOT NULL REFERENCES users.actors(id) ON DELETE CASCADE,
    status dialog_status NOT NULL DEFAULT 'active',
    reason dialog_close_reason,
    start_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    end_at TIMESTAMPTZ,
    last_activity_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    agent_version TEXT NOT NULL
);

COMMENT ON TABLE dialogs.dialogues IS 'Таблица логических диалогов. Функционирует поверх физической сессии.';
COMMENT ON COLUMN dialogs.dialogues.actor_id IS 'ID пользователя (владельца). Используется для быстрого поиска без JOIN сессий.';
COMMENT ON COLUMN dialogs.dialogues.status IS 'Статус: active - диалог идёт, completed - завершён.';
COMMENT ON COLUMN dialogs.dialogues.last_activity_at IS 'Метка времени последнего сообщения. Используется для расчёта таймаута неактивности.';

-- Индексы для оптимизации
CREATE INDEX idx_dialogues_actor_status ON dialogs.dialogues (actor_id, status) WHERE status = 'active';
CREATE INDEX idx_dialogues_session_status ON dialogs.dialogues (session_id, status);
CREATE INDEX idx_dialogues_agent_version ON dialogs.dialogues (agent_version);
-- Триггер auto-update для last_activity_at (не нужен, обновляется явно в коде, 
-- но можно использовать общий триггер если потребуется)
-- В данной архитектуре last_activity_at обновляется явно при проверке таймаута для точности.


-- 4.14 Создание таблицы сообщений диалогов
CREATE TABLE IF NOT EXISTS dialogs.row_messages (
    id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    parent_message_id UUID REFERENCES dialogs.row_messages(id) ON DELETE SET NULL,
    actor_id UUID NOT NULL REFERENCES users.actors(id) ON DELETE RESTRICT,
    actor_type actor_type NOT NULL,
    responded_by_actor_id UUID REFERENCES users.actors(id) ON DELETE SET NULL,
    session_id UUID NOT NULL REFERENCES dialogs.sessions(id) ON DELETE RESTRICT,
    dialogue_id UUID REFERENCES dialogs.dialogues(id) ON DELETE RESTRICT,
    row_text TEXT NOT NULL, -- Чистое сообщение
    timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
    answer_latency FLOAT, -- общее время ответа (временем записи сообщения и временем parent_message_id)
    orchestrator_step_id UUID REFERENCES orchestrator.orchestrator_steps(id) ON DELETE RESTRICT,
    updated_at TIMESTAMPTZ,
    agent_version TEXT NOT NULL
);

-- Подробные комментарии к таблице
COMMENT ON TABLE dialogs.row_messages IS 'Таблица сырых сообщений диалогов. Содержит все сообщения пользователей и агента с полным 
контекстом и метаданными обработки.';

-- Комментарии к колонкам
COMMENT ON COLUMN dialogs.row_messages.id IS 'Уникальный идентификатор сообщения (UUID)';
COMMENT ON COLUMN dialogs.row_messages.parent_message_id IS 'Ссылка на родительское сообщение (для ответов, цепочек и тредов). Позволяет строить иерархию сообщений и измерять latency ответа.';
COMMENT ON COLUMN dialogs.row_messages.actor_id IS 'ID отправителя сообщения (пользователь или агент). Ссылка на таблицу users.actors.';
COMMENT ON COLUMN dialogs.row_messages.actor_type IS 'Тип отправителя: user - пользователь, system - агент, owner - владелец системы и т.д. Дублируется из actors для оптимизации запросов.';
COMMENT ON COLUMN dialogs.row_messages.responded_by_actor_id IS 'ID актора, которому отвечает агент (системное сообщение). Для сообщений пользователя — заполняется при генерации ответа. Для системных сообщений — всегда NULL.';
COMMENT ON COLUMN dialogs.row_messages.session_id IS 'ID сессии диалога, в рамках которой отправлено сообщение. Связывает сообщения в непрерывный диалог.';
COMMENT ON COLUMN dialogs.row_messages.dialogue_id IS 'ID диалога к которому относится текущее сообщение';
COMMENT ON COLUMN dialogs.row_messages.row_text IS 'Исходный текст сообщения в том виде, в котором он получен (с возможными ошибками, опечатками, сленгом)';
COMMENT ON COLUMN dialogs.row_messages.timestamp IS 'Метка времени отправки/получения сообщения';
COMMENT ON COLUMN dialogs.row_messages.answer_latency IS 'Общее время ответа в секундах. Рассчитывается как разница между timestamp текущего сообщения и timestamp родительского сообщения (parent_message_id). Для сообщений пользователя обычно NULL.';
COMMENT ON COLUMN dialogs.row_messages.orchestrator_step_id IS 'Ссылка на шаг оркестратора, обработавший сообщение. Позволяет отследить весь путь обработки.';
COMMENT ON COLUMN dialogs.row_messages.updated_at IS 'Дата и время последнего обновления строки';
COMMENT ON COLUMN dialogs.row_messages.agent_version IS 'Версия агента глобально из pyproject.toml, на момент создания/обновления записи';

-- Индексы для оптимизации запросов
CREATE INDEX IF NOT EXISTS idx_row_messages_parent_id ON dialogs.row_messages (parent_message_id);
CREATE INDEX IF NOT EXISTS idx_row_messages_actor_id ON dialogs.row_messages (actor_id);
CREATE INDEX IF NOT EXISTS idx_row_messages_actor_type ON dialogs.row_messages (actor_type);
CREATE INDEX IF NOT EXISTS idx_row_messages_responded_by_actor ON dialogs.row_messages(responded_by_actor_id) WHERE responded_by_actor_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_row_messages_session_id ON dialogs.row_messages (session_id);
CREATE INDEX IF NOT EXISTS idx_row_messages_timestamp ON dialogs.row_messages (timestamp);
CREATE INDEX IF NOT EXISTS idx_row_messages_agent_version ON dialogs.row_messages (agent_version);
CREATE INDEX IF NOT EXISTS idx_row_messages_orchestrator_step ON dialogs.row_messages (orchestrator_step_id);
CREATE INDEX IF NOT EXISTS idx_row_messages_dialogue_timestamp ON dialogs.row_messages (dialogue_id, timestamp ASC);

-- Индекс для поиска по тексту (если нужен полнотекстовый поиск)
CREATE INDEX IF NOT EXISTS idx_row_messages_row_text_search ON dialogs.row_messages USING gin(to_tsvector('russian', row_text));

-- Подключение триггера updatet_at
DROP TRIGGER IF EXISTS trg_row_messages_update_updated_at ON dialogs.row_messages;
CREATE TRIGGER trg_row_messages_update_updated_at
    BEFORE UPDATE ON dialogs.row_messages
    FOR EACH ROW
    EXECUTE FUNCTION common.update_updated_at_column();


-- 4.15 Создание таблицы артефактов LLM (полные промпты и ответы)
CREATE TABLE IF NOT EXISTS metrics.llm_artifacts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    llm_metric_id UUID NOT NULL REFERENCES metrics.llm_internal(id) ON DELETE CASCADE,
    orchestrator_step_id UUID REFERENCES orchestrator.orchestrator_steps(id) ON DELETE SET NULL,
    messages_json JSONB NOT NULL,
    raw_response TEXT,
    final_params JSONB,
    agent_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE metrics.llm_artifacts IS 'Полные текстовые артефакты LLM-запросов: массив messages, сырой ответ и финальные параметры.';
COMMENT ON COLUMN metrics.llm_artifacts.messages_json IS 'Полный массив messages [{role, content}, ...] ушедший в LLM.';
COMMENT ON COLUMN metrics.llm_artifacts.raw_response IS 'Сырой текстовый ответ модели (content).';
COMMENT ON COLUMN metrics.llm_artifacts.final_params IS 'Фактически использованные параметры генерации.';

CREATE INDEX idx_llm_artifacts_metric ON metrics.llm_artifacts (llm_metric_id);
CREATE INDEX idx_llm_artifacts_created ON metrics.llm_artifacts (created_at DESC);


-- 4.16 Создание таблицы настроек агента
CREATE TABLE IF NOT EXISTS state.settings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    param_name TEXT NOT NULL UNIQUE,
    description TEXT,
    value_float REAL,
    value_text TEXT,
    value_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE state.settings IS 'Все настройки агента: оркестратор, состояния и т.д.';
COMMENT ON COLUMN state.settings.param_name IS 'Уникальное имя параметра (например, orchestrator_pulse_seconds, inactivity_sleep_minutes).';
COMMENT ON COLUMN state.settings.description IS 'Человекочитаемое описание.';
COMMENT ON COLUMN state.settings.value_float IS 'Числовое значение (float/int).';
COMMENT ON COLUMN state.settings.value_text IS 'Строковое значение (если нужно).';
COMMENT ON COLUMN state.settings.value_json IS 'JSONB для массивов, матриц, объектов (например, матрица omega).';
COMMENT ON COLUMN state.settings.created_at IS 'Дата создания записи.';
COMMENT ON COLUMN state.settings.updated_at IS 'Дата последнего обновления (автоматически через триггер).';

-- Индекс по имени
CREATE INDEX IF NOT EXISTS idx_state_settings_param_name ON state.settings (param_name);

-- Триггер автообновления updated_at
CREATE OR REPLACE FUNCTION state.update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_state_settings_update_updated_at ON state.settings;
CREATE TRIGGER trg_state_settings_update_updated_at
    BEFORE UPDATE ON state.settings
    FOR EACH ROW
    EXECUTE FUNCTION state.update_updated_at_column();

-- =============================================
-- Начальные значения
-- =============================================
INSERT INTO state.settings (param_name, value_float, description) VALUES
('orchestrator_pulse_seconds', 1.0, 'Интервал между проверками очереди задач оркестратора (секунды). Аналог пульса. Значение по умолчанию: 1.'),
('inactivity_sleep_minutes', 10.0,  'Число минут без сообщений пользователей, после которых агент переходит из active в sleep.'),
('dialogue_inactivity_timeout_minutes', 20.0, 'Таймаут неактивности диалога в минутах. Если last_activity_at старше порога, диалог закрывается и создаётся новый.')
ON CONFLICT (param_name) DO NOTHING;


-- 4.17 Создание таблицы причин выключений и простоев (фактические события с таймингами)
CREATE TABLE IF NOT EXISTS state.shutdown_reasons (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    actor_id UUID NOT NULL REFERENCES users.actors(id) ON DELETE CASCADE,
    shutdown_type state.shutdown_type NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE state.shutdown_reasons IS 'Факты выключений/простоев. Заполняется при выключении или при старте после креша.';
COMMENT ON COLUMN state.shutdown_reasons.id IS 'UUID';
COMMENT ON COLUMN state.shutdown_reasons.actor_id IS 'Пользователь, чьё отсутствие или действие вызвало отключение';
COMMENT ON COLUMN state.shutdown_reasons.shutdown_type IS 'Тип выключения (ENUM)';
COMMENT ON COLUMN state.shutdown_reasons.timestamp IS 'Метка времени создания записи метрики';

CREATE INDEX idx_shutdown_timestamp ON state.shutdown_reasons (timestamp);
CREATE INDEX idx_shutdown_actor ON state.shutdown_reasons (actor_id);

-- 4.18 Создание таблицы глобального жизненного цикла агента
CREATE TABLE IF NOT EXISTS state.agent_lifecycle (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    actor_id UUID NOT NULL REFERENCES users.actors(id) ON DELETE CASCADE,
    state_type state.agent_state_type NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at TIMESTAMPTZ,
    reason_change state.lifecycle_change_reason NOT NULL,
    shutdown_reason_id UUID NULL REFERENCES state.shutdown_reasons(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    agent_version TEXT NOT NULL DEFAULT 'unknown'
);

COMMENT ON TABLE state.agent_lifecycle IS 'История состояний off/sleep/active.';
COMMENT ON COLUMN state.agent_lifecycle.id IS 'UUID';
COMMENT ON COLUMN state.agent_lifecycle.actor_id IS 'Пользователь, чьё действие вызвало изменение состояния.';
COMMENT ON COLUMN state.agent_lifecycle.state_type IS 'Состояние: off, sleep, active';
COMMENT ON COLUMN state.agent_lifecycle.started_at IS 'Время начала состояния';
COMMENT ON COLUMN state.agent_lifecycle.ended_at IS 'Время окончания состояния (NULL – текущее)';
COMMENT ON COLUMN state.agent_lifecycle.reason_change IS 'Причина перехода (ENUM)';
COMMENT ON COLUMN state.agent_lifecycle.shutdown_reason_id IS 'Ссылка на shutdown_reasons (только для off)';
COMMENT ON COLUMN state.agent_lifecycle.created_at IS 'Дата создания записи';
COMMENT ON COLUMN state.agent_lifecycle.updated_at IS 'Дата обновления (триггер)';
COMMENT ON COLUMN state.agent_lifecycle.agent_version IS 'Версия агента, под которой было начато данное состояние жизненного цикла.';

CREATE UNIQUE INDEX lifecycle_active_global ON state.agent_lifecycle ((true)) WHERE ended_at IS NULL;
COMMENT ON INDEX state.lifecycle_active_global IS 'Гарантирует единственную активную запись lifecycle глобально (ended_at IS NULL)';
CREATE INDEX idx_lifecycle_started ON state.agent_lifecycle (started_at DESC);

DROP TRIGGER IF EXISTS trigger_agent_lifecycle_updated_at ON state.agent_lifecycle;
CREATE TRIGGER trigger_agent_lifecycle_updated_at
    BEFORE UPDATE ON state.agent_lifecycle
    FOR EACH ROW
    EXECUTE FUNCTION common.update_updated_at_column();
