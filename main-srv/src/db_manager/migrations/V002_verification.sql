-- =============================================
-- Migration: V002_verification.sql
-- Version: V002
-- Description: Подсистема верификации гипотез.
-- =============================================

-- Блок 1: ENUM типы
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'memory.verification_action_type') THEN
        CREATE TYPE memory.verification_action_type AS ENUM (
            'confirmed', 'rejected', 'edited', 'skipped'
        );
    END IF;
END $$;
COMMENT ON TYPE memory.verification_action_type IS 'Тип действия пользователя при верификации гипотезы';

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'memory.verification_session_status') THEN
        CREATE TYPE memory.verification_session_status AS ENUM ('active', 'completed', 'deferred');
    END IF;
END $$;
COMMENT ON TYPE memory.verification_session_status IS 'Статус сессии верификации';

-- Блок 2: Таблицы
CREATE TABLE IF NOT EXISTS memory.verification_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    actor_id UUID NOT NULL REFERENCES users.actors(id) ON DELETE CASCADE,
    status memory.verification_session_status NOT NULL DEFAULT 'active',
    hypotheses_total INT NOT NULL DEFAULT 0,
    hypotheses_confirmed INT NOT NULL DEFAULT 0,
    hypotheses_rejected INT NOT NULL DEFAULT 0,
    hypotheses_edited INT NOT NULL DEFAULT 0,
    hypotheses_skipped INT NOT NULL DEFAULT 0,
    proposal_task_id UUID REFERENCES orchestrator.orchestrator_tasks(id) ON DELETE SET NULL,
    hypothesis_ids UUID[] DEFAULT '{}'::UUID[] NOT NULL,
    deferred_until TIMESTAMPTZ,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at TIMESTAMPTZ
);
COMMENT ON TABLE memory.verification_sessions IS 'Сессии ручной верификации гипотез пользователем';
COMMENT ON COLUMN memory.verification_sessions.id IS 'Уникальный идентификатор сессии';
COMMENT ON COLUMN memory.verification_sessions.actor_id IS 'ID пользователя, с которым проводится верификация';
COMMENT ON COLUMN memory.verification_sessions.status IS 'Статус сессии: active, completed, deferred';
COMMENT ON COLUMN memory.verification_sessions.hypotheses_total IS 'Общее количество гипотез в сессии';
COMMENT ON COLUMN memory.verification_sessions.proposal_task_id IS 'UUID задачи orchestrator_tasks, к которой привязана сессия (verification_proposal)';
COMMENT ON COLUMN memory.verification_sessions.hypothesis_ids IS 'Список ID hypothesis (snapshot на момент уведомления) — какие факты рассматривала эта сессия';
COMMENT ON COLUMN memory.verification_sessions.deferred_until IS 'Время, до которого сессия отложена';

CREATE TABLE IF NOT EXISTS memory.verification_actions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID NOT NULL REFERENCES memory.verification_sessions(id) ON DELETE CASCADE,
    hypothesis_id UUID NOT NULL REFERENCES memory.hypotheses(id) ON DELETE CASCADE,
    actor_id UUID NOT NULL REFERENCES users.actors(id),
    action_type memory.verification_action_type NOT NULL,
    original_text TEXT,
    updated_text TEXT,
    user_comment TEXT,
    orchestrator_step_id UUID REFERENCES orchestrator.orchestrator_steps(id) ON DELETE SET NULL,
    prompt_id UUID REFERENCES orchestrator.prompts(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE memory.verification_actions IS 'Лог каждого действия пользователя при верификации';
COMMENT ON COLUMN memory.verification_actions.original_text IS 'Исходный текст гипотезы (до редактирования)';
COMMENT ON COLUMN memory.verification_actions.updated_text IS 'Уточнённый текст гипотезы (после LLM)';
COMMENT ON COLUMN memory.verification_actions.user_comment IS 'Комментарий пользователя при редактировании';

-- Блок 3: Настройки
INSERT INTO state.settings (param_name, value_float, description) VALUES
    ('verification_defer_minutes', 30.0, 'Минуты, на которые откладывается верификация при выборе "отложить"')
ON CONFLICT (param_name) DO NOTHING;

-- Блок 4: Типы задач и шагов оркестратора
INSERT INTO orchestrator.task_types (type_name, description) VALUES
    ('verification_proposal', 'Проверка необходимости и инициация предложения верификации'),
    ('hypothesis_refinement', 'LLM-уточнение гипотезы по комментарию пользователя')
ON CONFLICT (type_name) DO NOTHING;

INSERT INTO orchestrator.step_types (step_name, description, agent_version) VALUES
    ('verification_proposal', 'Анализ наличия draft-гипотез и отправка NOTIFY в UI', '1.2.0'),
    ('hypothesis_refinement', 'LLM-уточнение гипотезы на основе комментария', '1.2.0')
ON CONFLICT (step_name) DO NOTHING;

-- Блок 5: Промпт для LLM-уточнения гипотез
DO $$
DECLARE
    v_destination_id UUID;
    v_creator_id UUID;
    v_prompt_name TEXT := 'hypothesis_refinement';
    v_prompt_version TEXT := '1.1.0';
BEGIN
    SELECT id INTO v_destination_id FROM orchestrator.prompt_destinations WHERE name = 'internal_logic' LIMIT 1;
    SELECT id INTO v_creator_id FROM users.actors WHERE type = 'owner' LIMIT 1;

    IF NOT EXISTS (SELECT 1 FROM orchestrator.prompts WHERE name = v_prompt_name AND version = v_prompt_version) THEN
        INSERT INTO orchestrator.prompts (
            version, name, description, type, destination_id, text, params,
            prompt_effectiveness, status, created_by, agent_version, created_at
        ) VALUES (
            v_prompt_version, v_prompt_name,
            'Промпт для LLM-уточнения гипотезы на основе комментария пользователя',
            'internal'::public.prompt_type, v_destination_id,
            E'<Правила>\nТы — модуль уточнения гипотез интеллектуального агента.\n' ||
            E'Тебе дана исходная гипотеза, извлечённая из диалога, и комментарий пользователя.\n' ||
            E'Твоя задача — переформулировать гипотезу с учётом комментария.\n\n' ||
            E'Правила:\n' ||
            E'1. Исправь фактические ошибки, на которые указал пользователь. Его исправления имеют наивысший приоритет.\n' ||
            E'2. Добавь уточнения, если они есть.\n' ||
            E'3. Удали ошибочные элементы без оговорок (не пиши "информации об X нет" — просто убери X).\n' ||
            E'4. Не меняй суть гипотезы, если пользователь не просит.\n' ||
            E'5. Итоговая гипотеза должна быть связной, непротиворечивой.\n' ||
            E'6. Выведи только исправленный текст гипотезы.\n\n' ||
            E'<Исходная гипотеза>\n{{hypothesis_text}}\n\n' ||
            E'<Контекст (источник)>\n{{source_context}}\n\n' ||
            E'<Комментарий пользователя>\n{{user_comment}}\n\n' ||
            E'<Требования>\n' ||
            E'- Ответь ТОЛЬКО уточнённым текстом гипотезы, без пояснений.\n' ||
            E'- Язык ответа: русский.\n</Правила>\n',
            '{
             "model_name": "Qwen3.5-9B-Q4_K_M.gguf",
             "temperature": 0.7,
             "top_p": 0.8,
             "top_k": 20,
             "min_p": 0.0,
             "max_tokens": 32768,
             "presence_penalty": 1.5,
             "repetition_penalty": 1.0,
             "stop": ["<|im_end|>"],
             "chat_template_kwargs": {"enable_thinking": false}
            }'::jsonb,
            '{}'::jsonb, 'testing'::public.prompt_status, v_creator_id, '1.2.0', now()
        );
    END IF;
END $$;