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
COMMENT ON COLUMN memory.verification_sessions.status IS 'Статус сессии: active, completed, deferred';
COMMENT ON COLUMN memory.verification_sessions.hypotheses_total IS 'Общее количество гипотез в сессии';
COMMENT ON COLUMN memory.verification_sessions.proposal_task_id IS 'UUID задачи orchestrator_tasks, к которой привязана сессия (verification_proposal)';
COMMENT ON COLUMN memory.verification_sessions.hypothesis_ids IS 'Список ID hypothesis (snapshot на момент уведомления) — какие факты рассматривала эта сессия';
COMMENT ON COLUMN memory.verification_sessions.deferred_until IS 'Время, до которого сессия отложена';

CREATE TABLE IF NOT EXISTS memory.verification_actions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID NOT NULL REFERENCES memory.verification_sessions(id) ON DELETE CASCADE,
    hypothesis_id UUID NOT NULL REFERENCES memory.hypotheses(id) ON DELETE CASCADE,
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

-- Блок 3: Типы задач и шагов оркестратора
INSERT INTO orchestrator.task_types (type_name, description) VALUES
    ('verification_proposal', 'Проверка необходимости и инициация предложения верификации'),
    ('hypothesis_refinement', 'LLM-уточнение гипотезы по комментарию пользователя')
ON CONFLICT (type_name) DO NOTHING;

INSERT INTO orchestrator.step_types (step_name, description, agent_version) VALUES
    ('verification_proposal', 'Анализ наличия draft-гипотез и отправка NOTIFY в UI', '1.2.0'),
    ('hypothesis_refinement', 'LLM-уточнение гипотезы на основе комментария', '1.2.0')
ON CONFLICT (step_name) DO NOTHING;

-- Блок 4: Промпт для LLM-уточнения гипотез
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
            E'Тебе дана исходная гипотеза, извлечённая из диалога, контекст для понимания диалога и комментарий пользователя.\n' ||
            E'Твоя задача — переформулировать гипотезу с учётом комментария пользователя.\n\n' ||
            E'Задача:\n' ||
            E'1. Исправь ошибки, на которые указал пользователь и дополни с учетом его замечаний.\n' ||
            E'2. Исправь орфографию и пунктуацию если необходимо.\n' ||
            E'3. Добавь уточнения, если они появились у пользователя.\n' ||
            E'4. Ошибочные элементы на которые указал пользователь удалять без оговорок (не пиши "информации об X нет" — просто убери X).\n' ||
            E'5. Не меняй суть гипотезы, если пользователь не просит.\n' ||
            E'6. НЕ дополняй гипотезу текстом из контекста, он дан тебе в помощь ТОЛЬКО для понимания сути диалога.\n' ||
            E'7. Итоговая гипотеза должна быть связной, непротиворечивой.\n' ||
            E'8. Выведи исправленный текст гипотезы.\n\n' ||
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
             "max_tokens": 15000,
             "presence_penalty": 0.0,
             "repetition_penalty": 1.0,
             "stop": ["<|im_end|>"],
             "chat_template_kwargs": {"enable_thinking": false}
            }'::jsonb,
            '{}'::jsonb, 'testing'::public.prompt_status, v_creator_id, '1.2.0', now()
        );
    END IF;
END $$;


-- Блок 5: Промпт для классификатора тем гипотез
DO $$
DECLARE
    v_destination_id UUID;
    v_creator_id UUID;
    v_prompt_name TEXT := 'hypothesis_topic_classifier';
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
            'Классификатор гипотез по строгому справочнику тем. Возвращает JSON-маппинг ID гипотезы к ID темы.',
            'internal'::public.prompt_type, v_destination_id,
            E'<Правила>\n' ||
            E'Ты — модуль классификации фактов (гипотез) базы знаний агента.\n' ||
            E'Тебе дан список гипотез (с их ID, текстом, доменом и источником) и СТРОГИЙ справочник тем.\n\n' ||
            E'Твоя задача:\n' ||
            E'1. Выбрать для гипотез НАИБОЛЕЕ релевантные темы из справочника.\n' ||
            E'2. Используй подсказки "Домен" и "Источник" — они помогают понять контекст гипотезы.\n' ||
            E'3. Назначай тему только при полной смысловой уверенности. Если есть малейшее сомнение или тема подходит лишь частично — ставь null. Лучше пропустить тему, чем привязать неверно.\n' ||
            E'4. ЗАПРЕЩЕНО выдумывать новые темы или изменять названия существующих. Используй ТОЧНЫЕ названия из справочника.\n' ||
            E'5. Ответ должен быть СТРОГО в формате JSON-массива без markdown-обёртки.\n\n' ||
            E'<Справочник тем>\n' ||
            E'{topics_list}\n' ||
            E'</Справочник тем>\n\n' ||
            E'<Гипотезы для классификации>\n' ||
            E'{hypotheses_list}\n' ||
            E'</Гипотезы для классификации>\n\n' ||
            E'<Формат ответа (строго JSON)>\n' ||
            E'[\n' ||
            E'  {"hypothesis_id": "uuid1...", "topic_name": "название темы"},\n' ||
            E'  {"hypothesis_id": "uuid2...", "topic_name": null}\n' ||
            E']\n' ||
            E'</Формат ответа>\n</Правила>\n',
            '{
              "model_name": "Qwen3.5-9B-Q4_K_M.gguf",
              "temperature": 0.7,
              "top_p": 0.8,
              "top_k": 20,
              "min_p": 0.0,
              "max_tokens": 20000,
              "presence_penalty": 0.0,
              "repetition_penalty": 1.0,
              "stop": ["<|im_end|>"],
              "chat_template_kwargs": {"enable_thinking": false}
            }'::jsonb,
            '{}'::jsonb, 'testing'::public.prompt_status, v_creator_id, '1.3.0', now()
        );
    END IF;
END $$;

-- Блок 6: Промпт для классификатора forms гипотез
DO $$
DECLARE
v_destination_id UUID;
v_creator_id UUID;
v_prompt_name TEXT := 'hypothesis_form_classifier';
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
        'Классификатор гипотез по строгому справочнику форм сущностей. Возвращает JSON-маппинг ID гипотезы к code формы.',
        'internal'::public.prompt_type, v_destination_id,
        E'<Правила>\n' ||
        E'Ты — модуль классификации формы (природы) фактов базы знаний агента.\n' ||
        E'Тебе дан список гипотез (с их ID, текстом, доменом и источником) и СТРОГИЙ справочник форм сущностей.\n\n' ||
        E'Твоя задача:\n' ||
        E'1. Выбрать для гипотез НАИБОЛЕЕ релевантную форму из справочника.\n' ||
        E'2. Форма описывает ПРИРОДУ факта: чем является гипотиза по своей сути (факт, цель, задача, проект, сущность, навык, событие), независимо от домена.\n' ||
        E'3. Если гипотеза содержит рекомендацию, предложение, совет или план действий – это НЕ факт. Это либо цель (goal), либо задача (task), либо проект (project), в зависимости от наличия сроков и объёма. Факт – только констатация текущего состояния или свойства (например, "у меня есть", "составляет", "проживает").\n' ||
        E'4. Назначай форму только при полной смысловой уверенности. Если есть сомнение — ставь null. Лучше пропустить, чем назначить неверно.\n' ||
        E'5. ЗАПРЕЩЕНО выдумывать новые формы или изменять коды существующих. Используй ТОЧНЫЕ code из справочника.\n' ||
        E'6. Указывай точные uuid гипотез!\n' ||
        E'7. Ответ должен быть СТРОГО в формате JSON-массива без markdown-обёртки.\n' ||
        E'<Примеры классификации>\n' ||
        E'Пример 1:\n' ||
        E'Гипотеза: "Подготовить отчёт по продажам к 18:00 сегодня"\n' ||
        E'Форма: task (конкретное действие с дедлайном)\n' ||
        E'Пример 2:\n' ||
        E'Гипотеза: "Разобраться в тонкостях налогового законодательства"\n' ||
        E'Форма: null (неопределённость – может быть goal, skill или task, явных признаков нет)\n' ||
        E'</Примеры классификации>\n\n' ||
        E'<Справочник форм>\n' ||
        E'{forms_list}\n' ||
        E'</Справочник форм>\n\n' ||
        E'<Гипотезы для классификации>\n' ||
        E'{hypotheses_list}\n' ||
        E'</Гипотезы для классификации>\n\n' ||
        E'<Формат ответа (строго JSON)>\n' ||
        E'[\n' ||
        E'  {"hypothesis_id": "uuid1...", "form_code": "форма"},\n' ||
        E'  {"hypothesis_id": "uuid2...", "form_code": null}\n' ||
        E']\n' ||
        E'</Формат ответа>\n</Правила>\n',
        '{
          "model_name": "Qwen3.5-9B-Q4_K_M.gguf",
          "temperature": 0.7,
          "top_p": 0.8,
          "top_k": 20,
          "min_p": 0.0,
          "max_tokens": 20000,
          "presence_penalty": 0.0,
          "repetition_penalty": 1.0,
          "stop": ["<|im_end|>"],
          "chat_template_kwargs": {"enable_thinking": false}
        }'::jsonb,
        '{}'::jsonb, 'testing'::public.prompt_status, v_creator_id, '1.4.0', now()
    );
END IF;
END $$;