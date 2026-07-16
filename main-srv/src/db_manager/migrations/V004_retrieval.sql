-- =============================================
-- Migration: V004_retrieval.sql
-- Version: V004
-- Description: Подсистема преданализа запроса и выборки знаний из графа.
--              Добавляет колонки routing_context и retrieved_context в сообщения,
--              журнал выборки memory.retrieval_logs, типы задач/шагов и промпт роутинга.
-- =============================================

-- === БЛОК 1: Расширение таблицы сообщений ===
ALTER TABLE dialogs.row_messages
ADD COLUMN IF NOT EXISTS routing_context JSONB,
ADD COLUMN IF NOT EXISTS retrieved_context JSONB,
ADD COLUMN IF NOT EXISTS retrieval_log_id UUID;

COMMENT ON COLUMN dialogs.row_messages.routing_context IS
'Результат предразбора вопроса (question_routing): {domains: [code], topics: [uuid], raw_response: str, confidence: float}. Заполняется шагом question_routing.';
COMMENT ON COLUMN dialogs.row_messages.retrieved_context IS
'Собранный текстовый контекст из графа знаний для инъекции в финальный ответ (итоговый raw_text + массив node_ids). Заполняется шагом knowledge_retrieval.';
COMMENT ON COLUMN dialogs.row_messages.retrieval_log_id IS
'UUID записи полной трассировки выборки из memory.retrieval_logs. Ссылка для проваливания в детали.';

CREATE INDEX IF NOT EXISTS idx_row_messages_routing_context
ON dialogs.row_messages USING gin (routing_context)
WHERE routing_context IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_row_messages_retrieved_context
ON dialogs.row_messages USING gin (retrieved_context)
WHERE retrieved_context IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_row_messages_retrieval_log_id
ON dialogs.row_messages (retrieval_log_id)
WHERE retrieval_log_id IS NOT NULL;

-- === БЛОК 2: Журнал выборки знаний (полная трассировка) ===
CREATE TABLE IF NOT EXISTS memory.retrieval_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    message_id UUID NOT NULL REFERENCES dialogs.row_messages(id) ON DELETE CASCADE,
    orchestrator_step_id UUID REFERENCES orchestrator.orchestrator_steps(id) ON DELETE SET NULL,
    routing_context JSONB,
    strategy TEXT NOT NULL,
    filter_domains TEXT[],
    filter_topics UUID[],
    node_ids UUID[] NOT NULL DEFAULT '{}'::UUID[],
    edge_ids UUID[] NOT NULL DEFAULT '{}'::UUID[],
    nodes_count INT NOT NULL DEFAULT 0,
    edges_count INT NOT NULL DEFAULT 0,
    raw_content TEXT,
    total_tokens INT NOT NULL DEFAULT 0,
    trimmed BOOLEAN NOT NULL DEFAULT FALSE,
    avg_confidence REAL,
    latency FLOAT,
    error_message TEXT,
    agent_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Внешний ключ не делаем (избегаем циклических зависимостей при INSERT в обратном порядке),
-- связь логическая через retrieval_log_id в row_messages.

COMMENT ON TABLE memory.retrieval_logs IS
'Журнал выборки знаний из графа для ответов. Связан с сообщением и шагом оркестратора для сквозной трассировки.';
COMMENT ON COLUMN memory.retrieval_logs.message_id IS 'UUID сообщения-триггера выборки.';
COMMENT ON COLUMN memory.retrieval_logs.orchestrator_step_id IS 'UUID шага knowledge_retrieval.';
COMMENT ON COLUMN memory.retrieval_logs.routing_context IS 'Снимок routing_context на момент выборки (для воспроизводимости).';
COMMENT ON COLUMN memory.retrieval_logs.strategy IS 'Использованная стратегия: hybrid, vector_only, graph_only, fallback.';
COMMENT ON COLUMN memory.retrieval_logs.filter_domains IS 'Коды доменов, по которым фильтровался Qdrant.';
COMMENT ON COLUMN memory.retrieval_logs.filter_topics IS 'UUID тем, по которым фильтровался Qdrant.';
COMMENT ON COLUMN memory.retrieval_logs.node_ids IS 'Массив UUID узлов, вошедших в итоговый контекст.';
COMMENT ON COLUMN memory.retrieval_logs.edge_ids IS 'Массив UUID рёбер, использованных для расширения контекста.';
COMMENT ON COLUMN memory.retrieval_logs.raw_content IS 'Сырой собранный текст (то, что реально попало в промпт).';
COMMENT ON COLUMN memory.retrieval_logs.total_tokens IS 'Финальное количество токенов после подрезки.';
COMMENT ON COLUMN memory.retrieval_logs.trimmed IS 'Флаг, что контекст был обрезан из-за превышения MAX_CONTEXT_TOKENS.';
COMMENT ON COLUMN memory.retrieval_logs.avg_confidence IS 'Средняя уверенность узлов в выборке.';
COMMENT ON COLUMN memory.retrieval_logs.latency IS 'Время выполнения выборки (сек).';
COMMENT ON COLUMN memory.retrieval_logs.error_message IS 'Текст ошибки, если выборка упала (graceful degradation).';

CREATE INDEX idx_retrieval_logs_message ON memory.retrieval_logs (message_id);
CREATE INDEX idx_retrieval_logs_step ON memory.retrieval_logs (orchestrator_step_id);
CREATE INDEX idx_retrieval_logs_created ON memory.retrieval_logs (created_at DESC);
CREATE INDEX idx_retrieval_logs_strategy ON memory.retrieval_logs (strategy);
CREATE INDEX idx_retrieval_logs_trimmed ON memory.retrieval_logs (trimmed) WHERE trimmed = TRUE;
CREATE INDEX idx_retrieval_logs_nodes ON memory.retrieval_logs USING gin (node_ids);

-- === БЛОК 3: Новые типы задач и шагов оркестратора ===
INSERT INTO orchestrator.task_types (type_name, description)
VALUES ('question_preprocessing', 'Преданализ вопроса пользователя: роутинг доменов и выборка знаний')
ON CONFLICT (type_name) DO NOTHING;

INSERT INTO orchestrator.step_types (step_name, description, agent_version) VALUES
('question_routing',    'LLM-предразбор вопроса: определение релевантных доменов и тем', '1.4.0'),
('knowledge_retrieval', 'Поиск и сборка контекста из графа знаний по результатам роутинга', '1.4.0')
ON CONFLICT (step_name) DO NOTHING;

-- === БЛОК 4: Промпт роутинга (компактный, без CoT, JSON) ===
DO $$
DECLARE
    v_dest UUID;
    v_owner UUID;
BEGIN
    SELECT id INTO v_dest FROM orchestrator.prompt_destinations WHERE name = 'internal_logic' LIMIT 1;
    SELECT id INTO v_owner FROM users.actors WHERE type = 'owner' LIMIT 1;

    INSERT INTO orchestrator.prompts (
        version, name, description, type, destination_id, text, params,
        prompt_effectiveness, status, created_by, agent_version, created_at
    ) VALUES (
        '1.1.0',
        'question_domain_router',
        'Предразбор вопроса пользователя: выбор релевантных доменов и тем (без CoT, JSON)',
        'internal'::public.prompt_type,
        v_dest,
        E'<Правила>\n' ||
        E'Ты — модуль маршрутизации вопросов пользователя.\n' ||
        E'Твоя задача — определить, в каких ДОМЕНАХ базы знаний искать информацию.\n\n' ||
        E'=== ВХОДНЫЕ ДАННЫЕ ===\n' ||
        E'Вопрос пользователя: "{{question}}"\n\n' ||
        E'Доступные Domains (code | название | описание):\n{{domains_list}}\n\n' ||
        E'=== КРИТИЧЕСКИЕ ПРАВИЛА ===\n' ||
        E'1. СТРОГОСТЬ: Используй ТОЛЬКО codes доменов, переданные выше.\n' ||
        E'2. ОБЯЗАТЕЛЬНЫЕ ДОМЕНЫ:\n' ||
        E'   - Если вопрос касается личности, привычек, фактов о пользователе → ДОБАВЬ "user_profile".\n' ||
        E'   - Если вопрос касается внешнего мира, новостей, объективных фактов → ДОБАВЬ "world_state" (или general).\n' ||
        E'3. СОМНЕНИЯ: Не уверена в домене → не включай его. Лучше пропустить, чем фантазировать.\n\n' ||
        E'=== ФОРМАТ ОТВЕТА ===\n' ||
        E'Отвечай СТРОГО только JSON-объектом. Без markdown-разметки:\n' ||
        E'{"domains": ["code1", "code2"], "confidence": 0.9}\n' ||
        E'</Правила>',
        '{
            "model_name": "Qwen3.5-9B-Q4_K_M.gguf",
            "temperature": 0.7,
            "top_p": 0.85,
            "top_k": 20,
            "min_p": 0.0,
            "max_tokens": 4096,
            "presence_penalty": 0.0,
            "repetition_penalty": 1.0,
            "stop": ["<|im_end|>"],
            "chat_template_kwargs": {"enable_thinking": false}
        }'::jsonb,
        '{}'::jsonb,
        'testing'::public.prompt_status,
        v_owner,
        '1.4.0',
        now()
    ) ON CONFLICT (name, version) DO NOTHING;
END $$;

-- === БЛОК 5: Декомпозиция сложных запросов ===

-- Новый тип шага
INSERT INTO orchestrator.step_types (step_name, description, agent_version)
VALUES ('query_decomposition', 'LLM-декомпозиция сложного запроса на независимые подвопросы', '1.4.0')
ON CONFLICT (step_name) DO NOTHING;

-- Колонка для хранения подвопросов в журнале выборки
ALTER TABLE memory.retrieval_logs
ADD COLUMN IF NOT EXISTS sub_queries TEXT[] DEFAULT NULL;
COMMENT ON COLUMN memory.retrieval_logs.sub_queries IS
'Массив подвопросов после декомпозиции. NULL = декомпозиция не применялась (короткий запрос).';

-- Промпт декомпозиции
DO $$
DECLARE
    v_dest UUID;
    v_owner UUID;
BEGIN
    SELECT id INTO v_dest FROM orchestrator.prompt_destinations WHERE name = 'internal_logic' LIMIT 1;
    SELECT id INTO v_owner FROM users.actors WHERE type = 'owner' LIMIT 1;

    INSERT INTO orchestrator.prompts (
        version, name, description, type, destination_id, text, params,
        prompt_effectiveness, status, created_by, agent_version, created_at
    ) VALUES (
        '1.1.0',
        'query_decomposer',
        'Декомпозиция сложного запроса пользователя на независимые подвопросы для параллельного поиска в базе знаний',
        'internal'::public.prompt_type,
        v_dest,
        E'<Правила>\n' ||
        E'Ты — модуль декомпозиции сложных запросов.\n\n' ||
        E'ВХОД:\n' ||
        E'Запрос пользователя: "{{question}}"\n\n' ||
        E'ЗАДАЧА:\n' ||
        E'Разбей сложный составной запрос на несколько независимых подвопросов для поиска в базе знаний.\n' ||
        E'Каждый подвопрос должен быть:\n' ||
        E'1. Самодостаточным — понятным без контекста исходного запроса.\n' ||
        E'2. Сфокусированным на одной теме/сущности, естественно относящейся к одному из разделов базы (профиль пользователя, финансы, здоровье, общие факты, работа, спорт, безопасность, питание и т.п.).\n' ||
        E'3. Сформулированным как поисковый запрос (утверждение-тема, ключевые слова).\n\n' ||
        E'ПРАВИЛА:\n' ||
        E'1. НЕ дублируй смысловые части; объединяй близкие темы в один подвопрос.\n' ||
        E'2. Сохраняй конкретные сущности (имена, модели, места, даты, события).\n' ||
        E'3. Если запрос простой (одна тема) — верни массив из одного элемента, близкого к исходному запросу.\n\n' ||
        E'ПРИМЕРЫ:\n' ||
        E'Вход: "Какой расход топлива у моего Лансера 9 в городе и сколько денег ушло на бензин в прошлом месяце?"\n' ||
        E'Ответ: ["расход топлива Mitsubishi Lancer 9 городской цикл", "затраты на бензин за прошлый месяц"]\n\n' ||
        E'Вход: "Расскажи про расход Lancer 9 в городе, как подготовиться к поездке в деревню, и какая обстановка с топливом в России"\n' ||
        E'Ответ: ["расход топлива Mitsubishi Lancer 9 в городе", "подготовка автомобиля к поездке в деревню", "дефицит топлива в России обстановка"]\n\n' ||
        E'Вход: "Какие у меня цели на неделю и что говорят врачи о пользе бега"\n' ||
        E'Ответ: ["цели пользователя на неделю", "мнение врачей о пользе бега для здоровья"]\n\n' ||
        E'ОТВЕТ СТРОГО JSON-массивом без markdown:\n' ||
        E'["подвопрос1", "подвопрос2", "подвопрос3"]\n' ||
        E'</Правила>',
        '{
            "model_name": "Qwen3.5-9B-Q4_K_M.gguf",
            "temperature": 0.7,
            "top_p": 0.85,
            "top_k": 20,
            "min_p": 0.0,
            "max_tokens": 4096,
            "presence_penalty": 0.0,
            "repetition_penalty": 1.0,
            "stop": ["<|im_end|>"],
            "chat_template_kwargs": {"enable_thinking": false}
        }'::jsonb,
        '{}'::jsonb,
        'testing'::public.prompt_status,
        v_owner,
        '1.4.0',
        now()
    ) ON CONFLICT (name, version) DO NOTHING;
END $$;