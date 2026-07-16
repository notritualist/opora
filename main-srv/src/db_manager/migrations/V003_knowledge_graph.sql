-- =============================================
-- Migration: V003_knowledge_graph.sql
-- Version: V003
-- Description: Подсистема псевдографа памяти. Домен → Тема → Узел.
-- Строго внутри тем. Справочник связей = типы рёбер.
-- Разделение статусов интеграции через ENUM.
-- =============================================

-- 2. Узлы графа
CREATE TABLE IF NOT EXISTS memory.graph_nodes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    actor_id UUID REFERENCES users.actors(id) ON DELETE CASCADE,
    domain_id UUID REFERENCES memory.knowledge_domains(id) ON DELETE RESTRICT,
    topic_id UUID REFERENCES memory.topics(id) ON DELETE SET NULL,
    form_code TEXT REFERENCES memory.forms(code) ON DELETE SET NULL,
    description TEXT,
    summary TEXT,
    needs_summary_update BOOLEAN DEFAULT FALSE,
    needs_entity_binding BOOLEAN NOT NULL DEFAULT FALSE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    context_date DATE,
    needs_review BOOLEAN NOT NULL DEFAULT FALSE,
    source_hypothesis_ids UUID[] NOT NULL DEFAULT '{}'::UUID[],
    confidence REAL NOT NULL DEFAULT 1.0 CHECK (confidence >= 0.0 AND confidence <= 1.0),
    qdrant_point_id UUID,
    agent_version TEXT NOT NULL,
    retrieval_count INT NOT NULL DEFAULT 0,
    last_retrieved_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE memory.graph_nodes IS 'Узлы псевдографа. Живут строго внутри topic_id. Активен или в архиве (is_active).';
COMMENT ON COLUMN memory.graph_nodes.actor_id IS 'Владелец узла – идентификатор актора (пользователя). Обеспечивает изоляцию данных между пользователями.';
COMMENT ON COLUMN memory.graph_nodes.id IS 'Уникальный идентификатор узла (UUID). Используется во всех промптах, Qdrant и рёбрах.';
COMMENT ON COLUMN memory.graph_nodes.domain_id IS 'UUID домена знаний. Стабильный идентификатор для фильтрации, маршрутизации и Qdrant-payload.';
COMMENT ON COLUMN memory.graph_nodes.topic_id IS  'Идентификатор темы, к которой принадлежит узел. Может быть NULL, если узел ещё не привязан к теме (например, при импорте).';
COMMENT ON COLUMN memory.graph_nodes.form_code IS 'Форма сущности (из справочника memory.forms). Определяет природу узла: fact, goal, task, project, entity, skill, event.';
COMMENT ON COLUMN memory.graph_nodes.description IS 'Полное описание сущности. Обновляется при слиянии или сжатии.';
COMMENT ON COLUMN memory.graph_nodes.summary IS 'Краткая суть узла. Заполняется асинхронно задачей graph_summarize. NULL при создании.';
COMMENT ON COLUMN memory.graph_nodes.is_active IS 'Флаг активности. FALSE = узел в архиве.';
COMMENT ON COLUMN memory.graph_nodes.context_date IS 'Дата привязки факта (если извлечена). NULL для вневременных знаний.';
COMMENT ON COLUMN memory.graph_nodes.needs_review IS 'Флаг, указывающий, что узел требует ручной проверки (например, при низкой уверенности или конфликте фактов).';
COMMENT ON COLUMN memory.graph_nodes.needs_entity_binding IS 'Флаг: узел ожидает привязки к entity-агрегатору. Ставится при создании fact-узла, сбрасывается после привязки.';
COMMENT ON COLUMN memory.graph_nodes.source_hypothesis_ids IS 'Массив UUID гипотез, из которых был сформирован данный узел. Используется для трассировки происхождения знаний.';
COMMENT ON COLUMN memory.graph_nodes.confidence IS 'Уверенность в достоверности узла (от 0.0 до 1.0). Вычисляется на основе уверенности исходных гипотез и степени их согласованности.';
COMMENT ON COLUMN memory.graph_nodes.qdrant_point_id IS 'Идентификатор точки в векторной базе Qdrant, соответствующей эмбеддингу описания узла. Используется для семантического поиска.';
COMMENT ON COLUMN memory.graph_nodes.agent_version IS 'Версия агента (или конвейера обработки), создавшего или последний раз обновившего узел. Поле для отладки и миграций.';
COMMENT ON COLUMN memory.graph_nodes.retrieval_count IS 'Счетчик попаданий узла в итоговый контекст (retrieved_context).';
COMMENT ON COLUMN memory.graph_nodes.last_retrieved_at IS 'Время последнего попадания узла в контекст.';
COMMENT ON COLUMN memory.graph_nodes.created_at IS 'Метка времени создания узла. Устанавливается автоматически.';
COMMENT ON COLUMN memory.graph_nodes.updated_at IS 'Метка времени последнего обновления узла. Обновляется автоматически триггером.';

CREATE INDEX idx_graph_nodes_actor ON memory.graph_nodes (actor_id);
CREATE INDEX idx_graph_nodes_domain ON memory.graph_nodes (domain_id);
CREATE INDEX idx_graph_nodes_topic ON memory.graph_nodes (topic_id) WHERE topic_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_graph_nodes_form ON memory.graph_nodes (form_code) WHERE form_code IS NOT NULL;
CREATE INDEX idx_graph_nodes_active ON memory.graph_nodes (is_active) WHERE is_active = TRUE;
CREATE INDEX idx_graph_nodes_qdrant ON memory.graph_nodes (qdrant_point_id) WHERE qdrant_point_id IS NOT NULL;
CREATE INDEX idx_graph_nodes_needs_review ON memory.graph_nodes (needs_review) WHERE needs_review = TRUE;
CREATE INDEX idx_graph_nodes_needs_summary_update ON memory.graph_nodes (needs_summary_update) WHERE needs_summary_update = TRUE;
CREATE INDEX idx_graph_nodes_retrieval_count ON memory.graph_nodes (retrieval_count DESC) WHERE retrieval_count > 0;
CREATE INDEX IF NOT EXISTS idx_graph_nodes_needs_entity_binding ON memory.graph_nodes (needs_entity_binding) WHERE needs_entity_binding = TRUE;

DROP TRIGGER IF EXISTS trg_graph_nodes_update_updated_at ON memory.graph_nodes;
CREATE TRIGGER trg_graph_nodes_update_updated_at BEFORE UPDATE ON memory.graph_nodes FOR EACH ROW EXECUTE FUNCTION common.update_updated_at_column();

-- 3. Справочник типов связей (РЁБРА псевдографа)
CREATE TABLE IF NOT EXISTS memory.relation_types (
    code TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    agent_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE memory.relation_types IS 'Справочник типов рёбер псевдографа. Определяет семантику связи между узлами.';
COMMENT ON COLUMN memory.relation_types.code IS 'Код типа связи (уникальный строковой идентификатор). Используется в рёбрах для указания семантики связи между узлами.';
COMMENT ON COLUMN memory.relation_types.description IS 'Описание семантики типа связи, поясняющее, как интерпретировать связь (например, "contains", "related_to", "contradicts").';
COMMENT ON COLUMN memory.relation_types.is_active IS 'Флаг активности типа связи. Неактивные типы не должны использоваться в новых рёбрах (мягкое удаление).';
COMMENT ON COLUMN memory.relation_types.agent_version IS 'Версия агента или конвейера обработки, создавшего или последний раз обновившего запись. Используется для отладки и миграций.';
COMMENT ON COLUMN memory.relation_types.created_at IS 'Метка времени создания записи. Устанавливается автоматически.';

CREATE INDEX IF NOT EXISTS idx_relation_types_is_active ON memory.relation_types (is_active) WHERE is_active = TRUE;

INSERT INTO memory.relation_types (code, description, agent_version) VALUES
    ('has_topic',    'Домен включает тему', '1.3.0'),
    ('contains',     'Тема группирует узел', '1.3.0'),
    ('about', 'Факт описывает сущность (fact → entity). Связывает детальный факт с его агрегатором.', '1.4.0'),
    ('related_to',   'Смысловая близость узлов', '1.3.0'),
    ('contradicts',  'Противоречие фактов между узлами', '1.3.0'),
    ('refines',      'Уточнение/детализация другого узла', '1.3.0'),
    ('depends_on',   'Зависимость одного узла от другого', '1.3.0'),
    ('supersedes',   'Новый факт заменяет устаревший (временная эволюция)', '1.3.0')
ON CONFLICT (code) DO NOTHING;

-- 4. Рёбра графа (фактические связи)
CREATE TABLE IF NOT EXISTS memory.graph_edges (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    actor_id UUID REFERENCES users.actors(id) ON DELETE CASCADE,
    source_node_id UUID NOT NULL REFERENCES memory.graph_nodes(id) ON DELETE CASCADE,
    target_node_id UUID NOT NULL REFERENCES memory.graph_nodes(id) ON DELETE CASCADE,
    relation_type TEXT NOT NULL REFERENCES memory.relation_types(code) ON DELETE RESTRICT,
    source_hypothesis_ids UUID[] NOT NULL DEFAULT '{}'::UUID[],
    confidence REAL NOT NULL DEFAULT 1.0 CHECK (confidence >= 0.0 AND confidence <= 1.0),
    needs_review BOOLEAN NOT NULL DEFAULT FALSE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    agent_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE memory.graph_edges IS 'Рёбра псевдографа. Связи только внутри одной темы. Тип определяется справочником relation_types.';
COMMENT ON COLUMN memory.graph_edges.id IS 'Уникальный идентификатор ребра (UUID).';
COMMENT ON COLUMN memory.graph_edges.actor_id IS 'Владелец ребра – идентификатор актора (пользователя). Обеспечивает изоляцию данных.';
COMMENT ON COLUMN memory.graph_edges.source_node_id IS 'Идентификатор узла-источника связи.';
COMMENT ON COLUMN memory.graph_edges.target_node_id IS 'Идентификатор узла-цели связи.';
COMMENT ON COLUMN memory.graph_edges.relation_type IS 'Код типа связи (из справочника relation_types).';
COMMENT ON COLUMN memory.graph_edges.source_hypothesis_ids IS 'Массив UUID гипотез, на основе которых создано ребро.';
COMMENT ON COLUMN memory.graph_edges.confidence IS 'Уверенность в достоверности связи (0.0 – 1.0).';
COMMENT ON COLUMN memory.graph_edges.needs_review IS 'Флаг, указывающий, что ребро требует ручной проверки.';
COMMENT ON COLUMN memory.graph_edges.is_active IS 'Флаг активности. FALSE = ребро в архиве.';
COMMENT ON COLUMN memory.graph_edges.agent_version IS 'Версия агента, создавшего или обновившего ребро.';
COMMENT ON COLUMN memory.graph_edges.created_at IS 'Метка времени создания.';
COMMENT ON COLUMN memory.graph_edges.updated_at IS 'Метка времени последнего обновления (обновляется триггером).';

CREATE INDEX idx_graph_edges_source ON memory.graph_edges (source_node_id);
CREATE INDEX idx_graph_edges_target ON memory.graph_edges (target_node_id);
CREATE INDEX idx_graph_edges_relation ON memory.graph_edges (relation_type);
CREATE INDEX idx_graph_edges_actor ON memory.graph_edges (actor_id);
CREATE INDEX idx_graph_edges_is_active ON memory.graph_edges (is_active) WHERE is_active = TRUE;
CREATE INDEX idx_graph_edges_needs_review ON memory.graph_edges (needs_review) WHERE needs_review = TRUE;
CREATE INDEX idx_graph_edges_confidence ON memory.graph_edges (confidence);
CREATE UNIQUE INDEX idx_graph_edges_source_target_rel_unique ON memory.graph_edges (source_node_id, target_node_id, relation_type);

CREATE TRIGGER trg_graph_edges_update_updated_at BEFORE UPDATE ON memory.graph_edges FOR EACH ROW EXECUTE FUNCTION common.update_updated_at_column();

-- 5. Ревизии узлов
CREATE TABLE IF NOT EXISTS memory.graph_node_revisions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    node_id UUID NOT NULL REFERENCES memory.graph_nodes(id) ON DELETE CASCADE,
    previous_description TEXT,
    form_code TEXT REFERENCES memory.forms(code) ON DELETE SET NULL,
    new_description TEXT NOT NULL,
    hypothesis_id UUID REFERENCES memory.hypotheses(id) ON DELETE SET NULL,
    actor_id UUID REFERENCES users.actors(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE memory.graph_node_revisions IS 'История изменений описаний узлов графа.';
COMMENT ON COLUMN memory.graph_node_revisions.id IS 'Уникальный идентификатор записи ревизии.';
COMMENT ON COLUMN memory.graph_node_revisions.node_id IS 'Идентификатор узла, к которому относится ревизия.';
COMMENT ON COLUMN memory.graph_node_revisions.previous_description IS 'Предыдущее описание узла (до изменения). NULL для первой ревизии.';
COMMENT ON COLUMN memory.graph_node_revisions.form_code IS 'Форма узла на момент ревизии. Позволяет отследить смену формы при merge.';
COMMENT ON COLUMN memory.graph_node_revisions.new_description IS 'Новое описание узла (после изменения).';
COMMENT ON COLUMN memory.graph_node_revisions.hypothesis_id IS 'Идентификатор гипотезы, вызвавшей изменение (если применимо).';
COMMENT ON COLUMN memory.graph_node_revisions.actor_id IS 'Идентификатор актора (пользователя), инициировавшего изменение. NULL для системных изменений.';
COMMENT ON COLUMN memory.graph_node_revisions.created_at IS 'Метка времени создания записи ревизии (время изменения узла).';

CREATE INDEX idx_graph_node_revisions_node ON memory.graph_node_revisions(node_id);
CREATE INDEX idx_graph_node_revisions_actor ON memory.graph_node_revisions(actor_id);
CREATE INDEX idx_graph_node_revisions_hypothesis ON memory.graph_node_revisions(hypothesis_id);
CREATE INDEX idx_graph_node_revisions_created_at ON memory.graph_node_revisions(created_at);

-- 6. Промпты
DO $$
DECLARE
    v_dest UUID; v_owner UUID;
BEGIN
    SELECT id INTO v_dest FROM orchestrator.prompt_destinations WHERE name='internal_logic' LIMIT 1;
    SELECT id INTO v_owner FROM users.actors WHERE type='owner' LIMIT 1;

    -- Промпт 1: Слияние узлов графа / разрешение конфликтов
    INSERT INTO orchestrator.prompts (version,name,description,type,destination_id,text,params,status,created_by,agent_version,created_at)
    VALUES ('1.1.0','graph_merge_resolver','LLM-резолвер слияния гипотезы с узлом графа знаний. Строгий JSON.','internal',v_dest,
    E'Ты — модуль разрешения слияний узлов графа знаний.\n' ||
    E'Вход:\n' ||
    E'[УЗЕЛ] id:{node_id}, form:{node_form}, description:"{description}", date:{context_date}\n' ||
    E'[ФАКТ] "{hypothesis_text}", source:{knowledge_source}, conf:{confidence}, fact_date:{hypothesis_context_date}\n\n' ||
    E'Правила:\n' ||
    E'1. form указывает на природу сущности (fact/goal/task/project/entity/skill/event). Если формы РАЗНЫЕ (например goal и fact, или entity и task) → ВСЕГДА action:separate. Слияние, supersede, contradicts возможны ТОЛЬКО между одинаковыми формами.\n' ||
    E'2. Если факт уточняет/дополняет узел без конфликта → action:merge, relation:refines\n' ||
    E'3. Если факт противоречит узлу → action:link, relation:contradicts, needs_review:true\n' ||
    E'4. Если факт новее по времени/заменяет по смыслу узел → action:supersede, relation:supersedes\n' ||
    E'5. Если схожесть поверхностная (разные сущности) → action:separate\n' ||
    E'6. Если не уверен или конфликт сложный → action:review, needs_review:true\n' ||
    E'7. Даты не выдумывай. context_date явно указаны.\n' ||
    E'8. new_description пиши ТОЛЬКО при merge. Сохрани ВСЮ важную информацию.\n\n' ||
    E'9. confidence (0.0-1.0) — уверенность в корректности результата. 1.0=прямо подтверждено, 0.8-0.9=логически следует, 0.5-0.7=частично/косвенно, <0.5=не включай или review.\n' ||
    E'10. В ответе ОБЯЗАТЕЛЬНО верни id целевого узла из входа как target_node_id.\n\n' ||
    E'Ответ СТРОГО JSON без markdown:\n' ||
    E'{"target_node_id":"uuid","action":"...","relation":"...","new_description":"...","confidence":...,"needs_review":false,"context_date":"YYYY-MM-DD|null"}',
        '{"model_name":"Qwen3.5-9B-Q4_K_M.gguf","temperature":0.8,"top_p":0.8, "top_k":20, "min_p":0.0, "max_tokens":20000, "presence_penalty":0.0, "repetition_penalty":1.0,"stop":["<|im_end|>"],"chat_template_kwargs":{"enable_thinking":true}}'::jsonb,
    'testing',v_owner,'1.3.0',now()) ON CONFLICT (name,version) DO NOTHING;

    -- Промпт 2: Логические связи внутри темы
    INSERT INTO orchestrator.prompts (version,name,description,type,destination_id,text,params,status,created_by,agent_version,created_at)
    VALUES ('1.1.0','graph_relation_linker','LLM-построитель связей внутри темы графа. JSON массив.','internal',v_dest,
    E'Ты — модуль построения связей между узлами графа памяти.\n' ||
    E'Входные данные: JSON-массив объектов узлов. Каждый узел содержит поля:\n' ||
    E'{"id":"uuid","form":"fact/goal/task/project/entity/skill/event","form_meaning":"пояснение","description":"...","context_date":"..."}\n\n' ||
    E'Задача: сформировать связи только между РАЗНЫМИ узлами (source_id и target_id не должны совпадать).\n' ||
    E'Используй строго следующие типы связей (никакие другие не добавляй):\n' ||
    E'- contradicts — если в описаниях узлов явный смысловой конфликт (взаимоисключающие факты).\n' ||
    E'- refines — если один узел детализирует/уточняет другой. source_id = более конкретный, target_id = более общий.\n' ||
    E'- depends_on — если один узел по смыслу требует наличия другого. source_id = зависимый, target_id = тот, от которого зависит.\n' ||
    E'- supersedes — если один узел заменяет другой по времени/актуальности. source_id = новый/актуальный, target_id = устаревший.\n' ||
    E'- related_to — ТОЛЬКО для семантически близких узлов, если ни одна из более сильных связей (contradicts, refines, depends_on, supersedes) к ним не подходит.\n' ||
    E'- Если ни один тип связи НЕ подходит — не включай пару в результат.\n\n' ||
    E'Важные правила:\n' ||
    E'- Запрещены петли (source_id = target_id).\n' ||
    E'- Каждая пара (source_id, target_id) должна встречаться только один раз (без дублирования связей).\n' ||
    E'- Используй только те id, которые реально присутствуют во входном массиве. Если какого-то id нет — не создавай такую связь.\n' ||
    E'- confidence от 0.0 до 1.0: 1.0 = прямо следует из текста, 0.8-0.9 = высокая/логичная, 0.3-0.7 = косвенная.\n' ||
    E'- needs_review = true, если confidence < 0.7, иначе false.\n\n' ||
    E'Формат ответа: строго ОДИН JSON-массив, без любых обёрток (без markdown, без пояснений до/после).\n' ||
    E'Схема элемента:\n' ||
    E'{"source_id":"uuid","target_id":"uuid","relation":"...","confidence":0.85,"needs_review":false}\n\n' ||
    E'Пример:\n' ||
    E'Вход: [{"id":"a1","form":"fact","form_meaning":"утверждение","description":"Кофе повышает давление","context_date":"2025-01-01"},{"id":"b2","form":"fact","form_meaning":"утверждение","description":"Кофе безопасен для гипотоников","context_date":"2025-02-01"},{"id":"c3","form":"goal","form_meaning":"цель","description":"Следить за здоровьем","context_date":"2025-03-01"}]\n' ||
    E'Ответ: [{"source_id":"b2","target_id":"a1","relation":"refines","confidence":0.9,"needs_review":false},{"source_id":"b2","target_id":"c3","relation":"related_to","confidence":0.6,"needs_review":true}]',
    '{"model_name":"Qwen3.5-9B-Q4_K_M.gguf","temperature":0.7,"top_p":0.8,"top_k":20,"min_p":0.0,"max_tokens":20000,"presence_penalty":0.0,"repetition_penalty":1.0,"stop":["<|im_end|>"],"chat_template_kwargs":{"enable_thinking":false}}'::jsonb,
    'testing',v_owner,'1.3.0',now()) ON CONFLICT (name,version) DO NOTHING;

    -- Промпт 3: Иерархическое саммари
    INSERT INTO orchestrator.prompts (version,name,description,type,destination_id,text,params,status,created_by,agent_version,created_at)
    VALUES ('1.1.0','graph_node_summarizer','LLM-построитель иерархических саммари узлов.','internal',v_dest,
    E'Ты — модуль построения иерархических summary узлов графа памяти.\n' ||
    E'Вход:\n' ||
    E'[ЦЕЛЕВОЙ УЗЕЛ] id:{id}, description:"{description}"\n' ||
    E'[ВХОДЯЩИЕ ПОДУЗЛЫ/ЗАВИСИМОСТИ] JSON массив: [{id, summary, relation_type}]\n\n' ||
    E'Задача:\n' ||
    E'Сформируй СЖАТОЕ summary целевого узла (не более 250 символов).\n' ||
    E'- ВСЕГДА формулируй суть узла своими словами, убирая мусор и повторы.\n' || 
    E'- Если описание уже короткое, просто перефразируй его короче и чётче.\n' ||  
    E'- Если есть подузлы — вплети их ключевые факты цйелевого узла, но сохрани лаконичность (это не конспект, а выжимка).\n' ||  
    E'- Не добавляй предположений о недостающих данных, мета-комментариев («в структуре памяти…», «данная запись…») и любой воды.\n' ||  
    E'- Не раздувай текст искусственно. Если суть умещается в 70 символов — пусть будет 70, не растягивай.\n' ||
    E'- confidence (0.0-1.0) в ответе — твоя уверенность в том, что summary точно отражает суть узла и зависимостей. 1.0=полное покрытие, 0.7-0.9=высокая точность, <0.7=есть неоднозначности.\n\n' ||
    E'Ответ СТРОГО JSON без markdown:\n' ||
    E'{"summary":"...","confidence":...}',
    '{"model_name":"Qwen3.5-9B-Q4_K_M.gguf","temperature":0.8,"top_p":0.8,"top_k":20,"min_p":0.0,"max_tokens":12000,"presence_penalty":0.0,"repetition_penalty":1.0,"stop":["<|im_end|>"],"chat_template_kwargs":{"enable_thinking":false}}'::jsonb,
    'testing',v_owner,'1.3.0',now()) ON CONFLICT (name,version) DO NOTHING;
END $$;

-- Промпт 4: промпт для именования кластеров ===
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
        'entity_cluster_namer',
        'Именование кластера fact-узлов: генерация имени и описания entity-агрегатора',
        'internal'::public.prompt_type,
        v_dest,
        E'<Правила>\n' ||
        E'Ты — модуль именования сущностей базы знаний.\n\n' ||
        E'Тебе дан кластер из нескольких fact-узлов, которые семантически близки и описывают одну сущность реального мира.\n\n' ||
        E'ВХОД:\n' ||
        E'JSON-массив узлов: [{"id": "uuid", "description": "текст факта"}, ...]\n\n' ||
        E'ЗАДАЧА:\n' ||
        E'1. Определи, какую сущность реального мира описывают эти факты (человек, машина, книга, организация, проект и т.п.).\n' ||
        E'2. Дай сущности краткое имя (2-5 слов), которое однозначно её идентифицирует.\n' ||
        E'3. Дай краткое описание (1-2 предложения), поясняющее суть сущности.\n\n' ||
        E'ПРИМЕРЫ:\n' ||
        E'Вход: [{"id": "abc", "description": "Пользователь курит по пачке сигарет в день"}, {"id": "def", "description": "Курил 10 лет, хочет бросить"}]\n' ||
        E'Ответ: {"entity_name": "Курение пользователя", "entity_description": "Вредная привычка курения, стаж и желание бросить"}\n\n' ||
        E'Вход: [{"id": "ghi", "description": "BMW X5 2022 года"}, {"id": "jkl", "description": "Расход топлива 12л/100км"}]\n' ||
        E'Ответ: {"entity_name": "BMW X5 пользователя", "entity_description": "Личный автомобиль BMW X5 2022 года"}\n\n' ||
        E'ПРАВИЛА:\n' ||
        E'- Имя должно быть конкретным, не абстрактным.\n' ||
        E'- Описание должно отражать только суть.\n' ||
        E'- Если факты разнородные и не описывают одну сущность — верни {"entity_name": null, "entity_description": null}.\n\n' ||
        E'ОТВЕТ СТРОГО JSON без markdown:\n' ||
        E'{"entity_name": "имя", "entity_description": "описание"}\n' ||
        E'</Правила>',
        '{
            "model_name": "Qwen3.5-9B-Q4_K_M.gguf",
            "temperature": 0.7,
            "top_p": 0.8,
            "top_k": 20,
            "min_p": 0.0,
            "max_tokens": 8000,
            "presence_penalty": 0.0,
            "repetition_penalty": 1.0,
            "stop": ["<|im_end|>"],
            "chat_template_kwargs": {"enable_thinking": false}
        }'::jsonb,
        '{}'::jsonb,
        'testing'::public.prompt_status,
        v_owner,
        '1.5.0',
        now()
    ) ON CONFLICT (name, version) DO NOTHING;
END $$;