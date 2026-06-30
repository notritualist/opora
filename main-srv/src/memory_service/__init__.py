"""
main-srv/src/memory_service/__init__.py
Подсистема долговременной памяти агента.
Модули:
- constants: константы лимитов токенов и чанкинга
- memory_repository: CRUD гипотез и журнала обработок сообщений
- hypothesis_extractor: форматирование чанков, парсинг JSON, валидация
Интеграция: через orchestrator_tasks → orchestrator_steps → llm_metrics → hypotheses.
"""