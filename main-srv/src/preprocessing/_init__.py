"""
Подсистема преданализа запроса пользователя.

Архитектура:
- routing_composer.py     — Шаг 1: LLM-определение релевантных доменов/тем
- retrieval_composer.py   — Шаг 2: Поиск и сборка контекста из графа знаний
- pipeline.py             — Оркестрация двух шагов в рамках одной задачи
"""
version = "1.1.0"
description = "Query preprocessing: routing + knowledge retrieval"