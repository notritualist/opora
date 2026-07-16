"""
main-srv/src/model_service/providers/base.py

Абстрактный базовый класс (контракт) для всех LLM-провайдеров.

Определяет единый интерфейс и структуру возврата данных:
1. generate():
   - Принимает унифицированный набор параметров (temperature, top_p, max_tokens и т.д.).
   - Поддерживает **extra_params для провайдер-специфичных функций:
     - enable_search (bool): включение веб-поиска (например, для DashScope).
     - enable_thinking (bool): включение режима рассуждения (CoT).
   - Возвращает стандартизированный dict: {success, response, reasoning_content, metrics, error}.
2. is_available(): Проверка доступности эндпоинта провайдера.
3. get_model_info(): Возврат характеристик модели (n_ctx, max_tokens, поддержка reasoning/search/thinking).
4. close(): Корректное освобождение ресурсов провайдера.
"""
version = "1.2.0"
description = "Abstract base class for LLM providers"

from abc import ABC, abstractmethod
from typing import Dict, Any, List


class LLMProvider(ABC):
    @abstractmethod
    def generate(
        self,
        messages: List[Dict[str, str]],
        temperature: float,
        top_p: float,
        top_k: int,
        min_p: float,
        max_tokens: int,
        presence_penalty: float,
        stop: List[str],
        model_name: str,
        **extra_params,
    ) -> Dict[str, Any]:
        """
        Генерация ответа модели.
        
        Args:
            **extra_params: дополнительные параметры, в т.ч.:
                - enable_search (bool): включить веб-поиск (только для провайдеров, 
                  которые его поддерживают — DashScope)
                - enable_thinking (bool): включить режим рассуждения (thinking-модели)
                - chat_template_kwargs и любые другие
        
        Returns:
            Унифицированный dict:
            {
                "success": bool,
                "response": str,
                "reasoning_content": str,
                "metrics": {"usage": {...}, "timings": {...}, "model": str, ...},
                "error": str,
            }
        """
        pass

    @abstractmethod
    def is_available(self) -> bool:
        pass

    @abstractmethod
    def get_model_info(self, model_name: str) -> Dict[str, Any]:
        """
        Возвращает возможности модели.
        Fields:
            - n_ctx: int
            - max_tokens: int
            - supports_reasoning: bool
            - enable_search: bool (поддерживает ли модель веб-поиск)
            - enable_thinking: bool (поддерживает ли модель thinking-режим)
        """
        pass

    @abstractmethod
    def close(self) -> None:
        pass