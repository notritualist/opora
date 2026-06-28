"""
main-srv/src/model_service/providers/base.py

Абстрактный базовый класс для всех провайдеров LLM.
Определяет единый контракт, который должны реализовывать все провайдеры.
"""

__version__ = "1.1.0"
__description__ = "Abstract base class for LLM providers"

from abc import ABC, abstractmethod
from typing import Dict, Any, List

class LLMProvider(ABC):
    """
    Базовый интерфейс провайдера LLM.
    
    Все провайдеры должны:
    - Принимать одинаковый набор параметров генерации
    - Возвращать унифицированный формат ответа
    - Самостоятельно обрабатывать ошибки и повторы
    """
    
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
        **extra_params
    ) -> Dict[str, Any]:
        """
        Генерация ответа модели.
        
        Все параметры обязательные — передаются из промпта.
        
        Args:
            messages: список сообщений в формате OpenAI
            temperature, top_p, top_k, min_p, max_tokens, presence_penalty, stop: параметры генерации
            model_name: имя модели (для внутреннего роутинга провайдера)
            **extra_params: дополнительные параметры (например, chat_template_kwargs)
            
        Returns:
            dict: Унифицированный формат ответа:
                {
                    "success": bool,
                    "response": str,              # чистый ответ (content)
                    "reasoning_content": str,     # рассуждение (отдельное поле), может быть пустым
                    "metrics": {
                        "usage": {...},           # prompt_tokens, completion_tokens, total_tokens
                        "timings": {...},         # prompt_ms, predicted_per_second, etc.
                        "model": str,             # фактически использованная модель
                        "id": str,                # ID запроса
                        "host_nctx": int          # n_ctx модели
                    },
                    "error": str                  # пусто при успехе
                }
        """
        pass
    
    @abstractmethod
    def is_available(self) -> bool:
        """
        Проверка доступности провайдера.
        
        Returns:
            bool: True если провайдер готов к работе
        """
        pass
    
    @abstractmethod
    def get_model_info(self, model_name: str) -> Dict[str, Any]:
        """
        Возвращает информацию о возможностях модели.
        
        Args:
            model_name: имя модели
            
        Returns:
            dict: {
                "n_ctx": int,
                "supports_reasoning": bool,
                "max_tokens": int,
                ...
            }
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        Корректное закрытие ресурсов провайдера (HTTP-соединения, файлы и т.д.).
        """
        pass