"""
main-srv/src/model_service/providers/external_dashscope.py

Провайдер для API DashScope (Alibaba Cloud).
Заглушка: реализует интерфейс, но по умолчанию отключена.
"""

__version__ = "1.1.0"
__description__ = "DashScope provider stub (OpenAI-compatible API)"

import logging
import os
from typing import Dict, Any, List
import httpx

from .base import LLMProvider

logger = logging.getLogger(__name__)


class DashScopeProvider(LLMProvider):
    """
    Провайдер для DashScope (Alibaba Cloud) через OpenAI-compatible API.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.base_url = config["base_url"]
        self.timeout = config["timeout"]
        self.api_key = os.getenv(config.get("api_key_env", "DASHSCOPE_API_KEY"))
        
        if not self.api_key:
            logger.warning("DashScope API key not found in env var %s", config.get("api_key_env"))
        
        self.client = httpx.Client(
            timeout=httpx.Timeout(
                connect=10.0,
                read=self.timeout,
                write=30.0,
                pool=60.0
            ),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}" if self.api_key else ""
            }
        )
        
        logger.info("DashScopeProvider initialized: %s", self.base_url)
    
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
        Генерация через DashScope API.
        Возвращает тот же формат, что и LocalLlamaProvider.
        """
        # TODO: Реализация вызова DashScope API
        # Пока возвращаем заглушку для тестирования роутинга
        
        logger.debug("DashScopeProvider.generate called with model=%s", model_name)
        
        return {
            "success": True,
            "response": f"[DashScope stub] Ответ для модели {model_name}",
            "reasoning_content": "[stub reasoning]",
            "metrics": {
                "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
                "timings": {},
                "model": model_name,
                "id": "stub-id",
                "host_nctx": 32768
            },
            "error": ""
        }
    
    def is_available(self) -> bool:
        """Проверка доступности через тестовый запрос."""
        if not self.api_key:
            return False
        try:
            response = self.client.get(
                self.base_url.replace("/chat/completions", "/models"),
                timeout=5.0
            )
            return response.status_code == 200
        except Exception:
            return False
    
    def get_model_info(self, model_name: str) -> Dict[str, Any]:
        """Информация о модели из конфига."""
        models = self.config.get("models", {})
        return models.get(model_name, {
            "n_ctx": 32768,
            "supports_reasoning": True,
            "max_tokens": 8192
        })
    
    def close(self) -> None:
        if hasattr(self, "client"):
            self.client.close()