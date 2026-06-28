"""
main-srv/src/model_service/providers/local_llama.py

Провайдер для локального llama-server (API, совместимый с OpenAI).
Поддерживает модель Qwen 3.5-9b с отдельным полем `reasoning_content`.
"""

__version__ = "1.1.0"
__description__ = "Local llama-server provider via HTTP API"

import logging
import time
from typing import Dict, Any, List, Optional
import httpx

from .base import LLMProvider

logger = logging.getLogger(__name__)


class LocalLlamaProvider(LLMProvider):
    """
    Провайдер для llama-server с поддержкой:
    - повторных попыток при сетевых ошибках
    - извлечения reasoning_content как отдельного поля
    - сбора метрик (timings, usage)
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.base_url = config["base_url"]
        self.timeout = config["timeout"]
        self.retry_config = config.get("retry", {})
        self.models_config = config.get("models", {})
        
        self.client = httpx.Client(
            timeout=httpx.Timeout(
                connect=10.0,
                read=self.timeout,
                write=30.0,
                pool=60.0
            ),
            headers={"Content-Type": "application/json"}
        )
        
        logger.info("LocalLlamaProvider initialized: %s", self.base_url)
    
    def _call_server(self, payload: Dict[str, Any], model_name: str) -> Dict[str, Any]:
        """
        HTTP-вызов к llama-server с повторными попытками.
        """
        max_attempts = self.retry_config.get("max_attempts", 3)
        backoff_base = self.retry_config.get("backoff_seconds", 1.0)
        last_error: Optional[str] = None
        
        for attempt in range(1, max_attempts + 1):
            try:
                response = self.client.post(
                    url=self.base_url,
                    json=payload,
                    timeout=self.timeout
                )
                response.raise_for_status()
                return {"success": True, "data": response.json(), "error": ""}
                
            except httpx.TimeoutException as e:
                last_error = f"Timeout: {e}"
                logger.warning("Attempt %d/%d: %s", attempt, max_attempts, last_error)
                
            except httpx.HTTPStatusError as e:
                if e.response.status_code >= 500:
                    last_error = f"Server {e.response.status_code}: {e}"
                    logger.warning("Attempt %d/%d: %s", attempt, max_attempts, last_error)
                else:
                    return {
                        "success": False,
                        "data": None,
                        "error": f"HTTP {e.response.status_code}: {e.response.text}"
                    }
                    
            except httpx.RequestError as e:
                last_error = f"Network: {e}"
                logger.warning("Attempt %d/%d: %s", attempt, max_attempts, last_error)
                
            except Exception as e:
                last_error = f"Unexpected: {type(e).__name__}: {e}"
                logger.error("Critical error: %s", last_error, exc_info=True)
                break
            
            if attempt < max_attempts:
                time.sleep(backoff_base * (2 ** (attempt - 1)))
        
        return {"success": False, "data": None, "error": last_error or "Unknown error"}
    
    def _parse_response(self, raw_data: Dict[str, Any], model_name: str) -> Dict[str, Any]:
        """
        Парсит ответ llama-server и извлекает content + reasoning_content.
        """
        try:
            message = raw_data["choices"][0]["message"]
            content = message.get("content", "")
            reasoning = message.get("reasoning_content", "")  # ← отдельное поле!
            
            usage = raw_data.get("usage", {})
            timings = raw_data.get("timings", {})
            model_info = self.models_config.get(model_name, {})
            
            return {
                "success": True,
                "response": content,
                "reasoning_content": reasoning,
                "metrics": {
                    "usage": {
                        "prompt_tokens": usage.get("prompt_tokens", 0),
                        "completion_tokens": usage.get("completion_tokens", 0),
                        "total_tokens": usage.get("total_tokens", 0)
                    },
                    "timings": {
                        "cache_n": timings.get("cache_n", 0),
                        "prompt_ms": timings.get("prompt_ms", 0.0),
                        "prompt_per_token_ms": timings.get("prompt_per_token_ms", 0.0),
                        "prompt_per_second": timings.get("prompt_per_second", 0.0),
                        "predicted_per_second": timings.get("predicted_per_second", 0.0),
                        "predicted_ms": timings.get("predicted_ms", 0.0)
                    },
                    "model": raw_data.get("model", model_name),
                    "id": raw_data.get("id", ""),
                    "host_nctx": model_info.get("n_ctx", 32768)
                },
                "error": ""
            }
        except (KeyError, IndexError, TypeError) as e:
            return {
                "success": False,
                "response": "",
                "reasoning_content": "",
                "metrics": {},
                "error": f"Parse error: {type(e).__name__}: {e}"
            }
    
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
        Генерация через llama-server.
        """
        # Валидация
        if not messages or not isinstance(stop, list):
            return {
                "success": False, "response": "", "reasoning_content": "",
                "metrics": {}, "error": "Invalid input parameters"
            }
        
        # Формирование payload
        payload = {
            "model": model_name,
            "messages": messages,
            "temperature": temperature,
            "top_p": top_p,
            "top_k": top_k,
            "min_p": min_p,
            "max_tokens": max_tokens,
            "presence_penalty": presence_penalty,
            "stop": stop,
            "stream": False,
            **extra_params  # chat_template_kwargs и другие
        }
        
        # Вызов сервера
        raw = self._call_server(payload, model_name)
        if not raw["success"]:
            return {
                "success": False, "response": "", "reasoning_content": "",
                "metrics": {}, "error": raw["error"]
            }
        
        result = self._parse_response(raw["data"], model_name)
    
        # Проверка на пустой ответ + одна попытка регенерации
        if not result.get("response", " ").strip():
            # ИЗМЕНЕНО: Это восстанавливаемая ошибка, понижаем уровень лога
            logger.warning(
                "Empty response from model %s on first attempt, retrying... "
                "(Possible server-side cache invalidation or transient glitch)",
                model_name
            )
            retry_raw = self._call_server(payload, model_name)
            if retry_raw["success"]:
                retry = self._parse_response(retry_raw["data"], model_name)
                if retry.get("response", " ").strip():
                    logger.debug("Retry succeeded for model %s", model_name)
                    return retry
            # Если retry не помог — только тогда ошибка
            return {
                "success": False,
                "response": " ",
                "reasoning_content": result.get("reasoning_content", " "),
                "metrics": result.get("metrics", {}),
                "error": "Empty response after retry"
            }
        
        return result
    
    def is_available(self) -> bool:
        """Проверка доступности llama-server через health-check."""
        try:
            response = self.client.get(
                self.base_url.replace("/chat/completions", "/models"),
                timeout=5.0
            )
            return response.status_code == 200
        except Exception:
            return False
    
    def get_model_info(self, model_name: str) -> Dict[str, Any]:
        """Возвращает информацию о модели из конфига."""
        return self.models_config.get(model_name, {
            "n_ctx": 32768,
            "supports_reasoning": True,
            "max_tokens": 16384
        })
    
    def close(self) -> None:
        """Закрытие HTTP-соединений."""
        if hasattr(self, "client"):
            self.client.close()