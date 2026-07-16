"""
main-srv/src/model_service/providers/external_dashscope.py

Провайдер для API DashScope (Alibaba Cloud) с поддержкой специфичных функций Qwen.

Основные возможности:
1. Аутентификация:
   - Приоритетное чтение API-ключа из локального файла (keys/dashscope.key).
   - Fallback на переменную окружения (DASHSCOPE_API_KEY).
2. Специфичные параметры генерации:
   - enable_search: включение нативного веб-поиска DashScope.
   - enable_thinking: включение режима рассуждения (CoT) для thinking-моделей.
3. Обработка ответов и рассуждений:
   - Извлечение reasoning_content из нативного поля или парсинг из тегов <think>...</think>.
4. Умная эвристика метрик (без TTFT от API):
   - Оценка времени prefill (обработка промпта) и predict (генерация) на основе 
     сетевых задержек и количества токенов, так как DashScope API не возвращает TTFT напрямую.
5. Отказоустойчивость:
   - HTTP-запросы с экспоненциальным бэкоффом и ретраями (до 3 попыток для 5xx/429/Timeout).
"""

version = "1.2.0"
description = "DashScope provider with web-search and thinking support"

import logging
import os
import re
import time
from pathlib import Path
from typing import Dict, Any, List, Optional
import httpx
from .base import LLMProvider

logger = logging.getLogger(__name__)

KEYS_DIR = Path(__file__).parent.parent / "keys"
DASHSCOPE_KEY_FILE = KEYS_DIR / "dashscope.key"


def _load_api_key_from_file(key_file: Path) -> Optional[str]:
    try:
        if key_file.is_file():
            return key_file.read_text(encoding="utf-8").strip()
    except Exception as e:
        logger.warning("Failed to read API key from %s: %s", key_file, e)
    return None


class DashScopeProvider(LLMProvider):

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.base_url = config["base_url"]
        self.timeout = config["timeout"]
        self.retry_config = config.get("retry", {})

        # 1. Читаем ключ из файла
        self.api_key = _load_api_key_from_file(DASHSCOPE_KEY_FILE)
        if self.api_key:
            logger.info("DashScope API key loaded from file: %s", DASHSCOPE_KEY_FILE)
        else:
            # 2. Fallback: переменная окружения
            env_var = config.get("api_key_env", "DASHSCOPE_API_KEY")
            self.api_key = os.getenv(env_var)
            if self.api_key:
                logger.info("DashScope API key loaded from env: %s", env_var)
            else:
                logger.error(
                    "DashScope API key not found! Create %s or set %s",
                    DASHSCOPE_KEY_FILE, env_var,
                )

        self.client = httpx.Client(
            timeout=httpx.Timeout(connect=15.0, read=self.timeout, write=30.0, pool=60.0),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}" if self.api_key else "",
            },
        )
        logger.info("DashScopeProvider initialized: %s", self.base_url)

    def _call_api(self, payload: Dict[str, Any], model_name: str) -> Dict[str, Any]:
        max_attempts = self.retry_config.get("max_attempts", 3)
        backoff_base = self.retry_config.get("backoff_seconds", 1.0)
        last_error: Optional[str] = None
        elapsed = 0.0

        for attempt in range(1, max_attempts + 1):
            start = time.time()
            try:
                response = self.client.post(self.base_url, json=payload, timeout=self.timeout)
                elapsed = time.time() - start
                response.raise_for_status()
                return {"success": True, "data": response.json(), "error": "", "elapsed": elapsed}
            except httpx.TimeoutException as e:
                elapsed = time.time() - start
                last_error = f"Timeout after {elapsed:.2f}s: {e}"
                logger.warning("Attempt %d/%d for %s: %s", attempt, max_attempts, model_name, last_error)
            except httpx.HTTPStatusError as e:
                elapsed = time.time() - start
                status = e.response.status_code
                if status >= 500 or status == 429:
                    last_error = f"HTTP {status}: {e.response.text[:500]}"
                    logger.warning("Attempt %d/%d for %s: %s", attempt, max_attempts, model_name, last_error)
                else:
                    return {"success": False, "data": None, "error": f"HTTP {status}", "elapsed": elapsed}
            except httpx.RequestError as e:
                elapsed = time.time() - start
                last_error = f"Network: {e}"
                logger.warning("Attempt %d/%d for %s: %s", attempt, max_attempts, model_name, last_error)
            except Exception as e:
                elapsed = time.time() - start
                last_error = f"Unexpected: {type(e).__name__}: {e}"
                logger.error("Critical error: %s", last_error, exc_info=True)
                break

            if attempt < max_attempts:
                time.sleep(backoff_base * (2 ** (attempt - 1)))

        return {"success": False, "data": None, "error": last_error or "Unknown error", "elapsed": elapsed}


    def _parse_response(self, raw_data: Dict[str, Any], model_name: str, elapsed: float) -> Dict[str, Any]:
        try:
            message = raw_data["choices"][0]["message"]
            content = message.get("content", "") or ""
            reasoning = message.get("reasoning_content", "") or ""

            if not reasoning and "<think>" in content:
                match = re.search(r"<think>(.*?)</think>", content, re.DOTALL)
                if match:
                    reasoning = match.group(1).strip()
                    content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()

            usage = raw_data.get("usage", {})
            prompt_tokens = usage.get("prompt_tokens", 0)
            completion_tokens = usage.get("completion_tokens", 0)
            total_tokens = usage.get("total_tokens", 0)

            elapsed_ms = elapsed * 1000.0

            # ============================================================
            # === УМНАЯ ЭВРИСТИКА ДЛЯ API (без TTFT) ====================
            # ============================================================
            # Prefill (prompt processing) имеет фиксированные накладные расходы:
            # - ~200 мс на сетевые задержки + инициализацию
            # - ~1 мс на токен (грубо, для KV-cache build)
            # Генерация — всё остальное время.
            
            if completion_tokens > 0:
                # Фиксированная оценка prefill: 200мс + 1мс/токен, но не более 20% от elapsed
                estimated_prefill_ms = min(200.0 + prompt_tokens * 1.0, elapsed_ms * 0.2)
                estimated_predict_ms = elapsed_ms - estimated_prefill_ms
            else:
                estimated_prefill_ms = elapsed_ms
                estimated_predict_ms = 0.0

            prompt_per_second = (
                (prompt_tokens / (estimated_prefill_ms / 1000))
                if estimated_prefill_ms > 0 and prompt_tokens > 0 else 0.0
            )
            predicted_per_second = (
                (completion_tokens / (estimated_predict_ms / 1000))
                if estimated_predict_ms > 0 and completion_tokens > 0 else 0.0
            )
            prompt_per_token_ms = (
                (estimated_prefill_ms / prompt_tokens)
                if prompt_tokens > 0 else 0.0
            )

            model_info = self.config.get("models", {}).get(model_name, {})

            return {
                "success": True,
                "response": content,
                "reasoning_content": reasoning,
                "metrics": {
                    "usage": {
                        "prompt_tokens": prompt_tokens,
                        "completion_tokens": completion_tokens,
                        "total_tokens": total_tokens,
                    },
                    "timings": {
                        "cache_n": 0,
                        "prompt_ms": estimated_prefill_ms,
                        "prompt_per_token_ms": prompt_per_token_ms,
                        "prompt_per_second": prompt_per_second,
                        "predicted_per_second": predicted_per_second,
                        "predicted_ms": estimated_predict_ms,
                    },
                    "model": raw_data.get("model", model_name),
                    "id": raw_data.get("id", ""),
                    "host_nctx": model_info.get("n_ctx", 262144),
                    "client_elapsed": elapsed,
                },
                "error": "",
            }
        except (KeyError, IndexError, TypeError) as e:
            return {
                "success": False, "response": "", "reasoning_content": "",
                "metrics": {}, "error": f"Parse error: {type(e).__name__}: {e}",
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
        **extra_params,
    ) -> Dict[str, Any]:
        if not self.api_key:
            return {
                "success": False, "response": "", "reasoning_content": "",
                "metrics": {}, "error": "DashScope API key is not configured",
            }

        if not messages or not isinstance(stop, list):
            return {
                "success": False, "response": "", "reasoning_content": "",
                "metrics": {}, "error": "Invalid input parameters",
            }

        payload: Dict[str, Any] = {
            "model": model_name,
            "messages": messages,
            "temperature": temperature,
            "top_p": top_p,
            "max_tokens": max_tokens,
            "stream": False,
        }
        if stop:
            payload["stop"] = stop
        if presence_penalty != 0.0:
            payload["presence_penalty"] = presence_penalty

        # ============================================================
        # === Параметры веб-поиска и рассуждения ===
        # ============================================================
        enable_search = extra_params.pop("enable_search", False)
        if enable_search:
            payload["enable_search"] = True   # ← DashScope native параметр
            logger.debug("Web-search enabled for model %s", model_name)

        enable_thinking = extra_params.pop("enable_thinking", False)
        if enable_thinking:
            payload["enable_thinking"] = True  # ← DashScope native параметр
            logger.debug("Thinking mode enabled for model %s", model_name)

        # Пробрасываем всё остальное (chat_template_kwargs и т.д.)
        payload.update(extra_params)

        # Удаляем None
        payload = {k: v for k, v in payload.items() if v is not None}

        raw = self._call_api(payload, model_name)
        if not raw["success"]:
            return {
                "success": False, "response": "", "reasoning_content": "",
                "metrics": {}, "error": raw["error"],
            }
        return self._parse_response(raw["data"], model_name, raw["elapsed"])

    def is_available(self) -> bool:
        if not self.api_key:
            return False
        try:
            models_url = self.base_url.replace("/chat/completions", "/models")
            response = self.client.get(models_url, timeout=10.0)
            return response.status_code == 200
        except Exception as e:
            logger.warning("DashScope availability check failed: %s", e)
            return False

    def get_model_info(self, model_name: str) -> Dict[str, Any]:
        models = self.config.get("models", {})
        return models.get(model_name, {
            "n_ctx": 32768,
            "supports_reasoning": False,
            "max_tokens": 8192,
            "enable_search": False,
            "enable_thinking": False,
        })

    def close(self) -> None:
        if hasattr(self, "client"):
            self.client.close()