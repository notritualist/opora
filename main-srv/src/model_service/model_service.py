"""
main-srv/src/model_service/model_service.py

Singleton-роутер для LLM-провайдеров. Прямая маршрутизация на основе model_routing.yaml.

Архитектура и возможности:
1. Инициализация и конфигурация:
   - Загрузка плоского словаря routing.models (model_name → provider, n_ctx, params).
   - Ленивая инициализация провайдеров (LocalLlama, DashScope) с кэшированием.
2. Генерация (generate):
   - Автоматическое внедрение специфичных параметров модели (enable_search, enable_thinking) 
     из конфига, если они не переданы явно.
   - Делегирование вызова конкретному провайдеру.
   - Перезапись метрик (host_nctx) из routing.models как единого источника правды.
3. Информирование:
   - get_model_info: приоритет данных из routing.models над данными провайдера.
   - is_available: проверка доступности конкретной модели или любого провайдера.
"""
version = "1.1.0"
description = "Model router: direct dict-based routing (no patterns, no priority)"

import logging
import yaml
from pathlib import Path
from typing import Dict, Any, List, Optional
from threading import Lock

from .providers.base import LLMProvider
from .providers.local_llama import LocalLlamaProvider
from .providers.external_dashscope import DashScopeProvider

logger = logging.getLogger(__name__)


class ModelService:
    """
    Singleton-роутер для LLM-провайдеров.
    Логика: routing.models[model_name] → {provider, n_ctx, ...}
    """

    _instance: Optional["ModelService"] = None
    _init_lock: Lock = Lock()

    def __new__(cls, config_path: Optional[str] = None) -> "ModelService":
        with cls._init_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    def __init__(self, config_path: Optional[str] = None):
        if getattr(self, "_initialized", False):
            return
        self._initialized = True

        self.config = self._load_config(config_path)
        # routing.models — плоский dict: model_name → {provider, n_ctx, max_tokens, ...}
        self.routing_models: Dict[str, Dict[str, Any]] = (
            self.config.get("routing", {}).get("models", {})
        )
        self.providers_config = self.config.get("providers", {})

        # Кэш провайдеров (ленивая инициализация)
        self._providers: Dict[str, LLMProvider] = {}
        self._providers_lock: Lock = Lock()

        logger.info(
            "ModelService initialized with %d registered models",
            len(self.routing_models),
        )

    def _load_config(self, config_path: Optional[str]) -> Dict[str, Any]:
        if config_path is None:
            config_path = str(
                Path(__file__).parent.parent.parent / "configs" / "model_routing.yaml"
            )
        else:
            config_path = str(Path(config_path))

        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _resolve_provider_name(self, model_name: str) -> Optional[str]:
        """Прямой lookup: model_name → provider."""
        model_cfg = self.routing_models.get(model_name)
        if not model_cfg:
            logger.warning("Model '%s' is not registered in routing.models", model_name)
            return None
        provider_name = model_cfg.get("provider")
        if not provider_name:
            logger.error("No 'provider' specified for model '%s'", model_name)
            return None
        return provider_name

    def _get_provider(self, provider_name: str, model_name: str) -> Optional[LLMProvider]:
        with self._providers_lock:
            if provider_name in self._providers:
                return self._providers[provider_name]

            provider_config = self.providers_config.get(provider_name)
            if not provider_config:
                logger.error("Provider '%s' not found in providers config", provider_name)
                return None

            provider_type = provider_config.get("type")
            if provider_type == "llama_server":
                provider = LocalLlamaProvider(provider_config)
            elif provider_type == "openai_compatible":
                provider = DashScopeProvider(provider_config)
            else:
                logger.error("Unknown provider type: %s", provider_type)
                return None

            self._providers[provider_name] = provider
            logger.info("Provider '%s' initialized", provider_name)
            return provider

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
        # 1. Lookup модели
        model_cfg = self.routing_models.get(model_name)
        if not model_cfg:
            return {
                "success": False,
                "response": "",
                "reasoning_content": "",
                "metrics": {},
                "error": f"Model '{model_name}' is not registered in routing.models",
            }

        # 2. Авто-подтягиваем enable_search / enable_thinking из конфига модели,
        #    если они НЕ переданы явно в extra_params
        if "enable_search" not in extra_params and "enable_search" in model_cfg:
            extra_params["enable_search"] = model_cfg["enable_search"]
        if "enable_thinking" not in extra_params and "enable_thinking" in model_cfg:
            extra_params["enable_thinking"] = model_cfg["enable_thinking"]

        # 3. Резолв провайдера
        provider_name = model_cfg.get("provider")
        if not provider_name:
            return {
                "success": False, "response": "", "reasoning_content": "",
                "metrics": {}, "error": f"No provider for model '{model_name}'",
            }

        provider = self._get_provider(provider_name, model_name)
        if not provider:
            return {
                "success": False, "response": "", "reasoning_content": "",
                "metrics": {}, "error": f"Failed to initialize provider '{provider_name}'",
            }

        # 4. Делегируем вызов
        result = provider.generate(
            messages=messages,
            temperature=temperature,
            top_p=top_p,
            top_k=top_k,
            min_p=min_p,
            max_tokens=max_tokens,
            presence_penalty=presence_penalty,
            stop=stop,
            model_name=model_name,
            **extra_params,
        )

        # === ПЕРЕЗАПИСЬ host_nctx из routing.models (единый источник правды) ===
        # Провайдеры берут n_ctx из providers.<name>.models (которого может не быть).
        # routing.models[model_name].n_ctx — актуальный источник.
        if result.get("success") and model_cfg:
            correct_nctx = model_cfg.get("n_ctx")
            if correct_nctx and "metrics" in result:
                result["metrics"]["host_nctx"] = correct_nctx

        return result

    def is_available(self, model_name: Optional[str] = None) -> bool:
        if model_name:
            provider_name = self._resolve_provider_name(model_name)
            if not provider_name:
                return False
            provider = self._get_provider(provider_name, model_name)
            return bool(provider and provider.is_available())
        for name in self.providers_config:
            provider = self._get_provider(name, "")
            if provider and provider.is_available():
                return True
        return False

    def get_model_info(self, model_name: str) -> Dict[str, Any]:
        """
        Возвращает информацию о модели.
        ПРИОРИТЕТ: данные из routing.models[model_name] > данные от провайдера.
        """
        # 1. Сначала пытаемся взять из routing.models
        model_cfg = self.routing_models.get(model_name)
        if model_cfg:
            info = {
                "n_ctx": model_cfg.get("n_ctx", 32768),
                "max_tokens": model_cfg.get("max_tokens", 8192),
                "supports_reasoning": model_cfg.get("supports_reasoning", False),
                "enable_search": model_cfg.get("enable_search", False),
                "enable_thinking": model_cfg.get("enable_thinking", False),
                "provider": model_cfg.get("provider", "unknown"),
            }
            logger.debug("get_model_info('%s'): from routing.models → n_ctx=%d, provider=%s",
                         model_name, info["n_ctx"], info["provider"])
            return info

        # 2. Fallback: спрашиваем провайдер (для моделей не в routing.models)
        provider_name = self._resolve_provider_name(model_name)
        if provider_name:
            provider = self._get_provider(provider_name, model_name)
            if provider:
                return provider.get_model_info(model_name)

        # 3. Последний fallback
        logger.warning("get_model_info('%s'): model not found anywhere, returning defaults", model_name)
        return {
            "n_ctx": 32768,
            "max_tokens": 8192,
            "supports_reasoning": False,
            "provider": "unknown",
        }

    def list_models(self, provider: Optional[str] = None) -> List[str]:
        if provider:
            return [
                name for name, cfg in self.routing_models.items()
                if cfg.get("provider") == provider
            ]
        return list(self.routing_models.keys())

    def close(self):
        with self._providers_lock:
            for provider in self._providers.values():
                provider.close()
            self._providers.clear()
        logger.debug("All providers closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False