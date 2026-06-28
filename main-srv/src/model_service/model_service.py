"""
main-srv/src/model_service/model_service.py

A single entry point for generation via LLM.
Implements request routing to different providers based on the model name.

Architecture:
- Singleton: one instance per application
- Routing based on rules from model_routing.yaml (pattern matching)
- All providers return a unified response format
- The calling code (response_composer) remains unchanged

Usage example:
    model = ModelService()
    result = model.generate(
    messages=[...],
    model_name="Qwen3.5-9B-Q4_K_M.gguf", # ← routing by this field
    temperature=0.7,
    ...
    )
"""

__version__ = "1.0.0"
__description__ = "Model router: unified interface for multiple LLM providers"

import logging
import fnmatch
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
    
    Логика выбора провайдера:
    1. Получает model_name из вызова generate()
    2. Сопоставляет с правилами из routing.rules (по порядку priority)
    3. Возвращает экземпляр соответствующего провайдера
    4. Делегирует вызов generate() провайдеру
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
        
        # Загрузка конфигурации
        self.config = self._load_config(config_path)
        self.routing_rules = self.config.get("routing", {}).get("rules", [])
        self.providers_config = self.config.get("providers", {})
        
        # Кэш провайдеров (ленивая инициализация)
        self._providers: Dict[str, LLMProvider] = {}
        self._providers_lock: Lock = Lock()
        
        logger.info("ModelService router initialized with %d routing rules", len(self.routing_rules))
    
    def _load_config(self, config_path: Optional[str]) -> Dict[str, Any]:
        """Загрузка конфигурации из YAML."""
        if config_path is None:
            # Конфиг лежит в /main-srv/configs/ (согласно архитектуре)
            config_path = str(Path(__file__).parent.parent.parent / "configs" / "model_routing.yaml")
        else:
            config_path = str(Path(config_path))  # ← Явное преобразование Path → str
        
        assert config_path is not None  # ← Гарантируем, что путь не None
        with open(config_path, "r", encoding="utf-8") as f:  # ← Используем встроенный open()
            return yaml.safe_load(f)
    
    def _resolve_provider_name(self, model_name: str) -> Optional[str]:
        """
        Сопоставляет имя модели с провайдером по правилам роутинга.
        
        Правила применяются по порядку priority (убывание).
        Используется fnmatch для pattern matching (*.gguf, qwen-* и т.д.)
        
        Returns:
            str | None: имя провайдера из конфига или None
        """
        # Сортируем правила по priority (убывание)
        sorted_rules = sorted(
            [r for r in self.routing_rules if r.get("enabled", True)],
            key=lambda x: x.get("priority", 0),
            reverse=True
        )
        
        for rule in sorted_rules:
            pattern = rule.get("pattern", "*")
            if fnmatch.fnmatch(model_name, pattern):
                provider_name = rule.get("provider")
                # Проверяем, включен ли провайдер в конфиге
                if self.providers_config.get(provider_name, {}).get("enabled", False):
                    logger.debug("Model '%s' routed to provider '%s' (pattern: %s)", 
                               model_name, provider_name, pattern)
                    return provider_name
        
        logger.warning("No matching routing rule for model '%s'", model_name)
        return None
    
    def _get_provider(self, provider_name: str, model_name: str) -> Optional[LLMProvider]:
        """
        Возвращает или создаёт экземпляр провайдера (ленивая инициализация).
        """
        with self._providers_lock:
            if provider_name in self._providers:
                return self._providers[provider_name]
            
            provider_config = self.providers_config.get(provider_name)
            if not provider_config:
                logger.error("Provider '%s' not found in config", provider_name)
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
            logger.info("Provider '%s' initialized for model '%s'", provider_name, model_name)
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
        model_name: str,  # ← ключевое поле для роутинга!
        **extra_params
    ) -> Dict[str, Any]:
        """
        Генерация ответа через соответствующий провайдер.
        
        Все параметры передаются из промпта (orchestrator.prompts.params).
        model_name определяет, какой провайдер будет использован.
        
        Returns:
            dict: Унифицированный формат ответа (см. LLMProvider.generate)
        """
        # 1. Определяем провайдера по имени модели
        provider_name = self._resolve_provider_name(model_name)
        if not provider_name:
            return {
                "success": False,
                "response": "",
                "reasoning_content": "",
                "metrics": {},
                "error": f"No provider found for model '{model_name}'"
            }
        
        # 2. Получаем экземпляр провайдера
        provider = self._get_provider(provider_name, model_name)
        if not provider:
            return {
                "success": False,
                "response": "",
                "reasoning_content": "",
                "metrics": {},
                "error": f"Failed to initialize provider '{provider_name}'"
            }
        
        # 3. Делегируем вызов провайдеру
        return provider.generate(
            messages=messages,
            temperature=temperature,
            top_p=top_p,
            top_k=top_k,
            min_p=min_p,
            max_tokens=max_tokens,
            presence_penalty=presence_penalty,
            stop=stop,
            model_name=model_name,
            **extra_params
        )
    
    def is_available(self, model_name: Optional[str] = None) -> bool:
        """Проверка доступности провайдера для модели."""
        if model_name:
            provider_name = self._resolve_provider_name(model_name)
            if provider_name:
                provider = self._get_provider(provider_name, model_name)
                return provider.is_available() if provider else False  # ← Проверка на None
            return False
        
        # Проверка всех провайдеров
        for name, cfg in self.providers_config.items():
            if cfg.get("enabled", False):
                provider = self._get_provider(name, "")
                if provider and provider.is_available():  # ← Проверка на None
                    return True
        return False
    
    def get_model_info(self, model_name: str) -> Dict[str, Any]:
        """Возвращает информацию о модели через соответствующего провайдера."""
        provider_name = self._resolve_provider_name(model_name)
        if provider_name:
            provider = self._get_provider(provider_name, model_name)
            if provider:
                return provider.get_model_info(model_name)
        return {}
    
    def close(self):
        """Корректное закрытие всех провайдеров."""
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