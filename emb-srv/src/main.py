"""/emb-srv/src/main.py"""

__version__ = "1.0.0"
__description__ = "Главный модуль сервера эмбендингов"

import time
from datetime import datetime, timezone
import threading
import yaml
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from llama_cpp import Llama
from typing import Optional

# --- Пути и конфигурация ---
PROJECT_ROOT = Path(__file__).parent.parent

def load_config(name: str) -> dict:
    """Загружает конфигурационный файл из папки configs."""
    with open(PROJECT_ROOT / "configs" / name, encoding="utf-8") as f:
        return yaml.safe_load(f)

# Загрузка конфигов
MODEL_CFG = load_config("model_config.yaml")
SERVER_CFG = load_config("server_config.yaml")

# Путь к модели
MODEL_PATH = Path(MODEL_CFG["model_path"])

# Извлекаем имя файла модели для ответа API
MODEL_NAME = MODEL_PATH.name
N_CTX = MODEL_CFG["n_ctx"]
EMBEDDING_DIM = MODEL_CFG["embedding_dim"]

# --- Глобальная модель (инициализируется при запуске) ---
llm: Optional[Llama] = None

# 🔒 Блокировка для сериализации доступа к модели
model_lock = threading.Lock()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управляет жизненным циклом: загрузка и выгрузка модели."""
    global llm
    print("🔄 Загрузка модели...")
    llm = Llama(
        model_path=str(MODEL_PATH),
        n_gpu_layers=MODEL_CFG["n_gpu_layers"],
        n_ctx=N_CTX,
        embedding=True,
        verbose=False
    )
    print("✅ Модель успешно загружена.")
    yield
    # Очистка (опционально — память освобождается автоматически)
    llm = None
    print("🛑 Модель выгружена.")

# --- Настройка FastAPI ---
app = FastAPI(
    title="emb-srv",
    version=__version__,
    description=__description__,
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None
)

def now_iso_utc():
    """Возвращает текущее UTC время сервера в формате ISO 8601 с микросекундами."""
    return datetime.now(timezone.utc).isoformat(timespec='microseconds')

class EmbedRequest(BaseModel):
    """Модель входного запроса: содержит текст для векторизации."""
    text: str

@app.post("/embed")
async def embed(req: EmbedRequest):
    """
    Принимает текст и возвращает его эмбеддинг.
    Запросы ждут своей очереди — не отклоняются.
    duration_sec включает в себ яи время ожиданяи в очереди.
    """
    received_iso = now_iso_utc()
    received_ts = time.time() 
    vector = None
    error = None

    try:
        # Генерация эмбеддинга
        # 🔒 Ждём, пока модель освободится
        with model_lock:
            # Защита на случай гонки (теоретически не нужно при корректном lifespan)
            assert llm is not None, "Модель не загружена"

        result = llm.create_embedding(req.text)
        vector = result["data"][0]["embedding"]
        # Проверка размерности (ожидаем 2560 для Qwen3-4B)
        if len(vector) != EMBEDDING_DIM:
            raise ValueError(f"Неверная размерность эмбеддинга: {len(vector)} (ожидалось {EMBEDDING_DIM})")
    except Exception as e:
        error = str(e)
        vector = None
    finally:
        sent_iso = now_iso_utc()
        duration = time.time() - received_ts
        return JSONResponse({
            "vector": vector,
            "model": {
                "name": MODEL_NAME,
                "n_ctx": N_CTX,
                "embedding_dim": EMBEDDING_DIM
            },
            "params": {
                "received_at": received_iso,
                "sent_at": sent_iso,
                "duration_sec": round(duration, 4),
                "error": error
            }
        })

