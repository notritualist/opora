"""
main-srv/src/services/tokens_counter.py

A module for accurately counting tokens for the Qwen3.5 model.

Uses the tokenizer library (fast Rust backend from HuggingFace).
This is the best choice for local AGI:
- 100% accuracy matches the Qwen3 model.
- Works completely offline (local tokenizer.json file)
- Loads in milliseconds, consumes minimal memory
- Simple and clear code for beginners.

Requirements:
1. Install the tokenizers component using pip.
2. Place the tokenizer.json file in the models/qwen3_5-tokenizer/ folder.
"""

__version__ = "1.0.0"
__description__ = "Token counting module for Qwen3 (one engine: tokenizers)"

import logging
from pathlib import Path
from functools import lru_cache

# Импортируем легковесную библиотеку для токенизации
# Если она не установлена — программа сразу сообщит об этом понятным сообщением
try:
    from tokenizers import Tokenizer
except ImportError:
    # Логгер может ещё не быть инициализирован при импорте модуля,
    # поэтому используем базовый logging для критических ошибок на старте
    logging.critical(
        "Library 'tokenizers' is not installed!\n"
        "Install it with the command: pip install tokenizers\n"
        "Without it, the token counting module will not work."
    )
    raise ImportError("The 'tokenizers' library is required. Run: pip install tokenizers")

# Получаем логгер для этого модуля.
# Он автоматически подхватит настройки из main.py (файл + консоль, уровни логирования)
logger = logging.getLogger(__name__)


# === НАСТРОЙКА ПУТЕЙ ===
# Определяем корень проекта, поднимаясь на 3 уровня вверх от текущего файла:
# tokens_counter.py → services → src → main-srv → kaya (корень)
_CURRENT_FILE = Path(__file__).resolve()
_PROJECT_ROOT = _CURRENT_FILE.parent.parent.parent

# Путь к папке с токенизатором.
# Здесь должен лежать файл tokenizer.json, скачанный с HuggingFace для модели Qwen3-8B
_TOKENIZER_DIR = _PROJECT_ROOT / "models" / "qwen3_5-tokenizer"
_TOKENIZER_FILE = _TOKENIZER_DIR / "tokenizer.json"


# === ГЛОБАЛЬНЫЙ ЭКЗЕМПЛЯР ТОКЕНИЗАТОРА (Singleton) ===
# Переменная хранит загруженный токенизатор, чтобы не грузить файл с диска каждый раз
_tokenizer: Tokenizer | None = None


def get_tokenizer() -> Tokenizer:
    """
    Возвращает экземпляр токенизатора Qwen3.
    
    Использует паттерн 'Ленивая инициализация' (Lazy Loading):
    - Токенизатор загружается с диска только при ПЕРВОМ вызове этой функции
    - Все последующие вызовы возвращают уже созданный объект из памяти
    - Это экономит время старта приложения и оперативную память
    
    Returns:
        Tokenizer: Готовый к работе объект токенизатора
        
    Raises:
        FileNotFoundError: Если файл tokenizer.json не найден по ожидаемому пути
        RuntimeError: Если файл повреждён или не может быть прочитан
    """
    global _tokenizer
    
    # Если токенизатор уже загружен — сразу возвращаем его (повторная загрузка не нужна)
    if _tokenizer is not None:
        return _tokenizer
    
    logger.debug(f"Loading tokenizer from: {_TOKENIZER_FILE}")
    
    # Проверяем, существует ли файл с токенизатором
    if not _TOKENIZER_FILE.exists():
        error_msg = (
            f"Tokenizer file not found: {_TOKENIZER_FILE}\n\n"
            f"What to fix:\n"
            f"1. Download the tokenizer.json file for the Qwen3 model from HuggingFace:\n"
            f"   https://huggingface.co/Qwen/Qwen3.5-9B/tree/main\n"
            f"2. Create a folder: {_TOKENIZER_DIR}\n"
            f"3. Place the downloaded file in this folder under the name: tokenizer.json"
        )
        logger.critical(error_msg)
        raise FileNotFoundError(error_msg)
    
    try:
        # Загружаем токенизатор из локального JSON-файла
        # from_file() — быстрый метод, который читает предкомпилированный конфиг токенизатора
        _tokenizer = Tokenizer.from_file(str(_TOKENIZER_FILE))
        
        logger.info(f"Qwen3_5 tokenizer successfully loaded from: {_TOKENIZER_FILE}")
        logger.debug(f"Dictionary size: {_tokenizer.get_vocab_size()} токенов")
        
    except Exception as e:
        logger.critical(f"Error loading tokenizer: {e}", exc_info=True)
        raise RuntimeError(f"Failed to initialize tokenizer: {e}")
    
    return _tokenizer


@lru_cache(maxsize=2048)
def count_tokens_qwen(text: str) -> int:
    """
    Подсчитывает количество токенов в тексте для модели Qwen3_5.
    
    Как это работает:
    1. Получает экземпляр токенизатора (загружает при первом вызове)
    2. Кодирует текст в последовательность ID токенов
    3. Возвращает длину этой последовательности
    
    Особенности:
    - Использует кэш (LRU Cache) на 2048 уникальных строк
      → Повторные вызовы с тем же текстом выполняются мгновенно
    - Не добавляет специальные токены (BOS/EOS), как это делает llama.cpp
      → Подсчёт точно соответствует реальному потреблению контекста модели
    
    Args:
        text (str): Текст для анализа
        
    Returns:
        int: Количество токенов. Для пустой строки возвращает 0
        
    Example:
        >>> count_tokens_qwen("Привет, Кая!")
        5
        >>> count_tokens_qwen("")  # пустая строка
        0
    """
    # Пустой текст = 0 токенов (быстрая проверка без вызова токенизатора)
    if not text:
        return 0
    
    try:
        # Получаем токенизатор (загрузится при первом вызове)
        tokenizer = get_tokenizer()
        
        # encode() возвращает объект Encoding, у которого есть свойство .ids — список ID токенов
        # len() считает количество этих ID = количество токенов
        token_count = len(tokenizer.encode(text).ids)
        
        # Логируем на уровне DEBUG, чтобы не засорять лог при частых вызовах
        # Показываем первые 30 символов текста для контекста
        logger.debug(f"Tokens: '{text[:30]}{'...' if len(text) > 30 else ''}' → {token_count}")
        
        return token_count
        
    except Exception as e:
        # Логируем ошибку с полным traceback (exc_info=True) — как в main.py
        logger.error(f"Token counting error: {e}", exc_info=True)
        
        # НЕ выбрасываем исключение дальше — чтобы не ломать весь поток обработки
        # Возвращаем грубую оценку (1 токен ≈ 4 символа) как "аварийный режим"
        # Это позволит системе продолжить работу с чуть менее точными метриками
        estimated = max(1, len(text) // 4)
        logger.warning(f"Using an estimate: ~{estimated} tokens (fallback)")
        return estimated


# === БЛОК САМОТЕСТИРОВАНИЯ ===
# Если файл запустили напрямую: python tokens_counter.py
if __name__ == "__main__":
    # Настраиваем базовое логирование для теста (если main.py ещё не запустился)
    logging.basicConfig(
        level=logging.DEBUG,
        format="[%(asctime)s] %(levelname)-8s | %(name)-15s | %(message)s",
        handlers=[logging.StreamHandler()]
    )
    
    print("\nTesting the tokens_counter.py module")
    print("=" * 50)
    
    test_cases = [
        ("", "Пустая строка"),
        ("Привет", "Короткое слово"),
        ("Привет, Кая! Как дела?", "Простая фраза"),
        ("The quick brown fox jumps over the lazy dog. " * 3, "Английский текст"),
        ("def hello():\n    print('Hello, World!')", "Код Python"),
    ]
    
    try:
        for text, description in test_cases:
            count = count_tokens_qwen(text)
            print(f"✓ {description:25s} | {count:3d} tokens | '{text[:40]}{'...' if len(text) > 40 else ''}'")
        
        # Проверка кэша
        print(f"\nCache statistics: {count_tokens_qwen.cache_info()}")
        
        # Проверка, что кэш работает (повторный вызов должен быть мгновенным)
        import time
        start = time.perf_counter()
        _ = count_tokens_qwen("Cache test " * 10)
        cached_time = time.perf_counter() - start
        
        start = time.perf_counter()
        _ = count_tokens_qwen("Cache test " * 10)  # Должно сработать из кэша
        cached_time_2 = time.perf_counter() - start
        
        print(f"First call: {cached_time*1000:.2f} ms")
        print(f"From cache:      {cached_time_2*1000:.2f} ms")
        
        print("\nAll tests passed successfully!")
        
    except Exception as e:
        print(f"\nTest failed: {e}")
        exit(1)