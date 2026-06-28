"""
/main-srv/src/version.py

Этот модуль делает версию агента из файла pyproject.toml доступной глобально.
Она используется везде, где требуется версия релиза:
- при миграциях базы данных;
- в записях базы данных;
- при логировании запуска системы;
- в метриках мониторинга.
"""

import tomllib
from pathlib import Path

def get_project_version() -> str:
    try:
        pyproject_path = Path(__file__).parent.parent.parent / "pyproject.toml"
        with open(pyproject_path, "rb") as f:
            data = tomllib.load(f)
        return data["project"]["version"]
    except Exception as e:
        # Логирование через print — допустимо на старте, до инициализации логгера
        print(f"Failed to read version from pyproject.toml: {e}")
        return "0.0.0-dev"

# PEP 8: модуль должен экспортировать __version__
__version__ = get_project_version()