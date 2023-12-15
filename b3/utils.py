import os
import json
from typing import Any


def save_json(file: Any, name: str, path: str = "data/") -> str:
    os.makedirs(path, exist_ok=True)
    fullpath = f"{path}{name}.json"

    with open(fullpath, "w") as f:
        f.write(json.dumps(file))

    return fullpath


def load_json(name: str, path: str = "data/") -> Any:
    fullpath = f"{path}{name}.json"

    with open(fullpath, "r") as f:
        return json.load(f)
