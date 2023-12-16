import os
import json
from base64 import b64decode, b64encode
from typing import Any


def save_json(file: Any, name: str, path: str = "data/tmp/") -> str:
    os.makedirs(path, exist_ok=True)
    fullpath = f"{path}{name}.json"

    with open(fullpath, "w") as f:
        f.write(json.dumps(file))

    return fullpath


def load_json(name: str, path: str = "data/tmp/") -> Any:
    fullpath = f"{path}{name}.json"

    with open(fullpath, "r") as f:
        return json.load(f)


def decode_params(params: str | bytes) -> Any:
    """ Decode base64 encoded URL as json """
    return json.loads(b64decode(params).decode("utf-8"))


def encode_params(params: dict) -> str:
    """ Encode dictionary as base64 string """
    return b64encode(str(json.dumps(params).replace(" ", "")).encode()).decode(
        "utf-8"
    )