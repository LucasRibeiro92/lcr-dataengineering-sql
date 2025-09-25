from __future__ import annotations
import os
from dotenv import load_dotenv

# carrega .env assim que o módulo é importado
load_dotenv()

ENV_PREFIX = "DB_URL__"

def get_url(alias: str) -> str:
    key = f"{ENV_PREFIX}{alias.upper()}"
    url = os.getenv(key)
    if not url:
        # ajuda o debug: lista o que existe
        available = [k for k in os.environ.keys() if k.startswith(ENV_PREFIX)]
        raise RuntimeError(
            f"Variável {key} não definida. Disponíveis: {', '.join(available) or '(nenhuma)'}"
        )
    return url

def list_aliases() -> list[str]:
    return [k.removeprefix(ENV_PREFIX) for k in os.environ.keys() if k.startswith(ENV_PREFIX)]
