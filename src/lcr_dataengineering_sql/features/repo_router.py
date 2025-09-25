from __future__ import annotations
from typing import Mapping
from ..core.ports import Db
from .repo import Repo

class RepoRouter:
    def __init__(self, router: Mapping[str, Db]):
        self._router = router

    def for_db(self, alias: str) -> Repo:
        try:
            db = self._router[alias]
        except KeyError:
            raise KeyError(f"Alias '{alias}' não registrado no db_router.")
        return Repo(db)
