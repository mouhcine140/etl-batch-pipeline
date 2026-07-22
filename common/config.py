"""Configuration centralisée du pipeline, chargée depuis les variables
d'environnement (voir .env.example)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:  # pragma: no cover
    pass


def _database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    # Par défaut : DuckDB embarqué (fichier local, pas de serveur à lancer).
    # DuckDB est utilisé ici comme entrepôt analytique, pas seulement comme
    # store de dev : c'est le même moteur qui sert de cible dbt (voir
    # dbt_project/profiles.yml), donc pas d'écart entre l'environnement de
    # dev et celui décrit dans le repo.
    return f"duckdb:///{PROJECT_ROOT / 'warehouse.duckdb'}"


@dataclass(frozen=True)
class Settings:
    database_url: str = field(default_factory=_database_url)
    raw_data_dir: Path = PROJECT_ROOT / "data" / "raw"
    staging_table: str = "raw_online_retail"
    staging_schema: str = "main"


settings = Settings()
