"""Accès base de données : moteur SQLAlchemy partagé par le module de
chargement et les tests."""
from __future__ import annotations

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from common.config import settings

_engine: Engine | None = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(settings.database_url)
    return _engine


def write_dataframe(df: pd.DataFrame, table_name: str, if_exists: str = "replace") -> int:
    engine = get_engine()
    df.to_sql(table_name, engine, if_exists=if_exists, index=False)
    return len(df)


def read_sql(query: str) -> pd.DataFrame:
    return pd.read_sql(query, get_engine())
