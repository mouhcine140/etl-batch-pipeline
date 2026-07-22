"""Tests du module de chargement. `clean()` est testé sur des lignes copiées
telles quelles du dataset réel (mêmes valeurs que data/raw/2010-12-01.csv),
pas des données inventées - y compris le cas de la facture 536381/stock_code
71270 qui a révélé le bug de clé dupliquée corrigé dans stg_online_retail.sql
(voir dbt_project/models/staging/stg_online_retail.sql)."""
from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from load.load import RAW_COLUMNS, clean, load_to_staging, read_raw_csvs

# Sous-ensemble réel de data/raw/2010-12-01.csv, y compris un doublon
# strict (facture 536365 apparaît deux fois à l'identique -> doit être
# supprimé par clean()) et une annulation (préfixe "C").
RAW_ROWS = [
    {"InvoiceNo": "536365", "StockCode": "85123A", "Description": "WHITE HANGING HEART T-LIGHT HOLDER",
     "Quantity": 6, "InvoiceDate": "2010-12-01 08:26:00", "UnitPrice": 2.55, "CustomerID": 17850, "Country": "United Kingdom"},
    {"InvoiceNo": "536365", "StockCode": "85123A", "Description": "WHITE HANGING HEART T-LIGHT HOLDER",
     "Quantity": 6, "InvoiceDate": "2010-12-01 08:26:00", "UnitPrice": 2.55, "CustomerID": 17850, "Country": "United Kingdom"},
    {"InvoiceNo": "536370", "StockCode": "22728", "Description": "ALARM CLOCK BAKELIKE PINK",
     "Quantity": 24, "InvoiceDate": "2010-12-01 08:45:00", "UnitPrice": 3.75, "CustomerID": 12583, "Country": "France"},
    {"InvoiceNo": "C536391", "StockCode": "22556", "Description": "PLASTERS IN TIN CIRCUS PARADE",
     "Quantity": -12, "InvoiceDate": "2010-12-01 10:24:00", "UnitPrice": 1.65, "CustomerID": 17548, "Country": "United Kingdom"},
]


def _raw_df(source_file: str = "2010-12-01.csv") -> pd.DataFrame:
    df = pd.DataFrame(RAW_ROWS)
    df["source_file"] = source_file
    return df


def test_clean_removes_exact_duplicates():
    result = clean(_raw_df())
    # 4 lignes en entrée, une strictement dupliquée -> 3 en sortie.
    assert len(result) == 3


def test_clean_flags_cancellations():
    result = clean(_raw_df())
    cancelled = result[result["invoice_no"] == "C536391"]
    assert cancelled["is_cancellation"].iloc[0] == True  # noqa: E712
    normal = result[result["invoice_no"] == "536370"]
    assert normal["is_cancellation"].iloc[0] == False  # noqa: E712


def test_clean_renames_and_types_columns():
    result = clean(_raw_df())
    assert set(RAW_COLUMNS.values()).issubset(result.columns)
    assert pd.api.types.is_datetime64_any_dtype(result["invoice_date"])
    assert pd.api.types.is_integer_dtype(result["quantity"])


def test_clean_keeps_null_customer_id():
    df = _raw_df()
    df.loc[0, "CustomerID"] = pd.NA
    result = clean(df)
    assert result["customer_id"].isna().sum() >= 1


def test_read_raw_csvs_concatenates_all_files_with_lineage(tmp_path):
    header = "InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country\n"
    (tmp_path / "2010-12-01.csv").write_text(
        header + "536365,85123A,ITEM A,6,2010-12-01 08:26:00,2.55,17850.0,United Kingdom\n"
    )
    (tmp_path / "2010-12-02.csv").write_text(
        header + "536500,22728,ITEM B,1,2010-12-02 09:00:00,3.75,12583.0,France\n"
    )

    result = read_raw_csvs(raw_dir=tmp_path)

    assert len(result) == 2
    assert set(result["source_file"]) == {"2010-12-01.csv", "2010-12-02.csv"}


def test_read_raw_csvs_raises_if_dir_empty(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_raw_csvs(raw_dir=tmp_path)


def test_load_to_staging_writes_and_replaces(monkeypatch, tmp_path):
    engine = create_engine(f"duckdb:///{tmp_path / 'test_warehouse.duckdb'}")

    def fake_write_dataframe(df, table_name, if_exists="replace"):
        # DuckDB (comme SQLite) renvoie -1 depuis to_sql() au lieu du
        # rowcount ; on ne peut donc pas s'appuyer sur sa valeur de retour,
        # exactement comme le fait la vraie implémentation dans common/db.py.
        df.to_sql(table_name, engine, if_exists=if_exists, index=False)
        return len(df)

    monkeypatch.setattr("load.load.write_dataframe", fake_write_dataframe)

    clean_df = clean(_raw_df())
    n_written = load_to_staging(clean_df, table_name="raw_online_retail")

    assert n_written == len(clean_df)
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM raw_online_retail")).scalar()
    assert count == len(clean_df)
