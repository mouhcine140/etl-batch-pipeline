"""
Chargement des CSV bruts (data/raw/*.csv) vers une table de staging en base.

Le dataset "Online Retail" a deux particularités connues qu'on gère ici :
  - des lignes strictement dupliquées (même facture, même produit, même
    quantité...) - probablement des doubles scans côté système source ;
  - environ un quart des lignes sans CustomerID (commandes non rattachées à
    un compte client identifié).

On ne les "corrige" pas silencieusement : les doublons exacts sont retirés
(ce n'est pas une vente supplémentaire, c'est un artefact d'extraction), mais
les CustomerID manquants sont conservés tels quels - c'est une donnée réelle,
pas une erreur, et la couche dbt en aval décide comment la traiter (voir
dim_customers : un client "UNKNOWN" absorbe ces lignes plutôt que de les
exclure du chiffre d'affaires).
"""
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from common.config import settings
from common.db import write_dataframe

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

RAW_COLUMNS = {
    "InvoiceNo": "invoice_no",
    "StockCode": "stock_code",
    "Description": "description",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_date",
    "UnitPrice": "unit_price",
    "CustomerID": "customer_id",
    "Country": "country",
}


def read_raw_csvs(raw_dir: Path = settings.raw_data_dir) -> pd.DataFrame:
    csv_files = sorted(raw_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(
            f"Aucun fichier CSV dans {raw_dir}. Lancer extract.extract d'abord."
        )

    frames = []
    for path in csv_files:
        df = pd.read_csv(path, dtype={"CustomerID": "Int64"})
        df["source_file"] = path.name
        frames.append(df)
        logger.info("Lu %s (%d lignes)", path.name, len(df))

    return pd.concat(frames, ignore_index=True)


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=RAW_COLUMNS)

    n_before = len(df)
    df = df.drop_duplicates(subset=[c for c in df.columns if c != "source_file"])
    n_dupes = n_before - len(df)
    if n_dupes:
        logger.warning("%d lignes strictement dupliquées supprimées.", n_dupes)

    df["invoice_date"] = pd.to_datetime(df["invoice_date"], format="%Y-%m-%d %H:%M:%S")
    df["quantity"] = df["quantity"].astype(int)
    df["unit_price"] = df["unit_price"].astype(float)
    df["description"] = df["description"].str.strip()

    # Une facture commençant par "C" est une annulation (Quantity négatif) -
    # comportement documenté du dataset, pas une anomalie à filtrer.
    df["is_cancellation"] = df["invoice_no"].astype(str).str.startswith("C")

    n_null_customer = df["customer_id"].isna().sum()
    logger.info(
        "%d/%d lignes sans customer_id (%.1f%%) - conservées, traitées en aval par dbt.",
        n_null_customer, len(df), 100 * n_null_customer / len(df),
    )

    return df


def load_to_staging(df: pd.DataFrame, table_name: str = settings.staging_table) -> int:
    n_written = write_dataframe(df, table_name, if_exists="replace")
    logger.info("Table '%s' chargée : %d lignes.", table_name, n_written)
    return n_written


def run() -> dict:
    raw = read_raw_csvs()
    clean_df = clean(raw)
    n_written = load_to_staging(clean_df)
    return {
        "n_rows_raw": len(raw),
        "n_rows_loaded": n_written,
        "n_duplicates_removed": len(raw) - len(clean_df),
        "n_source_files": raw["source_file"].nunique(),
    }


if __name__ == "__main__":
    result = run()
    print(result)
