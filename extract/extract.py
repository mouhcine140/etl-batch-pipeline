"""
Extraction des données brutes du dataset "Online Retail" (UCI Machine
Learning Repository) : transactions e-commerce réelles d'un grossiste
britannique en ligne, période 01/12/2010 - 09/12/2011.

Le dataset original est distribué en un seul fichier Excel par UCI. On
utilise ici le mirroir CSV, découpé par jour, publié dans le repo GitHub
officiel de "Spark: The Definitive Guide" (Databricks), qui référence ce
même jeu de données comme exemple pédagogique pour les pipelines batch :
https://github.com/databricks/Spark-The-Definitive-Guide/tree/master/data/retail-data/by-day

Aucune donnée n'est générée artificiellement : chaque ligne extraite est une
transaction réelle du dataset original (numéro de facture, code produit,
quantité, prix, client, pays).

Usage :
    python -m extract.extract --dates 2010-12-01 2010-12-02 2010-12-03
    python -m extract.extract --start 2010-12-01 --end 2010-12-08
"""
from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = (
    "https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/"
    "master/data/retail-data/by-day"
)
RAW_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
EXPECTED_COLUMNS = [
    "InvoiceNo", "StockCode", "Description", "Quantity",
    "InvoiceDate", "UnitPrice", "CustomerID", "Country",
]


def daterange(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _validate_header(content: bytes, day: date) -> None:
    first_line = content.split(b"\n", 1)[0].decode("utf-8").strip()
    header_cols = [c.strip() for c in first_line.split(",")]
    if header_cols != EXPECTED_COLUMNS:
        raise ValueError(
            f"Schéma inattendu pour {day.isoformat()} : {header_cols} "
            f"(attendu : {EXPECTED_COLUMNS})"
        )


def download_day(day: date, dest_dir: Path = RAW_DATA_DIR, overwrite: bool = False) -> Path:
    """Télécharge le CSV réel d'une journée. Idempotent : ne retélécharge pas
    un fichier déjà présent sauf si overwrite=True."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / f"{day.isoformat()}.csv"

    if dest_path.exists() and not overwrite:
        logger.info("Fichier déjà présent, on garde la copie locale : %s", dest_path.name)
        return dest_path

    url = f"{BASE_URL}/{day.isoformat()}.csv"
    logger.info("Téléchargement de %s", url)

    response = requests.get(url, timeout=30)
    if response.status_code == 404:
        raise FileNotFoundError(
            f"Pas de données pour {day.isoformat()} (pas de vente ce jour-là, "
            f"ou hors période couverte par le dataset)."
        )
    response.raise_for_status()

    _validate_header(response.content, day)
    dest_path.write_bytes(response.content)
    logger.info("OK : %s (%d octets)", dest_path.name, len(response.content))
    return dest_path


def download_range(start: date, end: date, dest_dir: Path = RAW_DATA_DIR) -> list[Path]:
    downloaded = []
    skipped = []
    for day in daterange(start, end):
        try:
            downloaded.append(download_day(day, dest_dir=dest_dir))
        except FileNotFoundError as exc:
            logger.warning("%s", exc)
            skipped.append(day)
    logger.info(
        "Extraction terminée : %d fichiers récupérés, %d jours sans données.",
        len(downloaded), len(skipped),
    )
    return downloaded


def local_raw_files(dest_dir: Path = RAW_DATA_DIR) -> list[Path]:
    """Fichiers réels déjà présents localement (utilisé quand le réseau n'est
    pas disponible, ex. environnement d'exécution restreint)."""
    return sorted(dest_dir.glob("*.csv"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dates", nargs="+", help="Liste de dates au format YYYY-MM-DD")
    parser.add_argument("--start", type=str, help="Date de début (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="Date de fin (YYYY-MM-DD)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.dates:
        days = [date.fromisoformat(d) for d in args.dates]
        for day in days:
            try:
                download_day(day)
            except FileNotFoundError as exc:
                logger.warning("%s", exc)
    elif args.start and args.end:
        download_range(date.fromisoformat(args.start), date.fromisoformat(args.end))
    else:
        existing = local_raw_files()
        if existing:
            logger.info(
                "Aucune date fournie. %d fichier(s) réel(s) déjà présent(s) "
                "dans data/raw/ : %s", len(existing), [f.name for f in existing],
            )
        else:
            logger.error(
                "Aucune date fournie et data/raw/ est vide. Utiliser --dates "
                "ou --start/--end, ou vérifier data/raw/2010-12-01.csv."
            )


if __name__ == "__main__":
    main()
