"""Tests du module d'extraction. Le réseau est mocké avec `responses` : on ne
veut pas dépendre de GitHub pour que la CI passe, et on veut pouvoir tester
les cas d'erreur (404, schéma inattendu) sans avoir à les provoquer côté
serveur réel."""
from __future__ import annotations

from datetime import date

import pytest
import responses

from extract.extract import BASE_URL, download_day, download_range

VALID_CSV = (
    b"InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country\n"
    b"536365,85123A,WHITE HANGING HEART T-LIGHT HOLDER,6,2010-12-01 08:26:00,2.55,17850.0,United Kingdom\n"
)

WRONG_SCHEMA_CSV = b"foo,bar\n1,2\n"


@responses.activate
def test_download_day_writes_file(tmp_path):
    day = date(2010, 12, 1)
    responses.add(
        responses.GET, f"{BASE_URL}/{day.isoformat()}.csv",
        body=VALID_CSV, status=200,
    )

    path = download_day(day, dest_dir=tmp_path)

    assert path.exists()
    assert path.read_bytes() == VALID_CSV


@responses.activate
def test_download_day_idempotent_no_network_call(tmp_path):
    day = date(2010, 12, 1)
    existing = tmp_path / f"{day.isoformat()}.csv"
    existing.write_bytes(VALID_CSV)

    # Aucune réponse enregistrée : si le code tentait un appel réseau, ce
    # test échouerait avec une ConnectionError levée par `responses`.
    path = download_day(day, dest_dir=tmp_path)

    assert path == existing


@responses.activate
def test_download_day_404_raises_file_not_found(tmp_path):
    day = date(2099, 1, 1)
    responses.add(
        responses.GET, f"{BASE_URL}/{day.isoformat()}.csv",
        status=404,
    )

    with pytest.raises(FileNotFoundError):
        download_day(day, dest_dir=tmp_path)


@responses.activate
def test_download_day_rejects_unexpected_schema(tmp_path):
    day = date(2010, 12, 1)
    responses.add(
        responses.GET, f"{BASE_URL}/{day.isoformat()}.csv",
        body=WRONG_SCHEMA_CSV, status=200,
    )

    with pytest.raises(ValueError, match="Schéma inattendu"):
        download_day(day, dest_dir=tmp_path)


@responses.activate
def test_download_range_skips_days_without_data(tmp_path):
    start, end = date(2010, 12, 1), date(2010, 12, 3)
    responses.add(responses.GET, f"{BASE_URL}/2010-12-01.csv", body=VALID_CSV, status=200)
    responses.add(responses.GET, f"{BASE_URL}/2010-12-02.csv", status=404)
    responses.add(responses.GET, f"{BASE_URL}/2010-12-03.csv", body=VALID_CSV, status=200)

    downloaded = download_range(start, end, dest_dir=tmp_path)

    assert len(downloaded) == 2
    assert {p.name for p in downloaded} == {"2010-12-01.csv", "2010-12-03.csv"}
