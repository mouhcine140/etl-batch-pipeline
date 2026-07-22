#!/usr/bin/env bash
# Lance le pipeline complet en local, sans Airflow ni docker : extraction ->
# chargement -> transformation dbt -> tests dbt. Utile pour développer et
# déboguer rapidement avant de passer par docker-compose.
set -euo pipefail

cd "$(dirname "$0")/.."
export PYTHONPATH=.

echo "== 1/4 Extraction (télécharge les jours manquants, garde les fichiers déjà présents) =="
python -m extract.extract || true

echo "== 2/4 Chargement en base (DuckDB) =="
python -m load.load

echo "== 3/4 Transformation dbt (star schema) =="
cd dbt_project
dbt run --profiles-dir .

echo "== 4/4 Tests dbt (schéma + intégrité référentielle) =="
dbt test --profiles-dir .

echo ""
echo "Pipeline terminé. warehouse.duckdb contient raw_online_retail + le star schema."
