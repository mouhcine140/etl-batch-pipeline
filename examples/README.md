# Sorties d'un run réel

Ce dossier contient les sorties d'une exécution réelle de `bash scripts/run_pipeline_local.sh` sur l'échantillon de données réelles commité (`data/raw/2010-12-01.csv`), à titre de preuve. Régénérables à l'identique en relançant le script.

- `pipeline_run_stats.json` — comptage de lignes par table (staging + star schema), chiffre d'affaires net reconstitué, nombre de pays et d'annulations.
- `dbt_run_results.json` — sortie brute de `dbt run` (statut et durée par modèle).
- `dbt_test_results.json` — sortie brute de `dbt test` (19/19 tests passés : unicité, non-nullité, intégrité référentielle).
- `pytest_output.txt` — sortie de `pytest -v` (12/12 tests passés).

Ces chiffres reflètent un seul jour de données (349 lignes, 37 factures). Avec `extract.py` pointé sur une plage de dates plus large, ils montent en proportion — voir la section Résultats du README principal.
