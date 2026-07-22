"""
DAG orchestrant le pipeline ETL batch quotidien : extraction (téléchargement
des CSV réels du jour) -> chargement en base -> transformation dbt -> tests
dbt.

Contrairement à un pipeline où les données sont déjà disponibles au moment
du run, ici chaque exécution doit d'abord aller chercher le fichier du jour
avant de pouvoir charger quoi que ce soit - d'où l'étape extract en tête de
chaîne, avec gestion explicite du cas "pas de fichier pour cette date" (le
dataset source ne couvre qu'une période fixe, déc. 2010 à déc. 2011).
"""
from __future__ import annotations

from datetime import date as date_cls
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = "/opt/airflow/project/dbt_project"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="etl_batch_pipeline",
    description="Extraction Online Retail -> staging -> dbt (star schema) -> tests dbt",
    schedule="@daily",
    start_date=datetime(2010, 12, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "dbt", "online-retail"],
)
def etl_batch_pipeline():

    @task
    def extract(logical_date=None) -> list[str]:
        from extract.extract import download_day

        day = logical_date.date() if logical_date else date_cls.today()
        try:
            path = download_day(day)
            return [str(path)]
        except FileNotFoundError:
            # Pas de vente ce jour-là, ou date hors période couverte par le
            # dataset. On ne fait pas échouer le DAG pour autant : load()
            # retombera sur les fichiers déjà présents dans data/raw/.
            return []

    @task
    def load(_extracted_files: list[str]) -> dict:
        from load.load import run

        return run()

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir .",
    )

    extracted_files = extract()
    load_result = load(extracted_files)
    load_result >> dbt_run >> dbt_test


etl_batch_pipeline()
