"""
DAG d'orchestration du pipeline Benefits Lakehouse.

Reproduit en production ce que `make build` fait à la main : fraîcheur des
sources (RG_02), build + tests, historisation SCD2 (RG_11), tests singuliers,
puis régénération de la documentation. La cadence (toutes les 4 h) fait écho
au batch NiFi.

Chaque tâche est un appel dbt distinct : en cas d'échec, Airflow montre
précisément QUELLE étape du contrat a rompu, et ne rejoue que la suite.
En vrai projet, préférer astronomer-cosmos (un task par modèle dbt).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT = "dbt --no-use-colors --project-dir /opt/benefits/dbt --profiles-dir /opt/benefits/dbt"

default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="benefits_lakehouse",
    description="Pipeline contract-driven : bronze → silver → gold + tests",
    schedule="0 */4 * * *",   # même cadence que le batch NiFi
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["benefits", "dbt", "contract-driven"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"{DBT} deps",
    )

    # RG_02 — première ligne de défense : des sources périmées stoppent le run
    source_freshness = BashOperator(
        task_id="source_freshness",
        bash_command=f"{DBT} source freshness",
    )

    # Atelier : les seeds simulent l'ingestion NiFi/Redpanda.
    # En production, cette tâche disparaît (NiFi/Redpanda alimentent Bronze).
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"{DBT} seed --full-refresh",
    )

    # Build + tests génériques + unit tests, en mode alerte
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build",
    )

    # RG_11 — historisation SCD2 des permis
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT} snapshot",
    )

    # RG_13, 16, 23, 24, 29, 33 — contrôles transverses
    dbt_test_singular = BashOperator(
        task_id="dbt_test_singular",
        bash_command=f"{DBT} test --select tag:singular",
    )

    # Documentation-as-code : régénérée à chaque run planifié
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"{DBT} docs generate",
    )

    dbt_deps >> source_freshness >> dbt_seed >> dbt_build
    dbt_build >> dbt_snapshot >> dbt_test_singular >> dbt_docs
