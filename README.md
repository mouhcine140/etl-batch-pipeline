# Pipeline ETL batch — Online Retail

Pipeline batch qui extrait des transactions e-commerce réelles, les charge dans un entrepôt DuckDB, puis les transforme en modèle dimensionnel (star schema) avec dbt — le tout orchestré par Airflow.

Deuxième projet d'un portfolio de 5 projets data engineering. Contrairement au premier projet (données synthétiques, l'accent était mis sur le pipeline ML), celui-ci utilise un **jeu de données réel** et met l'accent sur les compétences ETL/analytics engineering classiques : ingestion de fichiers, modélisation dimensionnelle, tests de qualité déclaratifs, orchestration.

## Démo

[**demo/demo_run.mp4**](demo/demo_run.mp4) — enregistrement vidéo d'une exécution réelle : `pytest -v` (12/12), puis le pipeline complet `extract` → `load` → `dbt run` → `dbt test` (19/19 tests dbt). GitHub affiche un lecteur vidéo intégré au clic sur le fichier dans le repo.

## Sommaire

- [Démo](#démo)
- [Source des données](#source-des-données)
- [Architecture](#architecture)
- [Stack technique](#stack-technique)
- [Modèle dimensionnel](#modèle-dimensionnel)
- [Structure du repo](#structure-du-repo)
- [Installation & lancement](#installation--lancement)
- [Détails par composant](#détails-par-composant)
- [Résultats](#résultats)
- [Tests](#tests)
- [Problèmes rencontrés](#problèmes-rencontrés)
- [Limites & pistes d'amélioration](#limites--pistes-damélioration)

## Source des données

[**Online Retail Dataset**](https://archive.ics.uci.edu/dataset/352/online+retail) (UCI Machine Learning Repository) : transactions réelles d'un grossiste en ligne britannique, 01/12/2010 au 09/12/2011 — factures, produits, quantités, prix, clients, pays. Aucune donnée n'est générée artificiellement.

Le fichier source UCI est un classeur Excel unique. Pour simuler un flux d'ingestion batch réaliste (un fichier par jour), ce projet consomme le même dataset découpé en CSV quotidiens, tel que publié dans le repo GitHub officiel de [*Spark: The Definitive Guide*](https://github.com/databricks/Spark-The-Definitive-Guide/tree/master/data/retail-data/by-day) (Databricks) — une référence pédagogique courante pour ce jeu de données en contexte "pipeline batch".

`data/raw/2010-12-01.csv` (349 lignes, 37 factures réelles) est commité comme échantillon de référence pour que les tests et `dbt run` fonctionnent immédiatement après un clone. `extract/extract.py` télécharge les jours supplémentaires à la demande.

## Architecture

```
                        ┌──────────────────────┐
                        │   Airflow DAG (@daily) │
                        └───────────┬────────────┘
                                    │
        ┌───────────┐   ┌──────────┴───────────┐   ┌─────────────┐   ┌─────────────┐
        │  Extract   │──▶│        Load          │──▶│  dbt run    │──▶│  dbt test   │
        │ (CSV réel) │   │ (staging DuckDB)      │   │ (star schema)│   │ (19 tests)  │
        └─────┬──────┘   └──────────┬───────────┘   └──────┬──────┘   └──────┬──────┘
              │                     │                       │                 │
              ▼                     ▼                       ▼                 ▼
        data/raw/*.csv      raw_online_retail        dim_customers      not_null /
        (source réelle,     (table de staging)        dim_products       unique /
         1 fichier/jour)                               dim_date          relationships
                                                         fact_order_lines
```

Chaque flèche correspond à une tâche Airflow indépendante (voir `dags/etl_pipeline_dag.py`) qui appelle soit une fonction Python testable en isolation (`extract/`, `load/`), soit une commande dbt.

## Stack technique

| Domaine | Outil | Pourquoi |
|---|---|---|
| Langage | Python 3.11 | Standard de l'écosystème data |
| Entrepôt analytique | DuckDB | Moteur colonne embarqué, aucun serveur à gérer, parfait pour un batch de cette taille — de plus en plus utilisé en prod pour ce cas d'usage exact |
| Transformation | dbt (dbt-duckdb) | Modélisation SQL versionnée, tests déclaratifs, lineage — standard du marché pour la couche transformation |
| Orchestration | Apache Airflow (TaskFlow API) | Scheduling, retries, monitoring des DAGs |
| Extraction | requests | Téléchargement HTTP simple, pas besoin d'un framework plus lourd pour un flux de fichiers quotidiens |
| Chargement | pandas + SQLAlchemy | Nettoyage tabulaire, écriture générique compatible DuckDB/Postgres selon `DATABASE_URL` |
| Tests | pytest (extraction/chargement) + tests dbt (modèle) | Deux couches de tests avec des responsabilités différentes, voir plus bas |
| Conteneurisation | Docker / docker-compose | Airflow + sa base de métadonnées Postgres |

## Modèle dimensionnel

Star schema classique, grain de la table de faits = une ligne de facture :

```
                    ┌─────────────────┐
                    │  dim_customers   │
                    │  customer_id (PK)│
                    │  country         │
                    │  first_seen_at   │
                    │  n_orders        │
                    └────────┬─────────┘
                             │
┌─────────────────┐   ┌─────┴──────────────┐   ┌──────────────────┐
│   dim_products   │   │  fact_order_lines   │   │     dim_date      │
│  stock_code (PK) │───│  order_line_id (PK) │───│   date_day (PK)   │
│  description     │   │  invoice_no          │   │   year/month/day  │
│  n_times_sold    │   │  customer_id (FK)    │   │   is_weekend       │
└──────────────────┘   │  stock_code (FK)     │   └──────────────────┘
                        │  date_day (FK)       │
                        │  quantity, unit_price│
                        │  line_amount         │
                        │  is_cancellation      │
                        └──────────────────────┘
```

`customer_id = 'UNKNOWN'` absorbe les commandes sans compte client identifié plutôt que de les exclure (voir [Problèmes rencontrés](#problèmes-rencontrés)). Les annulations (`is_cancellation = true`, quantité négative) sont conservées comme des lignes à part entière : les filtrer fausserait le chiffre d'affaires réellement encaissé.

## Structure du repo

```
etl-batch-pipeline/
├── .github/workflows/ci.yml    # CI : tests + pipeline complet sur l'échantillon réel
├── extract/
│   └── extract.py            # Téléchargement des CSV réels (idempotent)
├── load/
│   └── load.py                # Nettoyage + chargement en staging
├── common/
│   ├── config.py               # Configuration centralisée (DATABASE_URL...)
│   └── db.py                   # Moteur SQLAlchemy partagé
├── dbt_project/
│   ├── models/staging/         # stg_online_retail (1 ligne source = 1 ligne)
│   ├── models/marts/           # dim_customers, dim_products, dim_date, fact_order_lines
│   └── profiles.yml            # Cible DuckDB locale, pas de secret à gérer
├── dags/
│   └── etl_pipeline_dag.py     # DAG Airflow : extract -> load -> dbt run -> dbt test
├── tests/                      # Tests pytest (extraction mockée, nettoyage des données)
├── demo/
│   └── demo_run.mp4             # Enregistrement vidéo d'une exécution réelle (pytest + pipeline)
├── examples/                    # Sorties d'un run réel (preuve de fonctionnement)
├── data/raw/                   # 2010-12-01.csv (réel, commité) + jours téléchargés (ignorés)
├── scripts/
│   └── run_pipeline_local.sh   # Pipeline complet sans Airflow/Docker
├── docker-compose.yml           # Airflow (webserver + scheduler) + sa base Postgres
├── Dockerfile.airflow
├── requirements.txt
└── pytest.ini
```

## Installation & lancement

### Option A — Démo rapide en local (sans Docker)

Nécessite Python 3.11+.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

bash scripts/run_pipeline_local.sh
```

Ce script télécharge les jours manquants (ou garde l'échantillon commité s'il n'y a pas d'accès réseau), charge les données en base, lance les transformations dbt et leurs tests — dans cet ordre, avec les logs de chaque étape.

Pour explorer l'entrepôt ensuite :

```bash
python -c "import duckdb; duckdb.connect('warehouse.duckdb').sql('select * from fact_order_lines limit 10').show()"
```

Pour télécharger plus de jours réels (accès internet direct requis) :

```bash
python -m extract.extract --start 2010-12-01 --end 2010-12-08
```

### Option B — Orchestration complète (Docker + Airflow)

```bash
docker-compose up --build
```

- Airflow UI : http://localhost:8080 (admin / admin) — déclencher manuellement le DAG `etl_batch_pipeline`
- `warehouse.duckdb` est partagé entre le conteneur Airflow et l'hôte via un bind mount du repo entier — consultable directement depuis l'hôte pendant/après un run

## Détails par composant

**Extraction** (`extract/extract.py`) — Télécharge les CSV quotidiens réels depuis la source publique, valide le schéma attendu avant écriture (`_validate_header`), et est idempotent : un fichier déjà présent n'est pas retéléchargé. Gère explicitement le cas "pas de vente ce jour-là" (404) sans faire échouer le pipeline.

**Chargement** (`load/load.py`) — Concatène tous les CSV de `data/raw/`, supprime les doublons stricts (voir plus bas), type les colonnes, et charge en table de staging (`raw_online_retail`) en full-refresh. Un `source_file` par ligne conserve la traçabilité vers le fichier d'origine.

**Transformation** (`dbt_project/`) — Couche `staging` (1:1 avec la source, typage) puis couche `marts` (star schema). Chaque modèle mart est testé (unicité de clé, non-nullité, intégrité référentielle vers les dimensions).

**Orchestration** (`dags/etl_pipeline_dag.py`) — DAG TaskFlow API : `extract` (Python) → `load` (Python) → `dbt run` → `dbt test` (BashOperator). Le run échoue si les tests dbt échouent, ce qui bloque la propagation de données incohérentes en aval.

## Résultats

Sur l'échantillon réel commité (349 lignes, 1 jour, 37 factures) :

| Table | Lignes |
|---|---|
| `raw_online_retail` (staging) | 349 |
| `dim_customers` | 25 |
| `dim_products` | 243 |
| `fact_order_lines` | 349 |

Chiffre d'affaires net reconstitué (hors annulations) sur cet échantillon : **14 111,59 £**. Avec `extract.py` pointé sur une plage de dates plus large (accès internet direct), ces volumes montent en proportion — le dataset complet couvre ~540 000 lignes sur une période d'un an.

## Tests

Deux couches, deux responsabilités :

```bash
pytest              # extraction (réseau mocké) + logique de nettoyage
cd dbt_project && dbt test --profiles-dir .   # intégrité du modèle dimensionnel
```

`pytest` (12 tests) couvre le code Python : gestion des erreurs HTTP (404, schéma inattendu), idempotence du téléchargement, dédoublonnage et typage des données. Les cas de test du nettoyage reprennent des lignes réelles du dataset (pas de données inventées), y compris le cas qui a révélé le bug décrit ci-dessous.

`dbt test` (19 tests) couvre le résultat des transformations SQL : unicité des clés primaires, non-nullité, cohérence référentielle entre faits et dimensions.

## Problèmes rencontrés

**Clé de ligne de facture non unique.** Le dataset source n'a pas de numéro de ligne : `(invoice_no, stock_code, invoice_date)` semblait suffire à identifier une ligne, jusqu'à tomber sur la facture `536381` / produit `71270`, présente deux fois à la même seconde avec des quantités différentes (1 puis 3 unités) — probablement deux scans distincts en caisse. Le premier `dbt test` sur `fact_order_lines` a échoué sur `unique_fact_order_lines_order_line_id` à cause de ça. Corrigé en ajoutant un `row_number()` partitionné sur ces trois colonnes avant de générer la clé de substitution (`stg_online_retail.sql`).

**Clients non identifiés.** Sur ce dataset, une partie significative des commandes n'a pas de `CustomerID` (achats invités ou export incomplet côté source). Plutôt que de les exclure — ce qui sous-évaluerait le chiffre d'affaires réel — elles sont regroupées sous un client `'UNKNOWN'` dans `dim_customers`, visible et filtrable explicitement dans les analyses en aval.

**Lignes strictement dupliquées.** Le dataset est connu pour contenir des lignes identiques en tout point (probablement des doubles extractions côté système source). `load.py` les détecte et les supprime avant chargement, avec un compteur loggé — pas de correction silencieuse.

## Limites & pistes d'amélioration

- **Chargement full-refresh** : `load.py` recharge toute la table de staging à chaque run plutôt que de faire un chargement incrémental. Correct à ce volume, à revoir avec un vrai flux quotidien continu (stratégie incrémentale dbt, ou merge/upsert côté chargement).
- **DuckDB en fichier local, pas en service partagé** : suffisant pour ce projet et cohérent avec l'usage batch/analytique visé, mais un vrai déploiement multi-utilisateurs demanderait un entrepôt partagé (Postgres, Snowflake, BigQuery...) — le code SQLAlchemy est déjà compatible via `DATABASE_URL`, seul le profil dbt serait à changer.
- **dim_date en spine simple** : pas de jours fériés ni de calendrier fiscal ; à enrichir si des analyses business le demandent.
- **Réconciliation des annulations** : les avoirs (factures `C...`) ne sont pas rapprochés de leur commande d'origine faute d'identifiant fiable dans la source ; une vraie réconciliation demanderait une heuristique (même client, même produit, fenêtre de temps) hors scope ici.
