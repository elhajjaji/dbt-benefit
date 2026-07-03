# Benefits Lakehouse — Atelier Contract-Driven

MVP Data Lakehouse ciblant l'analyse des données sociales : permettre à
l'institution qui les détient de piloter, contrôler et auditer ses versements
d'aide sociale. **35 règles de gestion** pilotées par un
contrat de données et exécutées par dbt, orchestrées par Airflow, observées
par Elementary et Marquez.

📖 **Le cahier d'atelier complet (3 journées) est dans [cahier_atelier.md](cahier_atelier.md)** (PDF : `make pdf`).
🧭 **Le pas-à-pas animateur avec toutes les commandes et corrigés : [guide_corrige.md](guide_corrige.md).**

## Démarrage rapide

```bash
# 1. Plateforme (Docker ≥ 24, ~13 conteneurs, 16 Go RAM recommandés)
cp .env.example .env
docker compose --profile "*" up -d          # ou : make up

# 2. Outillage Python
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pre-commit install

# 3. Pipeline dbt
make seed          # charge les 4 CSV de test en zone bronze
make unit          # unit tests dbt (logique sur données mockées)
make build         # modèles + tests (mode alerte)
make test-strict   # démo fail-fast : les RG bloquantes stoppent le pipeline
make snapshot      # historisation SCD2 des permis (RG_11)
make audit         # tableau de bord DPO : anomalies par RG (15 attendues)
make docs          # documentation interactive (http://localhost:8087)
make lineage       # dbt build instrumenté OpenLineage → Marquez
make report        # rapport qualité Elementary
make forget IND=IND-011   # RG_35 — droit à l'oubli (puis make seed pour restaurer)
```

## URLs de la plateforme

| Service | URL | Rôle |
|---|---|---|
| PostgreSQL source | `localhost:5433` (`pocs-postgres`, déjà disponible) | Base `pocs`, schéma `src` |
| pgAdmin | http://localhost:5050 | Exploration de la source |
| NiFi | https://localhost:8443/nifi | Ingestion batch |
| Redpanda Console | http://localhost:8086 | Topics CDC |
| MinIO Console | http://localhost:9001 | Stockage S3 |
| Nessie | http://localhost:19120 | Catalogue Iceberg versionné |
| Airflow | http://localhost:8090 | Orchestration (mdp : `docker logs benefits-airflow \| grep Password`) |
| Dremio | http://localhost:9047 | Moteur SQL lakehouse |
| Metabase | http://localhost:3002 | Dashboards |
| Marquez | http://localhost:3000 | Lignage OpenLineage |

Identifiants : voir `.env.example`.

## Arborescence

```
├── cahier_atelier.md          # cahier d'atelier (3 journées)
├── docker-compose.yml         # plateforme complète, profils sélectifs
├── Makefile                   # commandes usuelles (dont make pdf)
├── .pre-commit-config.yaml    # garde-fous locaux (sqlfluff, dbt parse…)
├── ingestion/                 # NiFi, Debezium, consommateur Redpanda
├── orchestration/airflow/     # DAG dbt (freshness → build → tests → docs)
├── dbt/
│   ├── dbt_project.yml        # ⭐ contrat machine-readable (vars.contracts)
│   ├── contracts/             # ⭐ contrats métier (BA, sans SQL)
│   ├── macros/                # moteur des RG + tests génériques + outils DPO
│   ├── models/staging/        # zone Silver
│   ├── models/marts/          # zone Gold (+ unit tests + exposures)
│   ├── models/docs.md         # doc blocks + page d'accueil dbt docs
│   ├── seeds/                 # données de test avec ~18 cas piégés
│   ├── snapshots/             # SCD2 permis (RG_11)
│   ├── tests/                 # tests singuliers (RG_13,16,23,24,29,33)
│   └── analyses/              # tableau de bord d'audit DPO
├── docs/pdf.css               # feuille de style du PDF
└── .github/workflows/
    ├── ci.yml                 # PR : lint → unit → build → test des tests → Pages
    └── nightly.yml            # nuit : fraîcheur, volumes, rapport Elementary
```

## Les 35 règles en un coup d'œil

- **Bronze** : traçabilité (RG_01), fraîcheur (02), volumétrie (30), anomalies apprises (31), dérive de schéma (32)
- **Silver** : dédup (03), clés (04), normalisations (05, 08), biographie (06), PII (07), domaines contract-driven (09, 10), SCD2 (11), couverture (12), chevauchements (13), NAVS (28)
- **Gold** : montants (14, 15), unicités (16, 17), couverture (18), cumuls (19), senior (20), calendrier (21), coût/jour (22), LAMAL (25), décès (26), migrants (27)
- **Transverse/Conformité** : orphelins (23), réconciliation (24), permis (29), minimisation PII (33), journal d'accès (34), droit à l'oubli (35)

Détail complet, mécanismes et fichiers : synthèse en fin de [cahier_atelier.md](cahier_atelier.md).
