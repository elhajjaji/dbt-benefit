# ── Atelier Benefits Lakehouse — commandes usuelles ────────────────────────
# Prérequis : cp .env.example .env  puis  pip install -r requirements.txt
.PHONY: up down ps deps seed build test test-strict unit snapshot docs report lineage audit freshen forget pdf clean

DBT = cd dbt && dbt
PSQL = docker exec -i $${SOURCE_CONTAINER:-pocs-postgres} psql -U $${POSTGRES_USER:-user} -d $${POSTGRES_DB:-pocs}

prep:          ## Crée les volumes locaux ./volumes/* (requis avant tout docker compose up)
	mkdir -p volumes/minio volumes/nifi volumes/redpanda volumes/dremio volumes/metabase volumes/airflow volumes/marquez-db
	chmod 777 volumes/*

up: prep       ## Démarre toute la plateforme
	docker compose --profile "*" up -d

down:          ## Arrête la plateforme (conserve les volumes)
	docker compose --profile "*" down

ps:            ## État des conteneurs
	docker compose --profile "*" ps

deps:          ## Installe les packages dbt (dbt_utils, dbt_expectations, elementary…)
	$(DBT) deps

seed: deps     ## Charge les 4 CSV de test dans la zone bronze
	$(DBT) seed --full-refresh

build:         ## Build complet : modèles + tests (mode alerte : les RG bloquantes ne cassent pas)
	$(DBT) build

test-strict:   ## Mode fail-fast : les RG bloquantes stoppent le pipeline (démo)
	$(DBT) build --vars '{rg_severity: error}'

test:          ## Tests uniquement
	$(DBT) test

unit:          ## Unit tests dbt : logique des modèles sur données mockées
	$(DBT) test --select "test_type:unit"

snapshot:      ## Historisation SCD2 des permis (RG_11)
	$(DBT) snapshot

docs:          ## Documentation interactive
	$(DBT) docs generate && $(DBT) docs serve --port 8087

report:        ## Rapport qualité Elementary (HTML)
	cd dbt && edr report

lineage:       ## dbt build instrumenté OpenLineage → Marquez (http://localhost:3000)
	cd dbt && dbt-ol build

audit:         ## Tableau de bord DPO des anomalies par RG
	$(DBT) compile --select audit_tableau_de_bord_rg -q
	$(PSQL) < dbt/target/compiled/benefits_lakehouse/analyses/audit_tableau_de_bord_rg.sql

freshen:       ## Simule une ingestion fraîche (réinitialise _ingested_at pour la démo freshness)
	$(PSQL) -c "update bronze.individu set _ingested_at = now(); update bronze.dossier set _ingested_at = now(); update bronze.permis set _ingested_at = now(); update bronze.prestation set _ingested_at = now();"

forget:        ## RG_35 — droit à l'oubli : make forget IND=IND-011
	$(DBT) run-operation rgpd_forget --args '{numero_individu: $(IND)}'
	$(DBT) build

# Build sous $HOME (chromium snap n'accède pas à /tmp) ; les liens relatifs
# sont réécrits vers GitHub (docs/pdf-links.lua) : aucun chemin local dans les PDF.
PDF_BUILD = $${HOME}/tmp_pdf

pdf:           ## Régénère cahier_atelier.pdf et guide_corrige.pdf depuis les markdown
	mkdir -p $(PDF_BUILD)
	pandoc cahier_atelier.md -f gfm -t html5 --standalone --embed-resources \
	  --lua-filter docs/pdf-links.lua \
	  --css docs/pdf.css --metadata pagetitle="Cahier d'Atelier — Benefits Lakehouse" \
	  -o $(PDF_BUILD)/cahier_atelier.html
	pandoc guide_corrige.md -f gfm -t html5 --standalone --embed-resources \
	  --lua-filter docs/pdf-links.lua \
	  --css docs/pdf.css --metadata pagetitle="Guide pratique — corrigé exécutable de l'atelier" \
	  -o $(PDF_BUILD)/guide_corrige.html
	cd $(PDF_BUILD) && chromium --headless=new --disable-gpu \
	  --no-pdf-header-footer --print-to-pdf=cahier_atelier.pdf \
	  "file://$(PDF_BUILD)/cahier_atelier.html"
	cd $(PDF_BUILD) && chromium --headless=new --disable-gpu \
	  --no-pdf-header-footer --print-to-pdf=guide_corrige.pdf \
	  "file://$(PDF_BUILD)/guide_corrige.html"
	cp $(PDF_BUILD)/cahier_atelier.pdf $(PDF_BUILD)/guide_corrige.pdf . && rm -rf $(PDF_BUILD)

clean:
	$(DBT) clean
