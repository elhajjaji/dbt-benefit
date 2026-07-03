-- Tableau de bord DPO : nombre d'anomalies par règle de gestion.
-- Compiler avec `dbt compile --select audit_tableau_de_bord_rg`
-- puis exécuter le SQL compilé (make audit).

select 'RG_06 — Incohérence biographique' as regle,
       count(*) as nb_anomalies
from {{ ref('stg_individu') }}
where not is_dates_coherentes

union all
select 'RG_09 — Permis hors référentiel OCPM',
       count(*)
from {{ ref('stg_permis') }}
where not is_permis_valide

union all
select 'RG_10 — Type de prestation hors LASLP',
       count(*)
from {{ ref('stg_prestation') }}
where not is_type_valide

union all
select 'RG_14 — Montant non strictement positif',
       count(*)
from {{ ref('stg_prestation') }}
where montant_prestation <= 0

union all
select 'RG_15 — Montant suspect (hors barème)',
       count(*)
from {{ ref('fct_prestations') }}
where is_montant_suspect

union all
select 'RG_16 — Forfait entretien multiple dans le mois',
       count(*)
from (
    select numero_individu
    from {{ ref('stg_prestation') }}
    where type_prestation = 'FORFAIT_ENTRETIEN'
    group by numero_individu, date_trunc('month', date_prestation)
    having count(*) > 1
) rg16

union all
select 'RG_17 — Doublon de LOYER par dossier',
       count(*)
from {{ ref('agg_loyers_dossier_mensuel') }}
where is_doublon_loyer

union all
select 'RG_18 — Versement sans couverture active',
       count(*)
from {{ ref('fct_prestations') }}
where not is_conforme_couverture

union all
select 'RG_19 — Dépassement de plafond mensuel',
       count(*)
from {{ ref('agg_prestations_mensuelles') }}
where is_plafond_mensuel_depasse

union all
select 'RG_20 — Forfait entretien versé à un senior',
       count(*)
from {{ ref('fct_prestations') }}
where is_alerte_senior

union all
select 'RG_25 — PRIME_LAMAL hors tranche 18-25 ans',
       count(*)
from {{ ref('fct_prestations') }}
where is_alerte_lamal_age

union all
select 'RG_26 — Versement postérieur au décès',
       count(*)
from {{ ref('fct_prestations') }}
where is_verse_apres_deces

union all
select 'RG_27 — Prestation migrant sans permis actif',
       count(*)
from {{ ref('fct_prestations') }}
where is_migrant_sans_permis

union all
select 'RG_28 — NAVS hors format NAVS13',
       count(*)
from {{ ref('stg_individu') }}
where not is_navs_valide

union all
select 'RG_29 — Permis en chevauchement',
       count(*)
from (
    {{ detect_overlapping_periods(
        relation     = ref('stg_permis'),
        key_column   = 'numero_individu',
        start_column = 'date_debut',
        end_column   = 'date_fin_normalisee'
    ) }}
) rg29

order by regle
