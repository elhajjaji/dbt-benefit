{{ config(severity='error', tags=['singular', 'rg24']) }}

-- RG_24 — réconciliation volumétrique : moins de 10 % de perte entre la zone
-- Bronze et la table de faits Gold. La tolérance absorbe les suppressions
-- légitimes (doublons techniques RG_03).
-- Toujours bloquant : une perte massive signifie un pipeline cassé.

with bronze_count as (
    select count(*) as nb_bronze
    from {{ source('bronze', 'prestation') }}
),

gold_count as (
    select count(*) as nb_gold
    from {{ ref('fct_prestations') }}
),

ecart as (
    select
        b.nb_bronze,
        g.nb_gold,
        b.nb_bronze - g.nb_gold as lignes_perdues
    from bronze_count b
    cross join gold_count g
)

select *
from ecart
where lignes_perdues > nb_bronze * 0.10
