{{ config(materialized='table', tags=['couverture']) }}
-- Règle appliquée : RG_12
--
-- Dossier et permis forment UNE seule table de couverture : vérifier si un
-- individu était couvert à une date donnée (RG_18) = une seule jointure,
-- quelle que soit la nature de la couverture (aide sociale ou statut migrant).
--
-- Union explicite (colonnes alignées) plutôt que dbt_utils.union_relations :
-- les deux entités n'ont pas le même schéma et l'intention reste lisible.

select
    numero_individu,
    'dossier' as source_couverture,
    numero_dossier as reference_couverture,
    date_debut,
    date_fin_normalisee
from {{ ref('stg_dossier') }}

union all

select
    numero_individu,
    'permis' as source_couverture,
    permis as reference_couverture,
    date_debut,
    date_fin_normalisee
from {{ ref('stg_permis') }}
where is_permis_valide  -- un permis hors référentiel (RG_09) n'ouvre pas de droit
