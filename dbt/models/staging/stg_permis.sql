{{ config(tags=['permis']) }}
-- Règles appliquées : RG_03, RG_04, RG_08, RG_09

with standardized as (

    {{ standardize_entity(
        source_relation = source('bronze', 'permis'),
        partition_cols  = ['numero_individu', 'permis', 'date_debut'],
        order_by        = '_ingested_at desc',
        sk_cols         = ['numero_individu', 'permis', 'date_debut']
    ) }}

)

select
    sk_id,
    id,
    numero_individu,
    permis,
    date_debut,
    date_fin,

    -- RG_08 : permis encore actif → date_fin normalisée
    {{ normalize_open_period('date_fin') }} as date_fin_normalisee,

    (date_fin is null or date_fin >= current_date) as is_actif,

    -- RG_09 : le type de permis appartient-il au référentiel OCPM du contrat ?
    ({{ sql_in_contract_domain('permis', 'permis', 'domaine_permis') }}) as is_permis_valide,

    _ingested_at,
    _source_system,
    _batch_id
from standardized
