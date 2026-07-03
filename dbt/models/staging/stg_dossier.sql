{{ config(tags=['dossier']) }}
-- Règles appliquées : RG_03, RG_04, RG_08

with standardized as (

    {{ standardize_entity(
        source_relation = source('bronze', 'dossier'),
        partition_cols  = ['numero_dossier', 'numero_individu', 'date_debut'],
        order_by        = '_ingested_at desc',
        sk_cols         = ['numero_dossier', 'numero_individu', 'date_debut']
    ) }}

)

select
    sk_id,
    id,
    numero_dossier,
    numero_individu,
    date_debut,
    date_fin,

    -- RG_08 : rattachement encore actif → date_fin normalisée pour les BETWEEN
    {{ normalize_open_period('date_fin') }} as date_fin_normalisee,

    (date_fin is null or date_fin >= current_date) as is_actif,

    _ingested_at,
    _source_system,
    _batch_id
from standardized
