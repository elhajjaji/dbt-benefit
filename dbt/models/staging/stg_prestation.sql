{{ config(tags=['prestation']) }}
-- Règles appliquées : RG_03, RG_04, RG_10, RG_15

with standardized as (

    {{ standardize_entity(
        source_relation = source('bronze', 'prestation'),
        partition_cols  = ['id'],
        order_by        = '_ingested_at desc',
        sk_cols         = ['id']
    ) }}

)

select
    sk_id,
    id,
    numero_individu,
    type_prestation,
    montant_prestation,
    date_debut_prestation,
    date_fin_prestation,
    date_prestation,

    -- RG_10 : type autorisé par la LASLP ? (domaine lu dans le contrat)
    ({{ sql_in_contract_domain('type_prestation', 'prestation', 'domaine_type_prestation') }})
        as is_type_valide,

    -- RG_15 : montant hors barème institutionnel ? (plafonds lus dans le contrat)
    {{ flag_montant_suspect('type_prestation', 'montant_prestation') }} as is_montant_suspect,

    _ingested_at,
    _source_system,
    _batch_id
from standardized
