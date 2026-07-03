{{ config(tags=['individu']) }}
-- Règles appliquées : RG_03, RG_04, RG_05, RG_06, RG_07, RG_28

with standardized as (

    {{ standardize_entity(
        source_relation = source('bronze', 'individu'),
        partition_cols  = ['numero_individu'],
        order_by        = '_ingested_at desc',
        sk_cols         = ['numero_individu']
    ) }}

),

mapped as (

    select
        sk_id,
        id,
        numero_individu,
        nom,
        prenom,
        email,
        navs,
        date_naissance,
        date_deces,

        -- RG_05 : normalisation du sexe
        case
            when sexe = 'H' then 'Homme'
            when sexe = 'F' then 'Femme'
            when sexe = 'X' then 'Non binaire'
            else 'Non spécifié'
        end as sexe_normalise,

        -- Âge courant (sert RG_20 en Gold)
        date_part('year', age(current_date, date_naissance))::int as age_calcule,

        -- RG_06 : cohérence biographique
        case
            when date_deces is not null and date_deces < date_naissance then false
            when date_naissance > current_date then false
            when date_naissance < '{{ get_contract("individu", "date_naissance_min") }}'::date then false
            else true
        end as is_dates_coherentes,

        -- RG_28 : format NAVS13 suisse (calculé AVANT le masquage PII,
        -- pour que le contrôle fonctionne quel que soit le rôle)
        coalesce(navs ~ '{{ get_contract("individu", "navs_regex") }}', false) as is_navs_valide,

        _ingested_at,
        _source_system,
        _batch_id
    from standardized

)

-- RG_07 : anonymisation PII — les colonnes nominatives ne sont exposées
-- qu'aux rôles habilités (var current_role, contrat individu.yml)
{% set role_habilite = var('current_role') in var('pii_roles_autorises') %}
select
    sk_id,
    id,
    numero_individu,
    {% if role_habilite %}
    nom,
    prenom,
    email,
    navs,
    {% else %}
    'MASQUÉ' as nom,
    'MASQUÉ' as prenom,
    'MASQUÉ' as email,
    'MASQUÉ' as navs,
    {% endif %}
    date_naissance,
    date_deces,
    sexe_normalise,
    age_calcule,
    is_dates_coherentes,
    is_navs_valide,
    _ingested_at,
    _source_system,
    _batch_id
from mapped
