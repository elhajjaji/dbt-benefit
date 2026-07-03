{{ config(tags=['aggregat']) }}
-- Règles appliquées : RG_19 (plafond cumul mensuel), pivot BI par type
-- Grain : un individu × un mois.

with base as (

    select
        numero_individu,
        type_prestation,
        montant_prestation,
        date_trunc('month', date_prestation)::date as mois
    from {{ ref('fct_prestations') }}

),

mensuel as (

    select
        numero_individu,
        mois,
        sum(montant_prestation) as total_mensuel,
        count(*) as nb_versements,

        -- RG_19 : plafond de cumul mensuel lu dans le contrat → alerte DPO
        sum(montant_prestation)
            > {{ get_contract('prestation', 'plafond_cumul_mensuel_chf') }}
            as is_plafond_mensuel_depasse
    from base
    group by numero_individu, mois

),

-- Pivot contract-driven : une colonne par type de prestation pour Metabase.
-- La liste des colonnes EST le domaine du contrat : ajouter un type dans
-- dbt_project.yml crée la colonne au prochain build.
pivote as (

    select
        numero_individu,
        mois,
        {{ dbt_utils.pivot(
            column='type_prestation',
            values=get_contract('prestation', 'domaine_type_prestation'),
            agg='sum',
            then_value='montant_prestation',
            else_value=0,
            prefix='mnt_'
        ) }}
    from base
    group by numero_individu, mois

)

select
    m.numero_individu,
    m.mois,
    m.total_mensuel,
    m.nb_versements,
    m.is_plafond_mensuel_depasse,
    {%- for type_prestation in get_contract('prestation', 'domaine_type_prestation') %}
    p."mnt_{{ type_prestation }}"{{ "," if not loop.last }}
    {%- endfor %}
from mensuel m
left join pivote p
    using (numero_individu, mois)
