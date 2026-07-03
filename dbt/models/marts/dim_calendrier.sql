{{ config(tags=['dimension']) }}
-- Règle appliquée : RG_21 (axe temps continu depuis le début d'activité)

with spine as (

    {{ dbt_utils.date_spine(
        datepart   = "day",
        start_date = "'" ~ var('date_debut_activite') ~ "'::date",
        end_date   = "(current_date + interval '1 day')::date"
    ) }}

)

select
    date_day::date                          as date_jour,
    extract(year from date_day)::int        as annee,
    extract(quarter from date_day)::int     as trimestre,
    extract(month from date_day)::int       as mois,
    to_char(date_day, 'TMMonth')            as nom_mois,
    extract(isodow from date_day)::int      as jour_semaine_iso,
    extract(isodow from date_day) in (6, 7) as is_weekend,
    date_trunc('month', date_day)::date     as premier_jour_mois
from spine
