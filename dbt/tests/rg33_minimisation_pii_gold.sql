{{ config(severity='error', tags=['singular', 'rg33']) }}

-- RG_33 — minimisation (LPD/RGPD) : AUCUNE colonne nominative ne doit
-- exister en zone Gold, quelle que soit la valeur qu'elle contient.
-- Ce test interroge le CATALOGUE (information_schema), pas les données :
-- il attrape le développeur qui fait transiter nom/email/navs dans un mart.
-- Toujours bloquant. Liste des colonnes interdites lue dans le contrat.

select
    table_schema,
    table_name,
    column_name
from information_schema.columns
where table_schema = 'gold'
  and column_name in (
      {%- for col in get_contract('gold', 'colonnes_pii_interdites') %}
      '{{ col }}'{{ "," if not loop.last }}
      {%- endfor %}
  )
