{{ config(severity=var('rg_severity', 'warn'), tags=['singular', 'rg23']) }}

-- RG_23 — aucune prestation ne doit référencer un individu inconnu.
-- Cas de test : P-013 verse à IND-999, absent du référentiel individu.

select
    p.numero_individu,
    count(*) as nb_prestations_orphelines
from {{ ref('stg_prestation') }} p
left join {{ ref('stg_individu') }} i
    on p.numero_individu = i.numero_individu
where i.numero_individu is null
group by p.numero_individu
