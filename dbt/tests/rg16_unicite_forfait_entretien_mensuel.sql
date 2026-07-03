{{ config(severity=var('rg_severity', 'warn'), tags=['singular', 'rg16']) }}

-- RG_16 — un seul FORFAIT_ENTRETIEN par individu et par mois.
-- Cas de test : P-001 et P-010 versent deux forfaits à IND-001 en janvier.

select
    numero_individu,
    date_trunc('month', date_prestation)::date as mois,
    count(*) as nb_forfaits
from {{ ref('stg_prestation') }}
where type_prestation = 'FORFAIT_ENTRETIEN'
group by numero_individu, date_trunc('month', date_prestation)::date
having count(*) > 1
