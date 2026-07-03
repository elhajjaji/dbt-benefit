{{ config(tags=['aggregat']) }}
-- Règle appliquée : RG_17 — unicité du LOYER par DOSSIER et par mois.
-- Le contrôle se fait au grain du foyer (dossier), pas de l'individu :
-- deux membres du même foyer ne peuvent pas toucher deux loyers le même mois.

select
    numero_dossier,
    date_trunc('month', date_prestation)::date as mois,
    count(*) as nb_loyers_du_mois,
    sum(montant_prestation) as total_loyer,
    count(*) > 1 as is_doublon_loyer
from {{ ref('fct_prestations') }}
where type_prestation = 'LOYER'
  and numero_dossier is not null
group by numero_dossier, date_trunc('month', date_prestation)::date
