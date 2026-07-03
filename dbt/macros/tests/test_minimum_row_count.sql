{# RG_30 — test générique contract-driven de volumétrie minimale.
   Le seuil vit dans le contrat (vars.contracts.bronze.volume_minimum) :
   une table Bronze anormalement vide signale une ingestion cassée AVANT
   que les zones aval ne soient reconstruites sur du vide.
   Usage (sources.yml) :
     - minimum_row_count:
         entity: prestation #}
{% test minimum_row_count(model, entity) %}

{% set volume_min = var('contracts')['bronze']['volume_minimum'][entity] %}

select
    count(*) as nb_lignes,
    {{ volume_min }} as volume_minimum_attendu
from {{ model }}
having count(*) < {{ volume_min }}

{% endtest %}
