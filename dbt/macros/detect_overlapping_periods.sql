{# RG_13 — retourne les paires de périodes qui se chevauchent pour la même
   clé. 0 ligne = aucun chevauchement = test réussi.
   Règle ALGORITHMIQUE : elle vit en macro, le contrat YAML n'en porte que
   l'intention. #}
{% macro detect_overlapping_periods(relation, key_column, start_column, end_column) %}

select
    a.{{ key_column }},
    a.{{ start_column }} as periode_a_debut,
    a.{{ end_column }}   as periode_a_fin,
    b.{{ start_column }} as periode_b_debut,
    b.{{ end_column }}   as periode_b_fin
from {{ relation }} a
inner join {{ relation }} b
    on  a.{{ key_column }}    = b.{{ key_column }}
    and a.{{ start_column }}  < b.{{ start_column }}
    and a.{{ end_column }}   >= b.{{ start_column }}

{% endmacro %}
