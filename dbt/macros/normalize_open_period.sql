{# RG_08 : un dossier/permis actif a date_fin = NULL.
   On normalise à 9999-12-31 (var date_fin_ouverte) pour fiabiliser
   les jointures BETWEEN. #}
{% macro normalize_open_period(column_name) %}
    coalesce({{ column_name }}, '{{ var("date_fin_ouverte") }}'::date)
{% endmacro %}
