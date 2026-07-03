{# ═════════════════════════════════════════════════════════════════════════
   Helpers de lecture du contrat de données (vars.contracts, dbt_project.yml)
   → aucune valeur métier codée en dur dans les modèles.
   ═════════════════════════════════════════════════════════════════════════ #}

{# Retourne le contrat d'une entité, ou une clé précise de ce contrat. #}
{% macro get_contract(entity, key=none) %}
    {% set contract = var('contracts')[entity] %}
    {% if key is none %}
        {% do return(contract) %}
    {% else %}
        {% do return(contract[key]) %}
    {% endif %}
{% endmacro %}

{# Expression SQL booléenne : la colonne appartient-elle au domaine du contrat ?
   Utilisée par RG_09 (permis OCPM) et RG_10 (types LASLP). #}
{% macro sql_in_contract_domain(column_name, entity, domain_key) %}
    {%- set allowed = get_contract(entity, domain_key) -%}
    {{ column_name }} in (
        {%- for value in allowed %}'{{ value }}'{% if not loop.last %}, {% endif %}{% endfor -%}
    )
{% endmacro %}

{# RG_15 — génère le CASE de plausibilité des montants à partir des plafonds
   du contrat. Ajouter un plafond dans dbt_project.yml suffit : aucun SQL
   à modifier. #}
{% macro flag_montant_suspect(type_col, montant_col) %}
    case
        {%- for type_prestation, plafond in get_contract('prestation', 'plafonds').items() %}
        when {{ type_col }} = '{{ type_prestation }}' and {{ montant_col }} > {{ plafond }} then true
        {%- endfor %}
        else false
    end
{% endmacro %}
