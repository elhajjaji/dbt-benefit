{# Test générique contract-driven : échoue pour chaque valeur hors du domaine
   déclaré dans le contrat (vars.contracts). Utilisé par RG_09 et RG_10.
   Usage dans schema.yml :
     - in_contract_domain:
         entity: permis
         domain_key: domaine_permis #}
{% test in_contract_domain(model, column_name, entity, domain_key) %}

select {{ column_name }} as valeur_hors_domaine
from {{ model }}
where {{ column_name }} is not null
  and not ({{ sql_in_contract_domain(column_name, entity, domain_key) }})

{% endtest %}
