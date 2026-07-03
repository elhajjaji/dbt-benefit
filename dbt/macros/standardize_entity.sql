{# ═════════════════════════════════════════════════════════════════════════
   RG_03 (déduplication technique) + RG_04 (clé surrogate déterministe)
   appliquées à n'importe quelle entité Bronze.
   ═════════════════════════════════════════════════════════════════════════ #}
{% macro standardize_entity(source_relation, partition_cols, order_by, sk_cols) %}

with source_data as (
    select * from {{ source_relation }}
),

deduplicated as (
    -- RG_03 : une seule ligne par clé métier, la plus récemment ingérée
    -- (NiFi et Redpanda peuvent livrer deux fois la même ligne)
    {{ dbt_utils.deduplicate(
        relation='source_data',
        partition_by=partition_cols | join(', '),
        order_by=order_by
    ) }}
)

select
    -- RG_04 : clé technique déterministe (identique en dev, recette, prod)
    {{ dbt_utils.generate_surrogate_key(sk_cols) }} as sk_id,
    *
from deduplicated

{% endmacro %}
