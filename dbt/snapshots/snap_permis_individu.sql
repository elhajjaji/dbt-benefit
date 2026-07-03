{% snapshot snap_permis_individu %}

{{ config(
    unique_key='id',
    strategy='check',
    check_cols=['permis', 'date_debut', 'date_fin'],
) }}

-- RG_11 — historisation SCD2 des permis.
-- À chaque `dbt snapshot`, dbt compare l'état courant de chaque ligne de
-- permis avec la version précédente. Si permis, date_debut ou date_fin a
-- changé (renouvellement, changement de statut N→F→B…), une nouvelle version
-- est créée avec dbt_valid_from / dbt_valid_to.
--
-- Exemple : Ivan Petrov (IND-009) passe de N à F le 15/03/2022. Le snapshot
-- conserve les deux versions, chacune avec sa période de validité — le
-- parcours administratif reste auditable.

select * from {{ source('bronze', 'permis') }}

{% endsnapshot %}
