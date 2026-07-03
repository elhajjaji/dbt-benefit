{# ═════════════════════════════════════════════════════════════════════════
   Outils DPO — RG_34 (journalisation des accès PII) et RG_35 (droit à
   l'oubli). Règles de CONFORMITÉ : elles ne transforment pas la donnée,
   elles gouvernent son accès et son cycle de vie.
   ═════════════════════════════════════════════════════════════════════════ #}

{# RG_34 — appelée en on-run-end (dbt_project.yml) : chaque exécution dbt est
   journalisée avec le rôle courant et le fait que les PII étaient exposées
   ou masquées. Le DPO peut auditer QUI a construit QUOI avec QUEL niveau
   d'accès, sur toute la profondeur d'historique. #}
{% macro log_pii_access(results) %}
    {% if execute %}
        {% set role = var('current_role') %}
        {% set pii_exposees = role in var('pii_roles_autorises') %}
        {% do run_query("create schema if not exists audit") %}
        {% do run_query("""
            create table if not exists audit.pii_access_log (
                invocation_id  text,
                executed_at    timestamp default now(),
                commande       text,
                role_courant   text,
                pii_exposees   boolean,
                nb_noeuds      int
            )
        """) %}
        {% do run_query(
            "insert into audit.pii_access_log
                 (invocation_id, commande, role_courant, pii_exposees, nb_noeuds)
             values ('" ~ invocation_id ~ "', '" ~ flags.WHICH ~ "', '" ~ role
                 ~ "', " ~ pii_exposees ~ ", " ~ (results | length) ~ ")"
        ) %}
        {{ log("RG_34 — accès journalisé : role=" ~ role ~ ", pii_exposees=" ~ pii_exposees, info=true) }}
    {% endif %}
{% endmacro %}


{# RG_35 — droit à l'oubli (art. 32 LPD / art. 17 RGPD) : purge un individu
   de la zone Bronze puis laisse le prochain build propager la disparition
   vers Silver et Gold. La purge est elle-même journalisée (sans PII !).

   Usage :
     dbt run-operation rgpd_forget --args '{numero_individu: IND-011}'
     dbt build        # propage l'oubli vers silver et gold #}
{% macro rgpd_forget(numero_individu) %}
    {% if execute %}
        {% do run_query("create schema if not exists audit") %}
        {% do run_query("""
            create table if not exists audit.rgpd_forget_log (
                numero_individu text,
                requested_at    timestamp default now(),
                invocation_id   text
            )
        """) %}
        {% for table_bronze in ['prestation', 'permis', 'dossier', 'individu'] %}
            {% set res = run_query(
                "delete from bronze." ~ table_bronze
                ~ " where numero_individu = '" ~ numero_individu ~ "'"
            ) %}
            {{ log("RG_35 — purge bronze." ~ table_bronze ~ " pour " ~ numero_individu, info=true) }}
        {% endfor %}
        {% do run_query(
            "insert into audit.rgpd_forget_log (numero_individu, invocation_id)
             values ('" ~ numero_individu ~ "', '" ~ invocation_id ~ "')"
        ) %}
        {{ log("RG_35 — oubli de " ~ numero_individu ~ " journalisé. Lancer `dbt build` pour propager.", info=true) }}
    {% endif %}
{% endmacro %}
