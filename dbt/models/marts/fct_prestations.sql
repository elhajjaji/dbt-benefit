{{ config(
    contract={'enforced': true},
    tags=['fait']
) }}
-- Règles appliquées : RG_04, RG_18, RG_20, RG_22 (coût journalier sécurisé),
--                     RG_25 (âge PRIME_LAMAL), RG_26 (versement après décès),
--                     RG_27 (prestation migrant sans permis actif)
-- Le contrat de colonnes dbt (contract: enforced) fige le schéma : tout
-- changement non déclaré casse le build en CI avant d'atteindre la production.

with prestations as (

    select * from {{ ref('stg_prestation') }}

),

individus as (

    select numero_individu, age_calcule, date_naissance, date_deces
    from {{ ref('stg_individu') }}

),

permis_valides as (

    select numero_individu, date_debut, date_fin_normalisee
    from {{ ref('stg_permis') }}
    where is_permis_valide

),

couverture as (

    select numero_individu, date_debut, date_fin_normalisee
    from {{ ref('stg_couverture_unifiee') }}

),

dossiers as (

    select numero_individu, numero_dossier, date_debut, date_fin_normalisee
    from {{ ref('stg_dossier') }}

),

enrichi as (

    select
        -- RG_04 : clé de la table de faits
        {{ dbt_utils.generate_surrogate_key(['p.id', 'p.numero_individu']) }} as pk_prestation,

        p.numero_individu,
        p.type_prestation,
        p.montant_prestation::numeric as montant_prestation,
        p.date_debut_prestation,
        p.date_fin_prestation,
        p.date_prestation,
        d.numero_dossier,

        -- RG_22 : coût journalier protégé contre la division par zéro —
        -- si date_debut = date_fin, la durée vaut 1 jour, pas 0
        round(({{ dbt_utils.safe_divide(
            'p.montant_prestation',
            'greatest((p.date_fin_prestation - p.date_debut_prestation) + 1, 1)'
        ) }})::numeric, 2) as montant_par_jour,

        -- RG_18 : versement adossé à une couverture active à la date du
        -- versement ? EXISTS pour éviter le fan-out (plusieurs couvertures)
        exists (
            select 1
            from couverture c
            where c.numero_individu = p.numero_individu
              and p.date_prestation between c.date_debut and c.date_fin_normalisee
        ) as is_conforme_couverture,

        -- RG_20 : senior + FORFAIT_ENTRETIEN = anomalie (seuil lu au contrat)
        coalesce(
            i.age_calcule >= {{ get_contract('individu', 'age_senior') }}
            and p.type_prestation = 'FORFAIT_ENTRETIEN',
            false
        ) as is_alerte_senior,

        -- RG_25 : PRIME_LAMAL réservée aux 18-25 ans (bornes lues au contrat,
        -- âge évalué à la date du versement, pas à aujourd'hui)
        coalesce(
            p.type_prestation = 'PRIME_LAMAL'
            and date_part('year', age(p.date_prestation, i.date_naissance))
                not between {{ get_contract('prestation', 'prime_lamal_age_min') }}
                        and {{ get_contract('prestation', 'prime_lamal_age_max') }},
            false
        ) as is_alerte_lamal_age,

        -- RG_26 : versement postérieur au décès du bénéficiaire
        coalesce(
            i.date_deces is not null and p.date_prestation > i.date_deces,
            false
        ) as is_verse_apres_deces,

        -- RG_27 : les types réservés aux migrants exigent un permis valide
        -- actif à la date du versement (types lus au contrat)
        (
            p.type_prestation in (
                {%- for t in get_contract('prestation', 'types_reserves_migrants') %}
                '{{ t }}'{{ "," if not loop.last }}
                {%- endfor %}
            )
            and not exists (
                select 1
                from permis_valides pv
                where pv.numero_individu = p.numero_individu
                  and p.date_prestation between pv.date_debut and pv.date_fin_normalisee
            )
        ) as is_migrant_sans_permis,

        p.is_type_valide,
        p.is_montant_suspect,

        -- Protection anti fan-out : un individu peut traverser plusieurs
        -- dossiers ; on attribue le dossier actif à la date du versement,
        -- et un seul (le plus récent en cas de chevauchement RG_13)
        row_number() over (
            partition by p.sk_id
            order by d.date_debut desc nulls last
        ) as rang_dossier

    from prestations p
    left join individus i
        on p.numero_individu = i.numero_individu
    left join dossiers d
        on p.numero_individu = d.numero_individu
        and p.date_prestation between d.date_debut and d.date_fin_normalisee

)

select
    pk_prestation,
    numero_individu,
    type_prestation,
    montant_prestation,
    date_debut_prestation,
    date_fin_prestation,
    date_prestation,
    numero_dossier,
    montant_par_jour,
    is_conforme_couverture,
    is_alerte_senior,
    is_alerte_lamal_age,
    is_verse_apres_deces,
    is_migrant_sans_permis,
    is_type_valide,
    is_montant_suspect
from enrichi
where rang_dossier = 1
