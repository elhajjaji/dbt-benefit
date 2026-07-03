{{ config(severity=var('rg_severity', 'warn'), tags=['singular', 'rg29']) }}

-- RG_29 — un individu ne peut pas détenir deux permis valides dont les
-- périodes se chevauchent (un seul statut légal de séjour à la fois).
-- Même macro que RG_13 : la règle algorithmique est écrite UNE fois,
-- appliquée à deux entités.
-- Cas de test : IND-006 cumule un permis B ouvert et un permis L ouvert.

{{ detect_overlapping_periods(
    relation     = ref('stg_permis'),
    key_column   = 'numero_individu',
    start_column = 'date_debut',
    end_column   = 'date_fin_normalisee'
) }}
