{{ config(severity=var('rg_severity', 'warn'), tags=['singular', 'rg13']) }}

-- RG_13 — un individu ne peut pas être rattaché à deux dossiers actifs en
-- même temps (double prise en charge).
-- Résultat attendu avec les seeds : IND-007 (Gisèle Moreau) apparaît deux
-- fois sur DOS-2022-099 avec des périodes ouvertes qui se chevauchent
-- → ce test remonte 1+ ligne (comportement voulu pour l'atelier).

{{ detect_overlapping_periods(
    relation     = ref('stg_dossier'),
    key_column   = 'numero_individu',
    start_column = 'date_debut',
    end_column   = 'date_fin_normalisee'
) }}
