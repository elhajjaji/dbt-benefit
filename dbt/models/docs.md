{% docs __overview__ %}

# Benefits Lakehouse — documentation du projet

Pipeline **contract-driven** d'analyse, de conformité et d'audit des versements
d'aide sociale d'une institution cantonale d'aide sociale. **35 règles de gestion**,
dont la partie déclarative (domaines, plafonds, seuils) vit dans le
contrat de données (`vars.contracts` de `dbt_project.yml`) et la partie
algorithmique dans les macros et tests singuliers.

## Architecture médaillon

| Zone | Schéma | Contenu | Règles |
|---|---|---|---|
| **Bronze** | `bronze` | Copie brute des sources + traçabilité | RG_01, RG_02, RG_30-32 |
| **Silver** | `silver` | Données standardisées, dédupliquées, PII maîtrisées | RG_03-13, RG_28 |
| **Gold** | `gold` | Faits, dimensions et agrégats prêts pour la BI | RG_14-22, RG_25-27 |
| **Audit** | `audit` | Journal des accès PII et des purges RGPD | RG_34, RG_35 |

## Où commencer

- La table de faits centrale : `fct_prestations`
- Le tableau de bord des anomalies : analyse `audit_tableau_de_bord_rg`
- Les contrats métier lisibles par le BA : dossier `contracts/`
- Les consommateurs déclarés : onglet **Exposures** (dashboard Metabase, rapport DPO)

*Cette page est générée depuis `models/docs.md` — la documentation est du
code : versionnée, revue en PR, jamais périmée.*

{% enddocs %}


{% docs type_prestation %}
Type de versement selon la LASLP. Domaine contrôlé par le contrat de données
(RG_10) : FORFAIT_ENTRETIEN, FORFAIT_INTEGRATION, LOYER, PRIME_LAMAL,
AIDE_URGENCE, AVANCE_RENTE, AIDE_EXCEPTIONNELLE, AIDE_MATERIELLE, FORFAIT_ETSP.
Particularités : le LOYER est versé au **dossier** (foyer), pas à l'individu
(RG_17) ; l'AIDE_URGENCE peut légitimement être versée plusieurs fois par mois ;
la PRIME_LAMAL est réservée aux 18-25 ans (RG_25).
{% enddocs %}

{% docs numero_individu %}
Identifiant métier du bénéficiaire (format `IND-xxx`). Les prestations sont
versées à l'individu — c'est la clé de jointure vers le référentiel
`stg_individu` (intégrité contrôlée par RG_23).
{% enddocs %}

{% docs numero_dossier %}
Identifiant du dossier **familial** (format `DOS-aaaa-nnn`). Tous les membres
d'un même foyer partagent ce numéro. Un individu ne peut être rattaché qu'à un
seul dossier actif à la fois (RG_13).
{% enddocs %}

{% docs ingested_at %}
Horodatage d'ingestion en zone Bronze (RG_01). Alimente la fraîcheur des
sources (RG_02), la déduplication technique (RG_03 — la version la plus
récente gagne) et la détection d'anomalies de volume (RG_31).
{% enddocs %}
