# Flux NiFi — chargement batch Bronze

Pipeline planifié (toutes les 4 h) qui copie les 4 tables `src.*` de PostgreSQL
vers les tables Iceberg `bronze.*` (MinIO + Nessie), sans transformation métier.

## Chaîne de processeurs (un groupe par table)

```
[QueryDatabaseTable]  →  [UpdateRecord]  →  [ConvertAvroToParquet]  →  [PutIceberg]
   (PostgreSQL src)      (colonnes RG_01)                              (MinIO/Nessie)
```

## Configuration clé

### QueryDatabaseTable
| Propriété | Valeur |
|---|---|
| Database Connection Pooling Service | `DBCPConnectionPool` → `jdbc:postgresql://postgres:5432/pocs` |
| Table Name | `src.individu` (idem dossier, permis, prestation) |
| Maximum-value Columns | `id` (chargement incrémental) |
| Scheduling | `0 0 */4 * * ?` (cron, toutes les 4 h) |

### UpdateRecord — traçabilité RG_01
| Propriété (Replacement Value Strategy = Literal Value) | Valeur |
|---|---|
| `/_ingested_at` | `${now():format('yyyy-MM-dd HH:mm:ss')}` |
| `/_source_system` | `postgres_src` |
| `/_batch_id` | `${UUID()}` |

### PutIceberg
| Propriété | Valeur |
|---|---|
| Catalog Service | `NessieCatalogService` → `http://nessie:19120/api/v1` |
| Warehouse | `s3a://warehouse` (MinIO : `http://minio:9000`, clés du `.env`) |
| Table Name | `bronze.individu` (idem pour les 3 autres) |
| Write Mode | `APPEND` |

## Accès
UI NiFi : https://localhost:8443/nifi — identifiants dans `.env`
(`NIFI_USER` / `NIFI_PASSWORD`).

> En atelier, si NiFi n'est pas démarré, l'équivalent fonctionnel est fourni par
> `dbt seed` (les CSV `dbt/seeds/*.csv` portent déjà les colonnes RG_01).
