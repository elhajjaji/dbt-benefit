"""
Consommateur Redpanda → Iceberg (zone Bronze).

Lit les événements CDC publiés par Debezium dans les topics benefits.bronze.src.*,
ajoute les colonnes de traçabilité (RG_01) et écrit dans les tables Iceberg
cataloguées par Nessie.

Usage (depuis l'hôte, Redpanda exposé sur localhost:19092) :
    pip install confluent-kafka "pyiceberg[s3fs]"
    python consumer_bronze.py prestation
"""
import datetime
import json
import sys
import uuid

from confluent_kafka import Consumer
from pyiceberg.catalog import load_catalog

TABLE = sys.argv[1] if len(sys.argv) > 1 else "prestation"

catalog = load_catalog(
    "nessie",
    **{
        "uri": "http://localhost:19120/iceberg/main",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "minio",
        "s3.secret-access-key": "minio_secret",
    },
)

consumer = Consumer(
    {
        "bootstrap.servers": "localhost:19092",
        "group.id": "benefits-bronze-writer",
        "auto.offset.reset": "earliest",
    }
)
consumer.subscribe([f"benefits.bronze.src.{TABLE}"])

print(f"Consommation de benefits.bronze.src.{TABLE} … (Ctrl+C pour arrêter)")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        event = json.loads(msg.value())
        payload = event.get("after") or event.get("payload", {}).get("after")
        if payload is None:  # tombstone / delete : hors périmètre atelier
            continue

        # RG_01 — traçabilité d'ingestion
        payload["_ingested_at"] = datetime.datetime.utcnow().isoformat()
        payload["_source_system"] = "redpanda_cdc"
        payload["_batch_id"] = str(uuid.uuid4())

        table = catalog.load_table(f"bronze.{TABLE}")
        table.append([payload])
        print(f"→ bronze.{TABLE} : {payload}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
