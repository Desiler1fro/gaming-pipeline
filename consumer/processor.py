import json, os, time
from datetime import datetime, timezone
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

KAFKA_BROKER  = os.getenv("KAFKA_BROKER",  "kafka:9092")
TOPIC         = os.getenv("KAFKA_TOPIC",   "gaming-events")
INFLUX_URL    = os.getenv("INFLUX_URL",    "http://influxdb:8086")
INFLUX_TOKEN  = os.getenv("INFLUX_TOKEN",  "")
INFLUX_ORG    = os.getenv("INFLUX_ORG",    "")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "")

def validate(event: dict) -> bool:
    """Descarta eventos incompletos o con valores inválidos."""
    required = ["source_id", "timestamp", "kills", "damage_dealt"]
    if not all(k in event for k in required):
        return False
    if event["kills"] < 0 or event["damage_dealt"] < 0:
        return False
    return True

def transform(event: dict) -> dict:
    """
    ETL: enriquece el evento con campos calculados.
    - kd_ratio: kills / (deaths + 1)  →  evita división por cero
    - level: clasifica al jugador según daño causado
    - alert: bandera si el daño supera 500 (modo rampage)
    """
    event["kd_ratio"] = round(event["kills"] / (event["deaths"] + 1), 2)

    if event["damage_dealt"] > 500:
        event["level"] = "rampage"
        event["alert"] = True
    elif event["damage_dealt"] > 250:
        event["level"] = "agresivo"
        event["alert"] = False
    else:
        event["level"] = "normal"
        event["alert"] = False

    return event

def to_influx_point(event: dict) -> Point:
    """
    Mapea el evento a un punto de InfluxDB.
    Tags = dimensiones categóricas (para filtrar/agrupar).
    Fields = valores numéricos o texto a almacenar.
    """
    return (
        Point("gaming_events")
        .tag("source_id",  event["source_id"])
        .tag("level",      event.get("level", "normal"))
        .tag("event_type", event.get("event_type", "unknown"))
        .tag("map_zone",   event.get("map_zone", "unknown"))
        .field("kills",         int(event["kills"]))
        .field("deaths",        int(event["deaths"]))
        .field("damage_dealt",  float(event["damage_dealt"]))
        .field("kd_ratio",      float(event["kd_ratio"]))
        .field("alert",         bool(event["alert"]))
        .time(datetime.now(timezone.utc), WritePrecision.NS)
    )

def connect_kafka():
    while True:
        try:
            c = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda m: json.loads(m.decode()),
                group_id="processor-group",
                auto_offset_reset="latest",
            )
            print("[CONSUMER] Conectado a Kafka")
            return c
        except NoBrokersAvailable:
            print("[CONSUMER] Esperando a Kafka...")
            time.sleep(5)

def connect_influx():
    while True:
        try:
            client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
            client.ping()
            print("[CONSUMER] Conectado a InfluxDB")
            return client.write_api(write_options=SYNCHRONOUS)
        except Exception as e:
            print(f"[CONSUMER] Esperando a InfluxDB... ({e})")
            time.sleep(5)

def main():
    consumer  = connect_kafka()
    write_api = connect_influx()
    count = 0
    for msg in consumer:
        event = msg.value
        if not validate(event):
            print(f"  [!] Evento inválido descartado: {event}")
            continue
        event = transform(event)
        write_api.write(INFLUX_BUCKET, INFLUX_ORG, to_influx_point(event))
        count += 1
        print(f"  [{count}] {event['source_id']} | level: {event['level']} | kd: {event['kd_ratio']} | alert: {event['alert']}")

if __name__ == "__main__":
    main()