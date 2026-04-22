import json, time, random, os
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC        = os.getenv("KAFKA_TOPIC",  "gaming-events")
INTERVAL_SEC = 2

# ── Jugadores del servidor ─────────────────────────────────────
SOURCES = [
    {"id": "player_01", "name": "SnipeMaster"},
    {"id": "player_02", "name": "DarkWolf"},
    {"id": "player_03", "name": "BladeRunner"},
    {"id": "player_04", "name": "GhostX"},
    {"id": "player_05", "name": "IronFist"},
]

MAP_ZONES = ["norte", "sur", "este", "oeste", "centro"]
EVENT_TYPES = ["kill", "death", "assist", "objective"]

def generate_event(source: dict) -> dict:
    """
    Genera un evento de partida para un jugador.
    """
    return {
        "source_id":     source["id"],
        "source_name":   source["name"],
        "timestamp":     datetime.now(timezone.utc).isoformat(),
        "kills":         random.randint(0, 3),
        "deaths":        random.randint(0, 2),
        "damage_dealt":  round(random.uniform(0, 800), 1),
        "event_type":    random.choice(EVENT_TYPES),
        "map_zone":      random.choice(MAP_ZONES),
    }

def connect() -> KafkaProducer:
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode(),
                key_serializer=lambda k: k.encode(),
                acks="all", retries=3,
            )
            print(f"[PRODUCER] Conectado a {KAFKA_BROKER}")
            return p
        except NoBrokersAvailable:
            print("[PRODUCER] Esperando a Kafka...")
            time.sleep(5)

def main():
    producer = connect()
    print(f"[PRODUCER] Enviando a '{TOPIC}' cada {INTERVAL_SEC}s\n")
    while True:
        for source in SOURCES:
            event = generate_event(source)
            producer.send(TOPIC, key=source["id"], value=event)
            print(f"  → {event['source_id']} | {event['event_type']} | dmg: {event['damage_dealt']}")
        producer.flush()
        time.sleep(INTERVAL_SEC)

if __name__ == "__main__":
    main()