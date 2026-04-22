#!/bin/bash
# Espera a que Kafka esté listo y crea el topic del proyecto

echo "[KAFKA-INIT] Esperando a que Kafka esté disponible..."

until kafka-topics.sh --bootstrap-server kafka:9092 --list > /dev/null 2>&1; do
  sleep 3
done

echo "[KAFKA-INIT] Kafka listo. Creando topic 'gaming-events'..."

kafka-topics.sh --bootstrap-server kafka:9092 \
  --create \
  --if-not-exists \
  --topic gaming-events \
  --partitions 1 \
  --replication-factor 1

echo "[KAFKA-INIT] Topic creado exitosamente."
kafka-topics.sh --bootstrap-server kafka:9092 --list