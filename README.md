# 🎮 Gaming Events — Real-Time Data Pipeline

Proyecto final de Herramientas de Visualización — UPY 2026  
Pipeline en tiempo real usando Docker, Kafka, Python e InfluxDB.

## Arquitectura
Producer (Python) → Kafka → Consumer (Python ETL) → InfluxDB → Dashboard (Plotly Dash)

## Dominio

Simulación de eventos de un servidor de videojuego multijugador.  
Cada 2 segundos se generan eventos de 5 jugadores activos con kills, muertes y daño causado.

## Esquema JSON (mensaje Kafka)

```json
{
  "source_id":    "player_01",
  "source_name":  "SnipeMaster",
  "timestamp":    "2026-04-21T15:30:00Z",
  "kills":        2,
  "deaths":       1,
  "damage_dealt": 340.5,
  "event_type":   "kill",
  "map_zone":     "norte"
}
```

## ETL — Decisiones del Consumer

| Paso | Descripción |
|------|-------------|
| Validación | Descarta eventos sin `source_id`, `timestamp`, `kills` o `damage_dealt` negativos |
| Transformación | Calcula `kd_ratio = kills / (deaths + 1)` |
| Alerta | `level = rampage` si `damage_dealt > 500`, `agresivo` si `> 250`, `normal` si no |

## Visualizaciones (Dashboard en localhost:8050)

| # | Tipo | Descripción |
|---|------|-------------|
| 1 | Bar chart | Kills por jugador clasificados por nivel |
| 2 | Bubble chart | Daño vs K/D Ratio, tamaño = kills |
| 3 | Line chart | Serie de tiempo de daño de un jugador seleccionado |

## Cómo correr el proyecto

```bash
docker compose up --build
```

Luego abre: [http://localhost:8050](http://localhost:8050)

## Verificación

```bash
# Ver todos los servicios corriendo
docker ps --format "table {{.Names}}\t{{.Status}}"

# Ver logs del producer
docker logs producer | tail -20

# Ver logs del consumer
docker logs consumer | tail -20
```

## Estructura del proyecto
gaming-pipeline/
├── docker-compose.yml
├── .env
├── README.md
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── simulator.py
├── consumer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── processor.py
├── dashboard/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py
└── infra/
└── init-kafka.sh

## Tecnologías

- Docker + Docker Compose
- Apache Kafka 7.5.0
- InfluxDB 2.7
- Python 3.11
- Plotly Dash 2.17.1