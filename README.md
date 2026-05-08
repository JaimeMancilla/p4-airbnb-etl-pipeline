# P4 · Pipeline ETL + Airflow + dbt + Cloud + Big Data
## Airbnb Buenos Aires — arquitectura medallón completa

![Python](https://img.shields.io/badge/Python-3.8-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.8.1-017CEE)
![dbt](https://img.shields.io/badge/dbt-1.7.0-FF694B)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791)
![Docker](https://img.shields.io/badge/Docker-compose-2496ED)
![GCS](https://img.shields.io/badge/GCS-Parquet-4285F4)
![PySpark](https://img.shields.io/badge/PySpark-4.1-E25A1C)

## Objetivo

Construir un stack de datos completo con arquitectura medallón
(Bronze → Silver → Gold) orquestado con Airflow, transformado con
dbt y almacenado en GCS como Parquet. El proyecto más diferenciador
del portafolio: muy pocos Data Scientists junior lo tienen.

## Arquitectura medallón

```
Airflow (orquestador)
├── extract_airbnb   → listings_clean.csv desde P1
├── extract_clima    → API Open-Meteo (99 días de clima)
├── extract_bcra     → API Bluelytics (90 días tipo de cambio)
└── load_to_postgres → carga las 3 fuentes a PostgreSQL

dbt (transformación)
├── Bronze  → 3 vistas sobre datos crudos (sin transformar)
├── Silver  → 3 tablas limpias con tipos correctos y validadas
└── Gold    → 1 tabla analítica con JOIN de las 3 fuentes

GCS (data lake)
└── 3 archivos Parquet en gs://portfolio-ds-jaime/p4-airbnb-etl/
```

## Fuentes de datos

| Fuente | Filas | Período |
|--------|-------|---------|
| Airbnb Buenos Aires (P1) | 27.348 | Enero 2026 |
| Open-Meteo API (clima) | 99 | Feb–May 2026 |
| Bluelytics API (USD/ARS) | 90 | Dic 2025–May 2026 |

## Modelo Gold: airbnb_analytics

27.348 propiedades enriquecidas con contexto económico y climático
de enero 2026 (tipo de cambio oficial promedio: 1.471 ARS).

| Columna | Descripción |
|---------|-------------|
| id, neighbourhood_cleansed, room_type | Datos de la propiedad |
| review_scores_rating, reviews_per_month | Métricas de reseñas |
| usd_oficial_venta_prom | TC oficial promedio enero 2026 |
| usd_blue_venta_prom | TC blue promedio enero 2026 |
| temp_max_prom, precipitacion_prom | Clima promedio enero 2026 |

## Tests de calidad dbt

| Resultado | Tests |
|-----------|-------|
| ✅ PASS=9 | not_null, unique, accepted_values |
| ⚠️ WARN=1 | review_scores_rating (3.303 nulos esperados — propiedades sin reseñas) |

## Parquet vs CSV

| Archivo | CSV | Parquet | Reducción |
|---------|-----|---------|-----------|
| listings | 47.6 MB | 13.5 MB | 72% |
| clima | ~5 KB | 4.3 KB | — |
| tipo_cambio | ~5 KB | 5.0 KB | — |

## Comparativa pandas vs PySpark (47MB)

| Métrica | pandas | PySpark |
|---------|--------|---------|
| Inicio sesión | 0.00s | 1.95s |
| Carga de datos | 0.78s | 3.82s |
| Análisis groupby | 0.01s | 1.05s |

Para 47MB pandas es 5x más rápido. PySpark tiene sentido a partir
de datasets > 10GB o procesamiento distribuido en cluster.

## Estructura

```
p4-airbnb-etl-pipeline/
├── dags/
│   └── etl_pipeline.py          ← DAG con 4 tareas
├── dbt/
│   ├── profiles.yml
│   └── airbnb_pipeline/
│       └── models/
│           ├── bronze/           ← stg_listings, stg_clima, stg_tipo_cambio
│           ├── silver/           ← listings_clean, clima_clean, tipo_cambio_clean
│           └── gold/             ← airbnb_analytics
├── notebooks/
│   └── 04_pyspark_intro.ipynb   ← comparativa pandas vs PySpark
├── src/
│   └── upload_to_gcs.py         ← subida a GCS en Parquet
├── data/raw/                    ← CSVs extraídos (ignorado por git)
└── docker-compose.yml
```

## Cómo reproducir

```bash
# 1. Clonar el repositorio
git clone https://github.com/JaimeMancilla/p4-airbnb-etl-pipeline.git
cd p4-airbnb-etl-pipeline

# 2. Levantar el stack
docker-compose up -d

# 3. Triggerear el DAG en localhost:8080
# DAG: etl_airbnb_pipeline → activar y triggerear

# 4. Correr modelos dbt
docker exec -it p4-airbnb-etl-pipeline-dbt-1 bash
cd airbnb_pipeline && dbt run && dbt test

# 5. Subir a GCS
python src/upload_to_gcs.py
```

## Decisiones técnicas

- **CROSS JOIN en Gold:** el scrape de Airbnb es puntual (enero 2026),
  se usa el promedio mensual de clima y TC como contexto del período
- **severity: warn en review_scores_rating:** los nulos son propiedades
  sin reseñas, dato válido que no debe eliminarse
- **engine.begin() + CASCADE:** necesario para DROP TABLE con vistas
  dependientes de dbt sin perder atomicidad

## Stack

Python · Airflow · dbt · PostgreSQL · Docker · GCS · Parquet · PySpark
