from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine

# ── argumentos por defecto para todas las tareas ──────────────────
default_args = {
    "owner": "jaime",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

DATA_RAW = Path("/opt/airflow/data/raw")
DATA_RAW.mkdir(parents=True, exist_ok=True)

def extract_airbnb():
    """Copia el CSV limpio de P1 a la carpeta raw del pipeline"""
    import shutil
    origen = Path("/opt/airflow/data/processed/listings_clean.csv")
    destino = DATA_RAW / "listings_clean.csv"
    shutil.copy(origen,destino)
    print(f"Airbnb: {destino} copiado correctamente")

def extract_clima():
    """Extrae datos de clima de Buenos Aires desde Open-Meteo API."""
    url = (
        "https://api.open-meteo.com/v1/forecast"
        "?latitude=-34.6037&longitude=-58.3816"
        "&daily=temperature_2m_max,temperature_2m_min,precipitation_sum"
        "&timezone=America/Argentina/Buenos_Aires"
        "&past_days=30"
    )
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()
    df = pd.DataFrame({
        "fecha": data["daily"]["time"],
        "temp_max": data["daily"]["temperature_2m_max"],
        "temp_min": data["daily"]["temperature_2m_min"],
        "precipitacion": data["daily"]["precipitation_sum"],
    })
    df.to_csv(DATA_RAW / "clima_buenos_aires.csv", index=False)
    print(f"Clima: {len(df)} días extraídos")
    
def extract_bcra():
    """Extrae tipo de cambio USD/ARS desde Bluelytics API."""
    url = "https://api.bluelytics.com.ar/v2/latest"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()
    df = pd.DataFrame([{
        "fecha": data["last_update"],
        "usd_oficial_compra": data["oficial"]["value_buy"],
        "usd_oficial_venta": data["oficial"]["value_sell"],
        "usd_blue_compra": data["blue"]["value_buy"],
        "usd_blue_venta": data["blue"]["value_sell"],
    }])
    df.to_csv(DATA_RAW / "tipo_cambio.csv", index=False)
    print(f"BCRA: tipo de cambio extraído — oficial {data['oficial']['value_avg']} ARS")

def load_to_postgres():
    """Carga los CSVs de data/raw/ a tablas en PostgreSQL"""
    engine = create_engine(
        "postgresql+psycopg2://airflow:airflow@postgres/airflow"
    )
    
    tablas = {
        "listings": DATA_RAW / "listings_clean.csv",
        "clima": DATA_RAW / "clima_buenos_aires.csv",
        "tipo_cambio": DATA_RAW / "tipo_cambio.csv",
    }
    
    for tabla,path in tablas.items():
        df = pd.read_csv(path)
        df.to_sql(tabla, engine, if_exists="replace", index=False)
        print(f"{tabla}: {len(df)} filas cargadas a PostgreSQL")

# ── definición del DAG ────────────────────────────────────────────
with DAG (
    dag_id = "etl_airbnb_pipeline",
    description = "Pipeline ETL: Airbnb +  Clima + BCRA → GCS → BigQuery",
    schedule = "@daily",
    start_date = datetime(2026,1,1),
    catchup = False,
    default_args = default_args,
    tags = ["etl", "airbnb", "p4"]
) as dag:
    
    t_extract_airbnb = PythonOperator(
        task_id = 'extract_airbnb',
        python_callable = extract_airbnb,
    )
    
    t_extract_clima = PythonOperator(
        task_id = "extract_clima",
        python_callable = extract_clima,
    )
    
    t_extract_bcra = PythonOperator(
        task_id = "extract_bcra",
        python_callable = extract_bcra,
    )
    
    t_load_postgres = PythonOperator(
        task_id = "load_to_postgres",
        python_callable = load_to_postgres,
    )
    
    [t_extract_airbnb, t_extract_clima, t_extract_bcra] >> t_load_postgres