import pandas as pd
from pathlib import Path
from google.cloud import storage

DATA_RAW = Path("data/raw")
BUCKET = "portfolio-ds-jaime"
PREFIX = "p4-airbnb-etl"

def upload_parquet(df, nombre, bucket_name, prefix):
    """Convierte un DataFrame a Parquet y lo sube a GCS"""
    ruta_local = Path(f"/tmp/{nombre}.parquet")
    df.to_parquet(ruta_local, index=False)
    
    cliente = storage.Client()
    bucket = cliente.bucket(bucket_name)
    blob = bucket.blob(f"{prefix}/{nombre}.parquet")
    blob.upload_from_filename(ruta_local)
    print(f"{nombre}.parquet subido a gs://{bucket_name}/{prefix}/{nombre}.parquet")
    
if __name__ == "__main__":
    tablas = {
        "listings": DATA_RAW / "listings_clean.csv",
        "clima": DATA_RAW / "clima_buenos_aires.csv",
        "tipo_cambio": DATA_RAW / "tipo_cambio.csv",
    }
    
    for nombre, path in tablas.items():
        df = pd.read_csv(path)
        upload_parquet(df, nombre, BUCKET, PREFIX)