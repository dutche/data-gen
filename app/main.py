from fastapi import FastAPI, UploadFile, File
import polars as pl
import io
from typing import Dict

app = FastAPI(title="Polars Schema Inspector", version="1.0.0")

# --- utilitaire ---
def load_polars_df(content: bytes, filename: str) -> pl.DataFrame:
    """
    Détecte automatiquement le format du fichier et le charge dans un DataFrame Polars.
    """
    name = filename.lower()
    buffer = io.BytesIO(content)

    # Détection par extension
    if name.endswith(".csv"):
        return pl.read_csv(buffer)
    elif name.endswith(".parquet"):
        return pl.read_parquet(buffer)
    elif name.endswith(".json") or name.endswith(".ndjson"):
        return pl.read_json(buffer)
    elif name.endswith(".feather") or name.endswith(".ipc"):
        return pl.read_ipc(buffer)
    else:
        # fallback : tenter CSV
        try:
            return pl.read_csv(buffer)
        except Exception as e:
            raise ValueError(f"Format non reconnu : {e}")


# --- endpoint principal ---
@app.post("/analyze/")
async def analyze_dataset(file: UploadFile = File(...)) -> Dict:
    """
    Upload d'un dataset (CSV, Parquet, JSON, IPC...) et extraction du schéma via Polars.
    """
    content = await file.read()

    try:
        df = load_polars_df(content, file.filename)
    except Exception as e:
        return {"error": f"Erreur lors du chargement du fichier : {str(e)}"}

    schema = {col: str(dtype) for col, dtype in df.schema.items()}

    return {
        "filename": file.filename,
        "columns": schema,
        "shape": df.shape,
        "head": df.head(5).to_dict(as_series=False),  # aperçu utile !
    }
