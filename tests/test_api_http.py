import time
import threading
import requests
import pytest
import polars as pl
from uvicorn import Config, Server
from app.main import app

API_URL = "http://127.0.0.1:8000/analyze/"

# --- Fixture pour lancer l'API FastAPI en background ---
@pytest.fixture(scope="session", autouse=True)
def start_api():
    """Démarre l'API FastAPI en background pour les tests HTTP."""
    config = Config(app=app, host="127.0.0.1", port=8000, log_level="error")
    server = Server(config=config)

    thread = threading.Thread(target=server.run)
    thread.start()
    time.sleep(1)  # laisse le temps au serveur de démarrer

    yield  # tests se lancent ici

    server.should_exit = True
    thread.join()


# --- Utilitaire pour envoyer un DataFrame ---
def send_df_http(df: pl.DataFrame, fmt="parquet"):
    import io
    buffer = io.BytesIO()

    if fmt == "csv":
        df.write_csv(buffer)
        filename = "data.csv"
        mime = "text/csv"
    elif fmt == "json":
        df.write_json(buffer)
        filename = "data.json"
        mime = "application/json"
    else:  # Parquet
        df.write_parquet(buffer)
        filename = "data.parquet"
        mime = "application/octet-stream"

    buffer.seek(0)
    files = {"file": (filename, buffer, mime)}
    response = requests.post(API_URL, files=files)
    return response


@pytest.mark.parametrize("fmt", ["csv", "json", "parquet"])
def test_api_http(fmt):
    df = pl.DataFrame({
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 32, 29],
        "salary": [50000.0, 62000.5, 58000.0],
    })

    response = send_df_http(df, fmt)
    assert response.status_code == 200
    data = response.json()
    assert data["shape"] == [3, 3]
    assert "columns" in data
