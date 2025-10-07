import io
import pytest
import polars as pl
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

@pytest.fixture
def sample_df():
    return pl.DataFrame({
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 32, 29],
        "salary": [50000.0, 62000.5, 58000.0],
    })


def send_df(df: pl.DataFrame, fmt: str):
    buffer = io.BytesIO()

    if fmt == "csv":
        df.write_csv(buffer)
        filename = "data.csv"
        mime = "text/csv"
    elif fmt == "json":
        df.write_json(buffer)
        filename = "data.json"
        mime = "application/json"
    else:  # Parquet par défaut
        df.write_parquet(buffer)
        filename = "data.parquet"
        mime = "application/octet-stream"

    buffer.seek(0)
    files = {"file": (filename, buffer, mime)}
    return client.post("/analyze/", files=files)


@pytest.mark.parametrize("fmt", ["csv", "json", "parquet"])
def test_analyze_auto_detect(sample_df, fmt):
    response = send_df(sample_df, fmt)
    assert response.status_code == 200
    data = response.json()

    assert "columns" in data
    assert "shape" in data
    assert data["shape"] == [3, 3]
    assert data["columns"]["age"] == "Int64"
    assert data["columns"]["salary"].startswith("Float")
    assert data["head"]["name"][0] == "Alice"
