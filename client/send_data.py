import io
import polars as pl
import requests

API_URL = "http://127.0.0.1:8000/analyze/"

def send_polars_df(df: pl.DataFrame, api_url: str, fmt="parquet"):
    buffer = io.BytesIO()

    if fmt == "csv":
        df.write_csv(buffer)
        filename = "data.csv"
        mime = "text/csv"
    elif fmt == "json":
        df.write_json(buffer)
        filename = "data.json"
        mime = "application/json"
    elif fmt == "ipc":
        df.write_ipc(buffer)
        filename = "data.ipc"
        mime = "application/octet-stream"
    else:  # Parquet par défaut
        df.write_parquet(buffer)
        filename = "data.parquet"
        mime = "application/octet-stream"

    buffer.seek(0)
    files = {"file": (filename, buffer, mime)}

    response = requests.post(api_url, files=files)
    print(response.json())

if __name__ == "__main__":
    df = pl.DataFrame({
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 32, 29],
        "salary": [50000.0, 62000.5, 58000.0],
    })

    send_polars_df(df, API_URL, fmt="ipc")  # essaie aussi fmt="csv" ou "json"
