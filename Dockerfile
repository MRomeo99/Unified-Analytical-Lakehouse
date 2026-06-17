FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml .
RUN pip install --no-cache-dir -e ".[dev]"

COPY . .

ENV DUCKDB_PATH=/app/data/beacon.duckdb
ENV DLT_DATA_DIR=/app/data/dlt
ENV SERVING_HOST=0.0.0.0
ENV SERVING_PORT=8000

EXPOSE 8000

CMD ["uvicorn", "serving.main:app", "--host", "0.0.0.0", "--port", "8000"]
