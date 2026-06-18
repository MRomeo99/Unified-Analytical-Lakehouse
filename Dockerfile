FROM python:3.11-slim

WORKDIR /app

# Install only serving-layer dependencies — not the full pipeline stack
RUN pip install --no-cache-dir \
    "fastapi>=0.111" \
    "uvicorn[standard]>=0.29" \
    "duckdb>=0.10" \
    "python-dotenv>=1.0"

COPY serving/ serving/

ENV DUCKDB_PATH=/app/data/beacon.duckdb
ENV PYTHONPATH=/app

EXPOSE 8000

CMD ["uvicorn", "serving.main:app", "--host", "0.0.0.0", "--port", "8000"]
