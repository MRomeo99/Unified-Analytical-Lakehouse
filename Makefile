.PHONY: up down seed ingest transform quality pipeline serve test lint docs reset

# Load .env if present
-include .env
export

up:
	docker compose up -d
	@echo "Waiting for services to be healthy..."
	@docker compose wait postgres minio || true
	@sleep 3
	@echo "Services are up. MinIO console: http://localhost:9001"

down:
	docker compose down

seed:
	python seeds/generate_postgres.py
	python seeds/generate_seo_api.py
	python seeds/generate_ad_spend.py
	@echo "Seed data generated."

ingest:
	python ingestion/pipelines.py
	@echo "Bronze layer loaded to MinIO."

transform:
	cd transform/beacon && dbt deps && dbt run && dbt test
	@echo "Silver and Gold layers built."

quality:
	python -c "import great_expectations as gx; ctx = gx.get_context(context_root_dir='quality'); result = ctx.run_checkpoint(checkpoint_name='beacon_checkpoint'); print('Quality gate passed!' if result.success else 'QUALITY GATE FAILED'); exit(0 if result.success else 1)"
	@echo "Data quality checks complete."

pipeline: seed ingest transform quality
	@echo "Full pipeline complete."

serve:
	uvicorn serving.main:app --host $${SERVING_HOST:-0.0.0.0} --port $${SERVING_PORT:-8000} --reload

test:
	pytest tests/ -v

lint:
	ruff check .
	black --check .

docs:
	cd transform/beacon && dbt docs generate && dbt docs serve --port 8080

reset:
	docker compose down -v
	rm -rf data/ .dagster/ transform/beacon/target/ transform/beacon/dbt_packages/
	@echo "Environment reset. Run 'make up' to start fresh."
