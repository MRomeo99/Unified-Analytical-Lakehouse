"""
Dagster Definitions object — wires all assets, checks, schedules, and sensors.
"""

from dagster import Definitions, define_asset_job, load_assets_from_modules

from orchestration.beacon_orchestration.assets import bronze_assets, gold_assets, silver_assets
from orchestration.beacon_orchestration.checks import (
    dim_clients_row_count,
    dim_clients_schema,
    fct_ad_performance_row_count,
    fct_leads_row_count,
)
from orchestration.beacon_orchestration.schedules import daily_pipeline_schedule
from orchestration.beacon_orchestration.sensors import ad_spend_csv_sensor

all_assets = load_assets_from_modules([bronze_assets, silver_assets, gold_assets])

beacon_job = define_asset_job(
    name="beacon_full_pipeline",
    description="Full Bronze → Silver → Gold pipeline.",
    selection="*",
)

defs = Definitions(
    assets=all_assets,
    asset_checks=[
        dim_clients_row_count,
        dim_clients_schema,
        fct_leads_row_count,
        fct_ad_performance_row_count,
    ],
    jobs=[beacon_job],
    schedules=[daily_pipeline_schedule],
    sensors=[ad_spend_csv_sensor],
)
