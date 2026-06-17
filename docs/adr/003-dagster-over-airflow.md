# ADR 003 — Dagster over Apache Airflow

## Status
Accepted

## Context
Needed an orchestrator that makes SLA compliance and data lineage visually inspectable without extra tooling. The medallion architecture requires clear visibility into whether Bronze, Silver, and Gold layers are fresh and healthy after each pipeline run. Airflow is explicitly mentioned in many AI data engineering job descriptions, so the choice of a different tool requires deliberate justification.

## Decision
Dagster with Software-Defined Assets (SDAs). Each medallion layer (Bronze, Silver, Gold) is modelled as a Dagster asset with a `FreshnessPolicy` attached. `AssetChecks` validate row counts and schema correctness after each layer completes. The Dagster UI displays green or red asset health at a glance — this is the SLA dashboard the project requires without any additional monitoring tooling. A stub at `examples/airflow_equivalent.py` demonstrates the equivalent Airflow DAG structure for keyword coverage and recruiter familiarity.

## Tradeoffs
Airflow has a substantially larger ecosystem, a richer library of community-maintained providers, and appears explicitly in more job postings than Dagster. Dagster's Software-Defined Asset model requires rethinking workflows as assets with upstream dependencies rather than as directed graphs of tasks, which carries a steeper initial learning curve for engineers already familiar with Airflow. Dagster's community and third-party operator library is smaller.

## Swap path to production
No changes are required to pipeline logic or transformation code. To migrate to Airflow: implement the equivalent task DAGs in `examples/airflow_equivalent.py` and promote that file to the active DAGs directory. Replace the Dagster daemon and webserver containers in `docker-compose.yml` with the Airflow scheduler, webserver, and worker services. Freshness SLAs currently expressed as `FreshnessPolicy` objects would be re-expressed as Airflow SLA miss callbacks or an external monitoring tool such as Monte Carlo or Metaplane.
