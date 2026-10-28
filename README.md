# Databricks Unit Test + Code Coverage + CI/CD + Asset Bundle

This is a production-style starter framework for Python/PySpark code deployed with
Databricks Declarative Automation Bundles (formerly Databricks Asset Bundles).

## Architecture

Developer -> Pull Request -> pytest -> coverage gate (85%)
                         -> bundle validate
                         -> merge main
                         -> deploy DEV -> run job
                         -> deploy STAGE
                         -> deploy PROD

## Repository structure

```text
.
├── databricks.yml
├── pyproject.toml
├── resources/
│   └── jobs.yml
├── src/
│   └── databricks_unit_test_framework/
│       ├── transformations.py
│       ├── jobs.py
│       └── notebook_entry.py
├── tests/
│   ├── conftest.py
│   ├── test_transformations.py
│   ├── test_data_quality.py
│   └── integration/
│       └── test_bundle_job.py
└── .github/workflows/ci.yml
```

## Run locally

Install uv, then:

```bash
uv sync
uv run pytest --cov=src/databricks_unit_test_framework \
  --cov-report=term-missing \
  --cov-report=xml:coverage.xml \
  --cov-report=html:htmlcov
```

Coverage is configured to fail below 85%.

## Databricks CLI

Install the current Databricks CLI and authenticate.

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run unit_test_framework_job --target dev
```

## CI/CD authentication

The GitHub workflow uses OIDC/workload identity federation rather than storing
a long-lived Databricks token in GitHub.

Configure GitHub Environment variables:

- DATABRICKS_HOST
- DATABRICKS_CLIENT_ID

Configure the Databricks service principal with the appropriate GitHub federation
policy and workspace permissions.

## Important design principle

Do not put most business logic directly in notebooks.

Keep transformations in:

`src/.../transformations.py`

Then:

1. pytest tests the functions locally.
2. coverage measures the Python business logic.
3. the notebook is only a thin Databricks entry point.
4. the bundle deploys notebooks/code/jobs consistently across environments.

## Extending this to Medallion

Recommended layout:

```text
src/
  bronze/
  silver/
  gold/
  dq/
  common/
tests/
  bronze/
  silver/
  gold/
  dq/
```

For your config-driven ingestion framework, configuration-driven behavior should
also be tested independently from Spark transformations.

Example future test categories:

- schema validation
- null checks
- duplicate-key checks
- watermark logic
- incremental load logic
- CDC handling
- bad-record handling
- SCD Type 1/2
- Bronze-to-Silver transformations
- Silver-to-Gold business rules
- audit/control-table updates
- retry/idempotency behavior
