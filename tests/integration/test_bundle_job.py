import os
import pytest


@pytest.mark.integration
def test_databricks_connection():
    """
    Smoke test placeholder for a Databricks-hosted integration suite.
    Run this separately from local unit tests.

    Required environment:
      DATABRICKS_HOST
      DATABRICKS_TOKEN or OIDC-compatible Databricks authentication
    """
    if not os.getenv("DATABRICKS_HOST"):
        pytest.skip("Databricks integration environment is not configured")

    assert os.getenv("DATABRICKS_HOST").startswith("https://")
