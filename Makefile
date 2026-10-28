test:
	uv run pytest --cov=src/databricks_unit_test_framework --cov-report=term-missing --cov-report=xml:coverage.xml

validate:
	databricks bundle validate --target dev

deploy:
	databricks bundle deploy --target dev

run:
	databricks bundle run unit_test_framework_job --target dev
