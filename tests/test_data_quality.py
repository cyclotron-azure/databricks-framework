from databricks_unit_test_framework.transformations import standardize_customer


def test_customer_id_is_not_null(spark):
    source = [
        (1, "John", "Doe", "john@example.com", True),
        (2, "Jane", "Smith", "jane@example.com", True),
    ]
    df = spark.createDataFrame(
        source,
        ["customer_id", "first_name", "last_name", "email", "is_active"],
    )

    result = standardize_customer(df)

    assert result.filter("customer_id IS NULL").count() == 0


def test_email_is_lowercase(spark):
    source = [(1, "John", "Doe", "JOHN@EXAMPLE.COM", True)]
    df = spark.createDataFrame(
        source,
        ["customer_id", "first_name", "last_name", "email", "is_active"],
    )

    result = standardize_customer(df)

    assert result.first()["email"] == "john@example.com"
