from databricks_unit_test_framework.transformations import (
    standardize_customer,
    calculate_order_amount,
)


def test_standardize_customer(spark):
    source = [
        (1, "  John ", " DOE ", " John.Doe@Example.COM ", True),
        (2, "Jane", "Smith", "JANE@EXAMPLE.COM", False),
        (2, "Jane", "Smith", "JANE@EXAMPLE.COM", False),
    ]
    columns = ["customer_id", "first_name", "last_name", "email", "is_active"]

    df = spark.createDataFrame(source, columns)
    actual = standardize_customer(df).orderBy("customer_id").collect()

    assert len(actual) == 2
    assert actual[0]["first_name"] == "John"
    assert actual[0]["last_name"] == "DOE"
    assert actual[0]["email"] == "john.doe@example.com"
    assert actual[0]["customer_status"] == "ACTIVE"
    assert actual[1]["customer_status"] == "INACTIVE"


def test_calculate_order_amount(spark):
    source = [(1001, 3, 12.50), (1002, 2, 10.25)]
    df = spark.createDataFrame(source, ["order_id", "quantity", "unit_price"])

    actual = calculate_order_amount(df).orderBy("order_id").collect()

    assert actual[0]["order_amount"] == 37.50
    assert actual[1]["order_amount"] == 20.50
