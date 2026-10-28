Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Framework (config-driven, PySpark + Delta)
# MAGIC
# MAGIC A reusable, rules-driven data quality engine for Databricks. Rules are plain config (inline Python or a Delta rules table); the engine runs every rule in a single pass over the data and writes:
# MAGIC
# MAGIC | Output table | What it holds |
# MAGIC |---|---|
# MAGIC | `dq_rules` | The rule catalog (optional — seed it from this notebook, then manage rules there) |
# MAGIC | `dq_results` | One row per rule per run: rows checked, rows failed, pass rate, PASS / WARN / FAIL / ERROR |
# MAGIC | `dq_scorecard` | One row per table per run: score by DQ dimension and an overall score |
# MAGIC | `<table>_quarantine` | Rows that failed any `error` rule, with the failing rule IDs attached |
# MAGIC | `<table>_clean` | Rows that passed every `error` rule (optional) |
# MAGIC
# MAGIC **Supported rule types**
# MAGIC
# MAGIC | Level | `rule_type` | Checks |
# MAGIC |---|---|---|
# MAGIC | Row | `not_null` | Value is present (blank strings count as missing by default) |
# MAGIC | Row | `unique` | No duplicates on one or more columns — or on SQL expressions, e.g. a normalised description |
# MAGIC | Row | `range` | Numeric/date value between `min` and `max` |
# MAGIC | Row | `regex` | Value matches a pattern |
# MAGIC | Row | `allowed_values` | Value is in a list |
# MAGIC | Row | `length` | String length between `min` and `max` |
# MAGIC | Row | `referential` | Value exists in a reference table (foreign key) |
# MAGIC | Row | `expression` | Any SQL boolean expression is true (cross-field rules) |
# MAGIC | Row | `gtin_check_digit` | GTIN-8/12/13/14 has a valid GS1 check digit |
# MAGIC | Table | `row_count` | Row count between `min` and `max` |
# MAGIC | Table | `freshness` | Newest timestamp is within `max_age_hours` |
# MAGIC | Table | `schema` | Required columns exist |
# MAGIC
# MAGIC Every row-level rule also accepts an optional `filter` (a SQL condition limiting which rows the rule applies to, e.g. `status = 'ACTIVE'`).
# MAGIC
# MAGIC **How to use:** set the widgets, run all. With `run_demo = true` the notebook builds a sample product master (MDM) dataset with planted defects so you can see every output end to end.
 
# COMMAND ----------
 
# MAGIC %md ## 1. Parameters
 
# COMMAND ----------
 
dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "dq_demo", "Schema")
dbutils.widgets.text("target_table", "product_master", "Table to check (name within schema)")
dbutils.widgets.dropdown("rules_source", "inline", ["inline", "table"], "Rules source")
dbutils.widgets.dropdown("run_demo", "true", ["true", "false"], "Create demo data")
dbutils.widgets.dropdown("write_clean", "true", ["true", "false"], "Write clean table")
dbutils.widgets.dropdown("fail_on_error", "false", ["true", "false"], "Fail job on FAIL")
 
CATALOG       = dbutils.widgets.get("catalog")
SCHEMA        = dbutils.widgets.get("schema")
TARGET_TABLE  = dbutils.widgets.get("target_table")
RULES_SOURCE  = dbutils.widgets.get("rules_source")
RUN_DEMO      = dbutils.widgets.get("run_demo") == "true"
WRITE_CLEAN   = dbutils.widgets.get("write_clean") == "true"
FAIL_ON_ERROR = dbutils.widgets.get("fail_on_error") == "true"
 
TABLE_FORMAT = "delta"
NS = f"{CATALOG}.{SCHEMA}"
 
def fq(name: str) -> str:
    """Fully-qualify a table name. Names that already contain a dot are left as-is."""
    return name if "." in name else f"{NS}.{name}"
 
RULES_TABLE     = fq("dq_rules")
RESULTS_TABLE   = fq("dq_results")
SCORECARD_TABLE = fq("dq_scorecard")
 
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {NS}")
print(f"Namespace: {NS} | target: {fq(TARGET_TABLE)} | rules: {RULES_SOURCE}")
 
# COMMAND ----------
 
import json
import uuid
from datetime import datetime, timedelta, timezone
from functools import reduce
 
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
 
# COMMAND ----------
 
# MAGIC %md ## 2. Demo data — product master (MDM) with planted defects
# MAGIC Skipped when `run_demo = false`.
 
# COMMAND ----------
 
if RUN_DEMO:
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    ts = lambda days: now - timedelta(days=days)
 
    taxonomy = spark.createDataFrame(
        [
            ("PKG-FS-CUP-HOT",  "Packaging", "Foodservice", "Hot Cups"),
            ("PKG-FS-CUP-COLD", "Packaging", "Foodservice", "Cold Cups"),
            ("PKG-FS-LID",      "Packaging", "Foodservice", "Lids"),
            ("PKG-FS-CLAM",     "Packaging", "Foodservice", "Clamshells"),
            ("PKG-RT-TRAY",     "Packaging", "Retail",      "Trays"),
            ("PKG-RT-BAG",      "Packaging", "Retail",      "Bags"),
        ],
        "taxonomy_code string, level_1 string, level_2 string, level_3 string",
    )
    taxonomy.write.format(TABLE_FORMAT).mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("taxonomy_reference"))
 
    product_schema = T.StructType([
        T.StructField("sku", T.StringType()),
        T.StructField("product_description", T.StringType()),
        T.StructField("taxonomy_code", T.StringType()),
        T.StructField("brand", T.StringType()),
        T.StructField("uom", T.StringType()),
        T.StructField("gtin", T.StringType()),
        T.StructField("net_weight_kg", T.DoubleType()),
        T.StructField("case_pack_qty", T.IntegerType()),
        T.StructField("status", T.StringType()),
        T.StructField("created_ts", T.TimestampType()),
        T.StructField("updated_ts", T.TimestampType()),
    ])
    products = [
        # --- clean rows ---------------------------------------------------------------------------------------------
        ("SKU-100001", "12 oz Paper Hot Cup White",      "PKG-FS-CUP-HOT",  "EcoServe", "CS", "10012345000017", 4.20,  1000, "ACTIVE",   ts(400), ts(2)),
        ("SKU-100002", "16 oz Paper Hot Cup White",      "PKG-FS-CUP-HOT",  "EcoServe", "CS", "4006381333931",  5.10,  1000, "ACTIVE",   ts(400), ts(3)),
        ("SKU-100003", "Dome Lid for 16 oz Cold Cup",    "PKG-FS-LID",      "ClearTop", "CS", "036000291452",   2.00,  1000, "ACTIVE",   ts(300), ts(1)),
        ("SKU-100004", "9 in Hinged Clamshell Foam",     "PKG-FS-CLAM",     "FoamPro",  "CS", "96385074",       3.30,   200, "ACTIVE",   ts(250), ts(5)),
        ("SKU-100005", "Meat Tray 2S Black",             "PKG-RT-TRAY",     "FreshPak", "CS", "10012345000024", 6.75,   500, "ACTIVE",   ts(200), ts(4)),
        ("SKU-100006", "24 oz PET Cold Cup Clear",       "PKG-FS-CUP-COLD", "ClearTop", "CS", "10012345000031", 7.40,   600, "ACTIVE",   ts(180), ts(2)),
        ("SKU-100007", "Produce Bag 12x20 Clear",        "PKG-RT-BAG",      "FreshPak", "RL", None,             1.10,     1, "INACTIVE", ts(900), ts(90)),
        # --- defects --------------------------------------------------------------------------------------------------
        ("SKU-100008", None,                             "PKG-FS-LID",      "ClearTop", "CS", "10012345000048", 1.90,  1000, "ACTIVE",   ts(100), ts(1)),   # missing description
        ("sku_100009", "Sip Lid 12-16 oz Black",         "PKG-FS-LID",      "ClearTop", "CS", "10012345000055", 1.80,  1000, "ACTIVE",   ts(100), ts(1)),   # bad SKU format
        ("SKU-100010", "20 oz Paper Hot Cup Kraft",      "PKG-FS-CUP-HOTT", "EcoServe", "CS", "10012345000062", 5.90,  1000, "ACTIVE",   ts(90),  ts(1)),   # taxonomy not in reference
        ("SKU-100011", "8 in Clamshell Paperboard",      "PKG-FS-CLAM",     "EcoServe", "BX", "10012345000079", 2.40,   250, "ACTIVE",   ts(80),  ts(1)),   # UOM not allowed
        ("SKU-100012", "Portion Cup 2 oz",               "PKG-FS-CUP-COLD", "ClearTop", "CS", "10012345000080", 1.20,  2500, "ACTIVE",   ts(70),  ts(1)),   # GTIN bad check digit
        ("SKU-100013", "Foam Tray 4P White",             "PKG-RT-TRAY",     "FoamPro",  "CS", None,             4.00,   500, "ACTIVE",   ts(60),  ts(1)),   # ACTIVE without GTIN
        ("SKU-100014", "Deli Container 32 oz",           "PKG-FS-CUP-COLD", "ClearTop", "CS", "10012345000093", -3.00,  240, "ACTIVE",   ts(50),  ts(1)),   # negative weight
        ("SKU-100015", "Hot Cup Sleeve Kraft",           "PKG-FS-CUP-HOT",  "EcoServe", "CS", "10012345000109", 3.10,     0, "ACTIVE",   ts(40),  ts(1)),   # case pack zero
        ("SKU-100001", "12 oz Paper Hot Cup White DUP",  "PKG-FS-CUP-HOT",  "EcoServe", "CS", "10012345000116", 4.20,  1000, "ACTIVE",   ts(30),  ts(1)),   # duplicate SKU
        ("SKU-100016", "12 OZ paper hot-cup, white",     "PKG-FS-CUP-HOT",  "EcoServe", "CS", "10012345000123", 4.20,  1000, "ACTIVE",   ts(20),  ts(1)),   # near-duplicate description of SKU-100001
        ("SKU-100017", "Clear Hinged Container 6x6",     "PKG-FS-CLAM",     None,       "CS", "10012345000130", 2.80,   300, "PENDING",  ts(10),  ts(12)),  # missing brand, updated before created
        ("SKU-100018", "Retail Bag Large",               "PKG-RT-BAG",      "FreshPak", "CS", "10012345000147", 2.20,   500, "ACTIVE",   ts(5),   ts(1)),
    ]
    spark.createDataFrame(products, product_schema) \
        .write.format(TABLE_FORMAT).mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("product_master"))
 
    print(f"Demo tables created: {fq('product_master')} ({len(products)} rows), {fq('taxonomy_reference')}")
 
# COMMAND ----------
 
# MAGIC %md ## 3. Rule catalog
# MAGIC
# MAGIC Each rule is a dictionary. Fields:
# MAGIC
# MAGIC - `rule_id` — unique ID; `table` — table the rule applies to (name within the schema, or fully qualified)
# MAGIC - `rule_type` — one of the types listed at the top; `columns` — columns the rule checks
# MAGIC - `params` — type-specific settings (see examples); `filter` — optional SQL condition for which rows the rule applies to
# MAGIC - `dimension` — completeness, uniqueness, validity, consistency, integrity, timeliness, volume
# MAGIC - `severity` — `error` (failing rows are quarantined) or `warn` (reported only)
# MAGIC - `threshold` — minimum pass rate for the rule to PASS (1.0 = every row must pass)
# MAGIC
# MAGIC The rules below are product master (MDM) examples. Swap in your own, or set `rules_source = table` and manage them in `dq_rules`.
 
# COMMAND ----------
 
def gtin_valid_sql(col: str) -> str:
    """SQL boolean: GTIN-8/12/13/14 is all digits with a valid GS1 mod-10 check digit."""
    p = f"lpad({col}, 14, '0')"
    weighted = " + ".join(f"cast(substr({p}, {i}, 1) as int) * {3 if i % 2 == 1 else 1}" for i in range(1, 14))
    return (
        f"({col} rlike '^[0-9]+$' AND length({col}) IN (8, 12, 13, 14) "
        f"AND (10 - (({weighted}) % 10)) % 10 = cast(substr({p}, 14, 1) as int))"
    )
 
 
RULES = [
    # ---- Completeness --------------------------------------------------------------------------------------------------
    {"rule_id": "PM-C01", "table": "product_master", "rule_type": "not_null", "columns": ["sku", "product_description", "taxonomy_code", "uom"],
     "dimension": "completeness", "severity": "error", "threshold": 1.0, "description": "Core product attributes must be populated"},
    {"rule_id": "PM-C02", "table": "product_master", "rule_type": "not_null", "columns": ["gtin"], "filter": "status = 'ACTIVE'",
     "dimension": "completeness", "severity": "error", "threshold": 1.0, "description": "Active products must have a GTIN"},
    {"rule_id": "PM-C03", "table": "product_master", "rule_type": "not_null", "columns": ["brand"],
     "dimension": "completeness", "severity": "warn", "threshold": 0.95, "description": "Brand should be populated"},
 
    # ---- Uniqueness ----------------------------------------------------------------------------------------------------
    {"rule_id": "PM-U01", "table": "product_master", "rule_type": "unique", "columns": ["sku"],
     "dimension": "uniqueness", "severity": "error", "threshold": 1.0, "description": "SKU is the primary key"},
    {"rule_id": "PM-U02", "table": "product_master", "rule_type": "unique", "columns": ["gtin"],
     "dimension": "uniqueness", "severity": "error", "threshold": 1.0, "description": "A GTIN may belong to only one SKU"},
    {"rule_id": "PM-U03", "table": "product_master", "rule_type": "unique", "columns": ["product_description", "brand"],
     "params": {"expressions": ["regexp_replace(upper(product_description), '[^A-Z0-9]', '')", "upper(trim(brand))"]},
     "dimension": "uniqueness", "severity": "warn", "threshold": 1.0,
     "description": "Possible duplicate products: same brand and same description once case, spaces and punctuation are ignored"},
 
    # ---- Validity ------------------------------------------------------------------------------------------------------
    {"rule_id": "PM-V01", "table": "product_master", "rule_type": "regex", "columns": ["sku"], "params": {"pattern": r"^SKU-[0-9]{6}$"},
     "dimension": "validity", "severity": "error", "threshold": 1.0, "description": "SKU format is SKU-######"},
    {"rule_id": "PM-V02", "table": "product_master", "rule_type": "allowed_values", "columns": ["uom"], "params": {"values": ["CS", "EA", "RL", "PK"]},
     "dimension": "validity", "severity": "error", "threshold": 1.0, "description": "Unit of measure is from the approved list"},
    {"rule_id": "PM-V03", "table": "product_master", "rule_type": "allowed_values", "columns": ["status"], "params": {"values": ["ACTIVE", "INACTIVE", "PENDING", "DISCONTINUED"]},
     "dimension": "validity", "severity": "error", "threshold": 1.0, "description": "Lifecycle status is from the approved list"},
    {"rule_id": "PM-V04", "table": "product_master", "rule_type": "gtin_check_digit", "columns": ["gtin"],
     "dimension": "validity", "severity": "error", "threshold": 1.0, "description": "GTIN has a valid GS1 check digit"},
    {"rule_id": "PM-V05", "table": "product_master", "rule_type": "range", "columns": ["net_weight_kg"], "params": {"min": 0.001, "max": 1000},
     "dimension": "validity", "severity": "error", "threshold": 1.0, "description": "Net weight is positive and realistic"},
    {"rule_id": "PM-V06", "table": "product_master", "rule_type": "range", "columns": ["case_pack_qty"], "params": {"min": 1, "max": 100000},
     "dimension": "validity", "severity": "error", "threshold": 1.0, "description": "Case pack is at least 1"},
    {"rule_id": "PM-V07", "table": "product_master", "rule_type": "length", "columns": ["product_description"], "params": {"min": 5, "max": 80},
     "dimension": "validity", "severity": "warn", "threshold": 1.0, "description": "Description length is 5-80 characters"},
 
    # ---- Integrity (referential) ---------------------------------------------------------------------------------------
    {"rule_id": "PM-I01", "table": "product_master", "rule_type": "referential", "columns": ["taxonomy_code"],
     "params": {"ref_table": "taxonomy_reference", "ref_column": "taxonomy_code"},
     "dimension": "integrity", "severity": "error", "threshold": 1.0, "description": "Taxonomy code exists in the taxonomy reference"},
 
    # ---- Consistency (cross-field) -------------------------------------------------------------------------------------
    {"rule_id": "PM-X01", "table": "product_master", "rule_type": "expression", "columns": ["created_ts", "updated_ts"],
     "params": {"expr": "updated_ts >= created_ts"},
     "dimension": "consistency", "severity": "warn", "threshold": 1.0, "description": "Last update is not before creation"},
 
    # ---- Timeliness / volume / schema (table-level) --------------------------------------------------------------------
    {"rule_id": "PM-T01", "table": "product_master", "rule_type": "freshness", "columns": ["updated_ts"], "params": {"max_age_hours": 72},
     "dimension": "timeliness", "severity": "warn", "threshold": 1.0, "description": "Table was updated in the last 3 days"},
    {"rule_id": "PM-N01", "table": "product_master", "rule_type": "row_count", "params": {"min": 10, "max": 5_000_000},
     "dimension": "volume", "severity": "error", "threshold": 1.0, "description": "Row count is within expected bounds"},
    {"rule_id": "PM-S01", "table": "product_master", "rule_type": "schema",
     "params": {"required_columns": ["sku", "product_description", "taxonomy_code", "uom", "gtin", "status"]},
     "dimension": "validity", "severity": "error", "threshold": 1.0, "description": "Required columns are present"},
]
 
# COMMAND ----------
 
# MAGIC %md ## 4. Rules table (optional)
# MAGIC `seed_rules_table()` writes the inline rules to `dq_rules`. With `rules_source = table`, rules are read from there instead, so rules can be changed without editing code.
 
# COMMAND ----------
 
RULES_TABLE_SCHEMA = (
    "rule_id string, table_name string, rule_type string, columns array<string>, params string, "
    "filter string, dimension string, severity string, threshold double, description string, active boolean"
)
 
def seed_rules_table(rules: list, mode: str = "overwrite") -> None:
    rows = [(
        r["rule_id"], fq(r["table"]), r["rule_type"], r.get("columns", []), json.dumps(r.get("params", {})),
        r.get("filter"), r.get("dimension", "validity"), r.get("severity", "error"),
        float(r.get("threshold", 1.0)), r.get("description", ""), bool(r.get("active", True)),
    ) for r in rules]
    spark.createDataFrame(rows, RULES_TABLE_SCHEMA).write.format(TABLE_FORMAT).mode(mode) \
        .option("overwriteSchema", "true").saveAsTable(RULES_TABLE)
    print(f"Wrote {len(rows)} rules to {RULES_TABLE}")
 
 
def load_rules(table_name: str) -> list:
    """Return active rules for one table, from the inline list or the rules table."""
    target = fq(table_name)
    if RULES_SOURCE == "table":
        rows = spark.table(RULES_TABLE).where(F.col("active") & (F.col("table_name") == target)).collect()
        return [{
            "rule_id": r.rule_id, "table": r.table_name, "rule_type": r.rule_type, "columns": list(r.columns or []),
            "params": json.loads(r.params or "{}"), "filter": r.filter, "dimension": r.dimension,
            "severity": r.severity, "threshold": r.threshold, "description": r.description,
        } for r in rows]
    return [r for r in RULES if fq(r["table"]) == target and r.get("active", True)]
 
 
if RUN_DEMO or not spark.catalog.tableExists(RULES_TABLE):
    seed_rules_table(RULES)
 
# COMMAND ----------
 
# MAGIC %md ## 5. Engine
 
# COMMAND ----------
 
ROW_RULES   = {"not_null", "unique", "range", "regex", "allowed_values", "length", "referential", "expression", "gtin_check_digit"}
TABLE_RULES = {"row_count", "freshness", "schema"}
 
 
def _validate(rule: dict, df_cols: set) -> str | None:
    """Return an error message if the rule is malformed, else None."""
    rt, cols, p = rule.get("rule_type"), rule.get("columns", []), rule.get("params", {}) or {}
    if rt not in ROW_RULES | TABLE_RULES:
        return f"Unknown rule_type '{rt}'"
    if rt == "schema":
        return None
    missing = [c for c in cols if c not in df_cols]
    if missing:
        return f"Column(s) not found: {missing}"
    if rt in ROW_RULES - {"expression"} and not cols:
        return "Rule needs at least one column"
    needs = {"regex": ["pattern"], "allowed_values": ["values"], "referential": ["ref_table", "ref_column"],
             "expression": ["expr"], "freshness": ["max_age_hours"]}
    lacking = [k for k in needs.get(rt, []) if k not in p]
    if lacking:
        return f"Missing params: {lacking}"
    if rt in ("range", "length", "row_count") and "min" not in p and "max" not in p:
        return "Needs 'min' and/or 'max'"
    return None
 
 
def _outside(c, p: dict):
    """Column expression: value is below min or above max (null -> null, i.e. passes)."""
    conds = []
    if "min" in p: conds.append(c < F.lit(p["min"]))
    if "max" in p: conds.append(c > F.lit(p["max"]))
    return reduce(lambda a, b: a | b, conds)
 
 
def _row_fail_expr(rule: dict, df: DataFrame, idx: int):
    """
    Returns (df, fail_column). The df may gain helper columns (window counts, join markers).
    fail_column is True when the row breaks the rule. Nulls pass every rule except not_null.
    """
    rt, cols, p = rule["rule_type"], rule.get("columns", []), rule.get("params", {}) or {}
    dtypes = dict(df.dtypes)
 
    if rt == "not_null":
        blank_is_null = p.get("blank_is_null", True)
        parts = []
        for c in cols:
            e = F.col(c).isNull()
            if blank_is_null and dtypes[c] == "string":
                e = e | (F.trim(F.col(c)) == "")
            parts.append(e)
        return df, reduce(lambda a, b: a | b, parts)
 
    if rt == "unique":
        keys = [F.expr(e) for e in p.get("expressions", cols)]
        helper = f"__dq_cnt_{idx}"
        df = df.withColumn(helper, F.count(F.lit(1)).over(Window.partitionBy(*keys)))
        any_null = reduce(lambda a, b: a | b, [k.isNull() for k in keys])
        return df, (F.col(helper) > 1) & ~any_null
 
    if rt == "range":
        return df, reduce(lambda a, b: a | b, [_outside(F.col(c), p) for c in cols])
 
    if rt == "length":
        return df, reduce(lambda a, b: a | b, [_outside(F.length(F.col(c)), p) for c in cols])
 
    if rt == "regex":
        return df, reduce(lambda a, b: a | b, [~F.col(c).cast("string").rlike(p["pattern"]) for c in cols])
 
    if rt == "allowed_values":
        if p.get("case_insensitive", False):
            vals = [str(v).upper() for v in p["values"]]
            return df, reduce(lambda a, b: a | b, [~F.upper(F.col(c)).isin(vals) for c in cols])
        return df, reduce(lambda a, b: a | b, [~F.col(c).isin(p["values"]) for c in cols])
 
    if rt == "gtin_check_digit":
        return df, reduce(lambda a, b: a | b, [~F.expr(gtin_valid_sql(f"`{c}`")) for c in cols])
 
    if rt == "expression":
        null_is_fail = p.get("null_is_fail", False)
        return df, F.coalesce(~F.expr(p["expr"]), F.lit(null_is_fail))
 
    if rt == "referential":
        key, marker = f"__dq_refkey_{idx}", f"__dq_refhit_{idx}"
        ref = (spark.table(fq(p["ref_table"])).select(F.col(p["ref_column"]).alias(key))
               .where(F.col(key).isNotNull()).distinct().withColumn(marker, F.lit(True)))
        c = cols[0]
        df = df.join(F.broadcast(ref) if p.get("broadcast", True) else ref, df[c] == ref[key], "left").drop(key)
        return df, F.col(c).isNotNull() & F.col(marker).isNull()
 
    raise ValueError(rt)
 
 
def _table_check(rule: dict, df: DataFrame, row_count: int):
    """Returns (passed: bool, observed_value: str)."""
    rt, cols, p = rule["rule_type"], rule.get("columns", []), rule.get("params", {}) or {}
    if rt == "row_count":
        ok = (row_count >= p.get("min", float("-inf"))) and (row_count <= p.get("max", float("inf")))
        return ok, str(row_count)
    if rt == "freshness":
        latest = df.agg(F.max(cols[0]).alias("m")).first()["m"]
        if latest is None:
            return False, "no timestamps"
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        if getattr(latest, "tzinfo", None) is not None:
            latest = latest.astimezone(timezone.utc).replace(tzinfo=None)
        if not isinstance(latest, datetime):  # DateType
            latest = datetime(latest.year, latest.month, latest.day)
        age_h = (now - latest).total_seconds() / 3600
        return age_h <= p["max_age_hours"], f"{age_h:.1f}h old (latest {latest:%Y-%m-%d %H:%M})"
    if rt == "schema":
        missing = [c for c in p.get("required_columns", cols) if c not in df.columns]
        return not missing, ("all present" if not missing else f"missing: {missing}")
    raise ValueError(rt)
 
 
def run_data_quality(table_name: str, rules: list | None = None, write_outputs: bool = True, write_clean: bool = True) -> dict:
    """Run all rules for one table. Returns dict with results, scorecard, quarantine and clean DataFrames."""
    target = fq(table_name)
    rules = rules if rules is not None else load_rules(table_name)
    run_id = str(uuid.uuid4())
    run_ts = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    base = spark.table(target)
    base_cols = base.columns
    col_set = set(base_cols)
 
    results, row_rules = [], []
 
    def add_result(r, evaluated, failed, status, observed=None, message=None):
        rate = None if evaluated in (None, 0) else round(1 - failed / evaluated, 6)
        results.append((
            run_id, run_ts, target, r.get("rule_id"), r.get("rule_type"), r.get("columns", []), r.get("dimension"),
            r.get("severity"), r.get("description"), r.get("filter"), evaluated, failed, rate,
            float(r.get("threshold", 1.0)), status, observed, message,
        ))
 
    def status_for(r, rate):
        if rate is None or rate >= float(r.get("threshold", 1.0)):
            return "PASS"
        return "FAIL" if r.get("severity", "error") == "error" else "WARN"
 
    # ---- validate rules ---------------------------------------------------------------------------------------------
    for r in rules:
        err = _validate(r, col_set)
        if err:
            add_result(r, None, None, "ERROR", message=err)
        elif r["rule_type"] in ROW_RULES:
            row_rules.append(r)
 
    # ---- build per-row fail/applies flags ---------------------------------------------------------------------------
    df = base
    flag_meta = []  # (rule, fail_col, applies_col)
    for i, r in enumerate(row_rules):
        try:
            df, fail = _row_fail_expr(r, df, i)
            applies = F.expr(r["filter"]) if r.get("filter") else F.lit(True)
            applies = F.coalesce(applies, F.lit(False))
            fcol, acol = f"__dq_fail_{i}", f"__dq_app_{i}"
            df = df.withColumn(acol, applies).withColumn(fcol, F.col(acol) & F.coalesce(fail, F.lit(False)))
            flag_meta.append((r, fcol, acol))
        except Exception as e:  # bad SQL in expr/filter, missing ref table, etc.
            add_result(r, None, None, "ERROR", message=f"{type(e).__name__}: {str(e)[:300]}")
 
    try:
        df = df.cache()
    except Exception:
        pass  # serverless compute does not support caching
 
    # ---- one aggregation pass for all row rules ---------------------------------------------------------------------
    aggs = [F.count(F.lit(1)).alias("__rows")]
    for _, fcol, acol in flag_meta:
        aggs += [F.sum(F.col(fcol).cast("long")).alias(f"{fcol}_n"), F.sum(F.col(acol).cast("long")).alias(f"{acol}_n")]
    agg = df.agg(*aggs).first().asDict()
    row_count = agg["__rows"]
 
    for r, fcol, acol in flag_meta:
        evaluated, failed = int(agg[f"{acol}_n"] or 0), int(agg[f"{fcol}_n"] or 0)
        rate = None if evaluated == 0 else 1 - failed / evaluated
        add_result(r, evaluated, failed, status_for(r, rate),
                   message=None if evaluated else "No rows matched the rule filter")
 
    # ---- table-level rules ------------------------------------------------------------------------------------------
    for r in rules:
        if r.get("rule_type") in TABLE_RULES and not _validate(r, col_set):
            try:
                ok, observed = _table_check(r, base, row_count)
                add_result(r, 1, 0 if ok else 1, status_for(r, 1.0 if ok else 0.0), observed=observed)
            except Exception as e:
                add_result(r, None, None, "ERROR", message=f"{type(e).__name__}: {str(e)[:300]}")
 
    results_df = spark.createDataFrame(results, RESULTS_SCHEMA)
 
    # ---- quarantine / clean split -----------------------------------------------------------------------------------
    def ids(sev):
        arr = [F.when(F.col(f), F.lit(r["rule_id"])) for r, f, _ in flag_meta if r.get("severity", "error") == sev]
        return F.filter(F.array(*arr), lambda x: x.isNotNull()) if arr else F.array().cast("array<string>")
 
    annotated = df.withColumn("dq_errors", ids("error")).withColumn("dq_warnings", ids("warn"))
    out_cols = base_cols + ["dq_errors", "dq_warnings"]
    quarantine_df = (annotated.where(F.size("dq_errors") > 0).select(*out_cols)
                     .withColumn("dq_run_id", F.lit(run_id)).withColumn("dq_run_ts", F.lit(run_ts)))
    clean_df = annotated.where(F.size("dq_errors") == 0).select(*base_cols)
 
    scorecard_df = build_scorecard(results_df)
 
    if write_outputs:
        results_df.write.format(TABLE_FORMAT).mode("append").option("mergeSchema", "true").saveAsTable(RESULTS_TABLE)
        scorecard_df.write.format(TABLE_FORMAT).mode("append").option("mergeSchema", "true").saveAsTable(SCORECARD_TABLE)
        quarantine_df.write.format(TABLE_FORMAT).mode("append").option("mergeSchema", "true").saveAsTable(f"{target}_quarantine")
        if write_clean:
            clean_df.write.format(TABLE_FORMAT).mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{target}_clean")
 
    return {"run_id": run_id, "row_count": row_count, "results": results_df, "scorecard": scorecard_df,
            "quarantine": quarantine_df, "clean": clean_df}
 
 
RESULTS_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()), T.StructField("run_ts", T.TimestampType()),
    T.StructField("table_name", T.StringType()), T.StructField("rule_id", T.StringType()),
    T.StructField("rule_type", T.StringType()), T.StructField("columns", T.ArrayType(T.StringType())),
    T.StructField("dimension", T.StringType()), T.StructField("severity", T.StringType()),
    T.StructField("description", T.StringType()), T.StructField("filter", T.StringType()),
    T.StructField("rows_evaluated", T.LongType()), T.StructField("rows_failed", T.LongType()),
    T.StructField("pass_rate", T.DoubleType()), T.StructField("threshold", T.DoubleType()),
    T.StructField("status", T.StringType()), T.StructField("observed_value", T.StringType()),
    T.StructField("message", T.StringType()),
])
 
 
def build_scorecard(results_df: DataFrame) -> DataFrame:
    """
    Score per dimension = average pass rate of that dimension's rules (each rule counts equally).
    Overall score = average of the dimension scores. Rules in ERROR status are excluded from scoring.
    """
    scored = results_df.where(F.col("pass_rate").isNotNull())
    by_dim = scored.groupBy("run_id", "run_ts", "table_name", "dimension").agg(
        F.avg("pass_rate").alias("score"),
        F.count("*").alias("rules"),
        F.sum(F.when(F.col("status") == "FAIL", 1).otherwise(0)).alias("fails"),
        F.sum(F.when(F.col("status") == "WARN", 1).otherwise(0)).alias("warns"),
    )
    return by_dim.groupBy("run_id", "run_ts", "table_name").agg(
        F.round(F.avg("score") * 100, 2).alias("overall_score"),
        F.map_from_entries(F.collect_list(F.struct("dimension", F.round(F.col("score") * 100, 2)))).alias("dimension_scores"),
        F.sum("rules").alias("rules_scored"),
        F.sum("fails").alias("rules_failed"),
        F.sum("warns").alias("rules_warned"),
    ).join(
        results_df.groupBy("run_id").agg(F.sum(F.when(F.col("status") == "ERROR", 1).otherwise(0)).alias("rules_errored")),
        "run_id",
    )
 
# COMMAND ----------
 
# MAGIC %md ## 6. Run
 
# COMMAND ----------
 
run = run_data_quality(TARGET_TABLE, write_outputs=True, write_clean=WRITE_CLEAN)
print(f"Run {run['run_id']} on {fq(TARGET_TABLE)} — {run['row_count']} rows")
 
# COMMAND ----------
 
# MAGIC %md ### Rule results
 
# COMMAND ----------
 
display(
    run["results"]
    .select("rule_id", "dimension", "severity", "status", "rows_evaluated", "rows_failed",
            F.round(F.col("pass_rate") * 100, 2).alias("pass_pct"), "observed_value", "description", "message")
    .orderBy(F.expr("CASE status WHEN 'ERROR' THEN 0 WHEN 'FAIL' THEN 1 WHEN 'WARN' THEN 2 ELSE 3 END"), "rule_id")
)
 
# COMMAND ----------
 
# MAGIC %md ### Scorecard
 
# COMMAND ----------
 
display(run["scorecard"])
display(
    run["scorecard"].select("table_name", F.explode("dimension_scores").alias("dimension", "score")).orderBy("score")
)
 
# COMMAND ----------
 
# MAGIC %md ### Quarantined rows (failed at least one `error` rule)
 
# COMMAND ----------
 
display(run["quarantine"])
 
# COMMAND ----------
 
# MAGIC %md ### Trend across runs
 
# COMMAND ----------
 
display(
    spark.table(SCORECARD_TABLE)
    .where(F.col("table_name") == fq(TARGET_TABLE))
    .select("run_ts", "overall_score", "rules_failed", "rules_warned", "rules_errored")
    .orderBy(F.desc("run_ts"))
    .limit(30)
)
 
# COMMAND ----------
 
# MAGIC %md ## 7. Gate
# MAGIC With `fail_on_error = true`, the notebook raises if any `error` rule FAILs or any rule is malformed — use this to stop a downstream job task. Either way, a JSON summary is returned for Jobs (`dbutils.notebook.exit`).
 
# COMMAND ----------
 
summary_row = run["scorecard"].first()
status_counts = {r["status"]: r["count"] for r in run["results"].groupBy("status").count().collect()}
summary = {
    "run_id": run["run_id"],
    "table": fq(TARGET_TABLE),
    "rows": run["row_count"],
    "overall_score": summary_row["overall_score"] if summary_row else None,
    "status_counts": status_counts,
    "quarantined_rows": run["quarantine"].count(),
}
print(json.dumps(summary, indent=2))
 
if FAIL_ON_ERROR and (status_counts.get("FAIL", 0) or status_counts.get("ERROR", 0)):
    failed = [r["rule_id"] for r in run["results"].where(F.col("status").isin("FAIL", "ERROR")).collect()]
    raise Exception(f"Data quality gate failed for {fq(TARGET_TABLE)}: {failed}")
 
dbutils.notebook.exit(json.dumps(summary))