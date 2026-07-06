"""Generic metadata-driven ETL engine (Lakeflow Spark Declarative Pipelines).

ONE static engine, N sources. Each app-provisioned ETL pipeline
(slvr_{source}_etl) points its libraries glob at this file and passes:

  pf.source      dataflow_group to serve (e.g. "sfdc")
  pf.spec_table  fully qualified dataflow_spec table (e.g. workspace.ctl.dataflow_spec)
  pf.env         dev | prod (informational, lands in table properties)

For every ACTIVE spec row in the group the engine registers:
  - bronze streaming/batch table   (only for source_format delta|cloudfiles —
    SaaS sources land bronze via their Lakeflow Connect ingestion pipeline)
  - silver stitch materialized view (transforms + crosswalk join + DQ expectations)

Metadata rows are tombstoned, never deleted (ADR-010): the engine reads only
is_active rows and logs every exclusion, because SDP drops managed datasets
that disappear from the graph.
"""

import os
import sys

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

SOURCE = spark.conf.get("pf.source")
SPEC_TABLE = spark.conf.get("pf.spec_table")
ENV = spark.conf.get("pf.env", "dev")

# SDP executes source files without __file__; the provisioner passes the deployed
# engine directory via pf.engine_dir so sibling modules stay importable.
try:
    _ENGINE_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _ENGINE_DIR = spark.conf.get("pf.engine_dir", "")
if _ENGINE_DIR and _ENGINE_DIR not in sys.path:
    sys.path.append(_ENGINE_DIR)

from spec_reader import (  # noqa: E402
    ENGINE_VERSION,
    load_rows,
    parse_dq,
    parse_keys,
    parse_transforms,
)


def _props(row):
    base = dict(row["table_properties"] or {})
    base.update(
        {
            "generated_by": "pipeline_factory",
            "pf.dataflow_id": row["dataflow_id"],
            "pf.spec_version": str(row["spec_version"]),
            "pf.framework_version": ENGINE_VERSION,
            "pf.env": ENV,
        }
    )
    return base


def register_bronze(name, fmt, src, select_cols, cluster_by, props):
    """Bronze for engine-owned sources (delta snapshot / cloudFiles autoloader).
    All values arrive as arguments — never closed-over loop variables (SDP
    late-binding pitfall)."""

    @dp.table(name=name, cluster_by=cluster_by or None, table_properties=props)
    def bronze():
        if fmt == "cloudfiles":
            reader = (
                spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", src.get("file_format", "json"))
                .load(src["path"])
            )
        else:  # delta snapshot from a raw table
            reader = spark.read.table(src["raw_table"])
        return reader.select(*select_cols) if select_cols else reader

    return bronze


def register_silver(name, bronze_fqn, transforms, crosswalk_table, keys, dq, cluster_by, props):
    """Silver stitch: transformed source columns + inner crosswalk join, with
    data-quality expectations from metadata applied on target column names."""

    def silver():
        from pyspark.sql import functions as F

        src = spark.read.table(bronze_fqn).alias("src")
        select_exprs = [
            f"{c['transform'] or ('src.`' + c['name'] + '`')} AS `{c['target']}`"
            for c in transforms
        ]
        if crosswalk_table and keys:
            xw = spark.read.table(crosswalk_table).alias("xw")
            cond = " AND ".join(f"src.`{k['source']}` = xw.`{k['source']}`" for k in keys)
            return src.join(xw, on=F.expr(cond), how="inner").selectExpr(*select_exprs)
        return src.selectExpr(*select_exprs)

    fn = dp.materialized_view(name=name, cluster_by=cluster_by or None, table_properties=props)(
        silver
    )
    for nm, cond in dq["expect"].items():
        fn = dp.expect(nm, cond)(fn)
    for nm, cond in dq["expect_or_drop"].items():
        fn = dp.expect_or_drop(nm, cond)(fn)
    for nm, cond in dq["expect_or_fail"].items():
        fn = dp.expect_or_fail(nm, cond)(fn)
    return fn


active, _tombstoned = load_rows(spark, SPEC_TABLE, SOURCE)
print(f"engine {ENGINE_VERSION}: {len(active)} active dataflows for group '{SOURCE}'")

for row in active:
    src_details = dict(row["source_details"] or {})
    tgt = dict(row["target_details"] or {})
    transforms = parse_transforms(row["column_transforms"])
    dq = parse_dq(row["data_quality_expectations"])
    keys = parse_keys(row["crosswalk_keys"])
    cluster_by = list(row["cluster_by"] or [])
    select_cols = list(row["select_columns"] or [])
    props = _props(row)

    # The engine registers bronze ONLY when it owns the landing: cloudfiles with
    # a path, or delta snapshot from an explicit raw_table. Otherwise bronze is
    # external (managed Lakeflow Connect pipeline, or a pre-existing table) and
    # the engine only consumes it.
    owns_bronze = (row["source_format"] == "cloudfiles" and src_details.get("path")) or (
        row["source_format"] == "delta" and src_details.get("raw_table")
    )
    if owns_bronze:
        register_bronze(
            tgt["bronze_table"], row["source_format"], src_details, select_cols, cluster_by, props
        )

    register_silver(
        tgt["silver_table"],
        tgt["bronze_table"],
        transforms,
        tgt.get("crosswalk_table"),
        keys,
        dq,
        cluster_by,
        props,
    )
