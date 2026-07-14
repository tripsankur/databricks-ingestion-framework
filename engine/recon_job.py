"""Generic reconciliation job (framework — ONE copy for every source).

Runs as a workflow task (spark_python_task) with a real entry point and
parameters — the two external-review findings on the old per-source rendered
recon (no __main__, unused compare columns) are fixed here by construction:
this file IS executed as a script, and the attribute comparison below is the
result, not an aspiration.

Reads its per-entity config from {spec_table} (dataflow_spec metadata), compares
bronze (post-ingestion source) against the silver target through the crosswalk,
and writes rates + bounded record diffs to the ctl recon tables.

  --source      dataflow_group (e.g. sfdc)
  --entity      optional single entity; default = every active entity in group
  --spec-table  fully qualified dataflow_spec table
  --recon-id / --run-id   correlation ids from the caller (app or workflow)
  --catalog / --schema    ctl location for recon_* result tables
"""

import argparse
import json
import os
import sys

# serverless spark_python_task exec()s the file without __file__; argv[0] is the
# script path there, so sibling modules stay importable in both contexts
try:
    _ENGINE_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _ENGINE_DIR = os.path.dirname(os.path.abspath(sys.argv[0])) if sys.argv else ""
if _ENGINE_DIR and _ENGINE_DIR not in sys.path:
    sys.path.append(_ENGINE_DIR)

from pyspark.sql import SparkSession

from spec_reader import build_compare_exprs, load_rows, parse_keys, parse_transforms

MAX_DIFF_SAMPLES = 25


def recon_entity(spark, ctl, row, recon_id):
    tgt = dict(row["target_details"] or {})
    src_tbl = tgt["bronze_table"]
    tgt_tbl = tgt["silver_table"]
    xw_tbl = tgt.get("crosswalk_table")
    keys = parse_keys(row["crosswalk_keys"])
    transforms = parse_transforms(row["column_transforms"])
    entity = row["entity"]

    src_key = keys[0]["source"] if keys else None
    tgt_key = keys[0]["target"] if keys else None

    source_count = spark.table(src_tbl).count()
    target_count = spark.table(tgt_tbl).count()

    join_sql = (
        f"FROM {src_tbl} src "
        + (f"INNER JOIN {xw_tbl} xw ON src.`{src_key}` = xw.`{src_key}` " if xw_tbl else "")
        + f"INNER JOIN {tgt_tbl} tgt ON tgt.`{tgt_key}` = src.`{src_key}`"
    )

    key_matches = spark.sql(f"SELECT COUNT(*) AS n {join_sql}").collect()[0]["n"]
    key_match_rate = key_matches / source_count if source_count else 0.0

    compare = build_compare_exprs(transforms)
    col_names = [c for c, _ in compare]
    diff_selects = [f"CAST({cond} AS INT) AS `diff_{c}`" for c, cond in compare]
    # keep both sides' raw values so record diffs are actionable
    value_selects = []
    by_target = {c["target"]: c for c in transforms}
    for c, _ in compare:
        t = by_target[c]
        src_expr = t["transform"] or f"src.`{t['name']}`"
        value_selects.append(f"CAST(({src_expr}) AS STRING) AS `srcv_{c}`")
        value_selects.append(f"CAST(tgt.`{c}` AS STRING) AS `tgtv_{c}`")

    row_match_rate = attr_match_rate = 1.0
    n_joined = 0
    if compare:
        spark.sql(
            f"SELECT src.`{src_key}` AS _key, "
            + ", ".join(diff_selects + value_selects)
            + f" {join_sql}"
        ).createOrReplaceTempView("recon_joined")

        n_joined = spark.table("recon_joined").count()
        sums = spark.sql(
            "SELECT "
            + ", ".join(f"SUM(`diff_{c}`) AS `d_{c}`" for c in col_names)
            + ", SUM(CAST(("
            + " + ".join(f"`diff_{c}`" for c in col_names)
            + ") > 0 AS INT)) AS rows_with_diff FROM recon_joined"
        ).collect()[0]
        total_cells = n_joined * len(col_names)
        total_diffs = sum(sums[f"d_{c}"] or 0 for c in col_names)
        rows_with_diff = sums["rows_with_diff"] or 0
        attr_match_rate = 1 - (total_diffs / total_cells) if total_cells else 0.0
        row_match_rate = 1 - (rows_with_diff / n_joined) if n_joined else 0.0

    spark.sql(
        f"""INSERT INTO {ctl}.recon_entity_result
        (recon_id, entity, source_count, target_count, key_match_rate, row_match_rate, attr_match_rate, created_at)
        VALUES ('{recon_id}', '{entity}', {source_count}, {target_count},
                {key_match_rate}, {row_match_rate}, {attr_match_rate}, current_timestamp())"""
    )

    # bounded, batched record-diff samples with actual values
    for c in col_names:
        diffs = spark.sql(
            f"SELECT _key, `srcv_{c}` AS sv, `tgtv_{c}` AS tv FROM recon_joined "
            f"WHERE `diff_{c}` = 1 LIMIT {MAX_DIFF_SAMPLES}"
        ).collect()
        if not diffs:
            continue

        def lit(v):
            return "NULL" if v is None else "'" + str(v).replace("'", "''") + "'"

        values = ", ".join(
            f"('{recon_id}', '{entity}', {lit(d['_key'])}, '{c}', {lit(d['sv'])}, {lit(d['tv'])}, current_timestamp())"
            for d in diffs
        )
        spark.sql(
            f"""INSERT INTO {ctl}.recon_record_diff
            (recon_id, entity, key_value, column_name, source_value, target_value, created_at)
            VALUES {values}"""
        )

    print(
        f"recon {entity}: src={source_count} tgt={target_count} "
        f"key={key_match_rate:.4f} row={row_match_rate:.4f} attr={attr_match_rate:.4f}"
    )
    return {
        "entity": entity,
        "source_count": source_count,
        "target_count": target_count,
        "key_match_rate": key_match_rate,
        "row_match_rate": row_match_rate,
        "attr_match_rate": attr_match_rate,
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True)
    p.add_argument("--entity", default="")
    p.add_argument("--spec-table", required=True)
    # optional since 1.1.0 (ADR-011): as a scheduled workflow task there is no app
    # to mint ids — recon_id self-generates; run_id defaults to the job run id
    # passed via {{job.run_id}} so it joins ingestion_runs
    p.add_argument("--recon-id", default="")
    p.add_argument("--run-id", default="")
    p.add_argument("--catalog", default="workspace")
    p.add_argument("--schema", default="ctl")
    args = p.parse_args()
    if not args.recon_id:
        import uuid

        args.recon_id = str(uuid.uuid4())
    if not args.run_id:
        args.run_id = args.recon_id

    spark = SparkSession.builder.getOrCreate()
    ctl = f"`{args.catalog}`.`{args.schema}`"

    active, _ = load_rows(spark, args.spec_table, args.source)
    if args.entity:
        active = [r for r in active if r["entity"] == args.entity]
    if not active:
        raise RuntimeError(f"no active dataflow rows for source={args.source} entity={args.entity or '*'}")

    spec_id = active[0]["dataflow_id"]
    spec_version = active[0]["spec_version"]
    spark.sql(
        f"""INSERT INTO {ctl}.recon_runs (recon_id, run_id, spec_id, spec_version, status, started_at, finished_at)
        VALUES ('{args.recon_id}', '{args.run_id}', '{spec_id}', {spec_version}, 'running', current_timestamp(), NULL)"""
    )

    try:
        results = [recon_entity(spark, ctl, row, args.recon_id) for row in active]
    except Exception:
        spark.sql(
            f"""UPDATE {ctl}.recon_runs SET status = 'failed', finished_at = current_timestamp()
            WHERE recon_id = '{args.recon_id}'"""
        )
        raise

    spark.sql(
        f"""UPDATE {ctl}.recon_runs SET status = 'succeeded', finished_at = current_timestamp()
        WHERE recon_id = '{args.recon_id}'"""
    )
    print(json.dumps({"recon_id": args.recon_id, "entities": results}))


if __name__ == "__main__":
    main()
