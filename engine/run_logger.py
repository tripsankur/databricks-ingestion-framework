"""Ingestion run logger (ADR-011) — workflow task on EVERY run, app or cron.

Writes one ctl.ingestion_runs row per active entity of the source (absolute
bronze/silver counts + state) and upserts ctl.watermarks with the observed
high-water mark for incremental entities. run_id comes from the workflow's
{{job.run_id}} dynamic value, so scheduled runs and app builds correlate the
same way (recon_runs.run_id joins ingestion_runs.run_id).

  --source        dataflow_group
  --spec-table    fully qualified dataflow_spec table
  --run-id        the Lakeflow job run id ({{job.run_id}})
  --trigger-type  build | schedule | manual (job parameter; default schedule)
  --catalog / --schema   ctl location
"""

import argparse
import os
import sys
from datetime import datetime, timezone

try:
    _ENGINE_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _ENGINE_DIR = os.path.dirname(os.path.abspath(sys.argv[0])) if sys.argv else ""
if _ENGINE_DIR and _ENGINE_DIR not in sys.path:
    sys.path.append(_ENGINE_DIR)

def lit(v):
    return "NULL" if v is None else "'" + str(v).replace("'", "''") + "'"


def main():
    from pyspark.sql import SparkSession

    from spec_reader import load_rows

    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True)
    p.add_argument("--spec-table", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--trigger-type", default="schedule")
    p.add_argument("--catalog", default="workspace")
    p.add_argument("--schema", default="ctl")
    args = p.parse_args()

    spark = SparkSession.builder.getOrCreate()
    ctl = f"`{args.catalog}`.`{args.schema}`"
    started = datetime.now(timezone.utc).isoformat()

    active, _ = load_rows(spark, args.spec_table, args.source)
    if not active:
        raise RuntimeError(f"no active dataflow rows for source={args.source}")

    for row in active:
        tgt = dict(row["target_details"] or {})
        src = dict(row["source_details"] or {})
        bronze, silver = tgt.get("bronze_table"), tgt.get("silver_table")
        state, detail = "succeeded", None
        bronze_count = silver_count = None
        try:
            bronze_count = spark.table(bronze).count() if bronze else None
            silver_count = spark.table(silver).count() if silver else None
        except Exception as e:  # table missing / permission — log the run anyway
            state, detail = "failed", str(e)[:300]

        spark.sql(
            f"""INSERT INTO {ctl}.ingestion_runs
            (run_id, source, entity, trigger_type, bronze_table, silver_table,
             bronze_count, silver_count, state, started_at, finished_at, detail)
            VALUES ({lit(args.run_id)}, {lit(args.source)}, {lit(row['entity'])}, {lit(args.trigger_type)},
                    {lit(bronze)}, {lit(silver)},
                    {bronze_count if bronze_count is not None else 'NULL'},
                    {silver_count if silver_count is not None else 'NULL'},
                    {lit(state)}, TIMESTAMP'{started}', current_timestamp(), {lit(detail)})"""
        )

        # observed high-water mark for incremental entities (audit/replay)
        cursor = src.get("cursor_column")
        if cursor and bronze and state == "succeeded":
            try:
                hwm = spark.sql(f"SELECT MAX(`{cursor}`) AS v FROM {bronze}").collect()[0]["v"]
                if hwm is not None:
                    spark.sql(
                        f"""MERGE INTO {ctl}.watermarks w
                        USING (SELECT {lit(args.source)} AS source, {lit(row['entity'])} AS entity) s
                        ON w.source = s.source AND w.entity = s.entity
                        WHEN MATCHED THEN UPDATE SET cursor_column = {lit(cursor)},
                          last_value = {lit(str(hwm))}, last_run_id = {lit(args.run_id)},
                          updated_at = current_timestamp()
                        WHEN NOT MATCHED THEN INSERT (source, entity, cursor_column, last_value, last_run_id, updated_at)
                        VALUES ({lit(args.source)}, {lit(row['entity'])}, {lit(cursor)},
                                {lit(str(hwm))}, {lit(args.run_id)}, current_timestamp())"""
                    )
            except Exception as e:  # non-fatal
                print(f"warn: watermark for {row['entity']}: {e}")

        print(
            f"logged {row['entity']}: bronze={bronze_count} silver={silver_count} "
            f"state={state} trigger={args.trigger_type} run={args.run_id}"
        )


if __name__ == "__main__":
    main()
