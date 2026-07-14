"""Schema-drift detection (audit gap #1, CRITICAL) — first task of the workflow.

Compares the LIVE source schema against the contract-selected columns in
ctl.dataflow_spec BEFORE ingestion runs, so breaking drift halts the chain
instead of silently corrupting bronze.

Policy comes from ctl.batch_config.drift_policy:
  warn (default) - record drift events, continue
  fail           - record events, exit 1 on MISSING columns (halts workflow)
  pass           - record nothing but missing-column errors as warnings

Detection per entity (Salesforce/P1 today; other formats no-op until their
describe adapters exist):
  MISSING - contract-selected column no longer exists in the source  -> the dangerous one
  ADDED   - new source column not in the contract                    -> informational

Events land in ctl.drift_events. Credentials come from the secret scope written
by the connect wizard (sfdc_{conn}_*); when absent the check degrades to a
warning and passes (managed-connector orgs without discovery creds).
"""

import argparse
import json
import os
import sys
import urllib.parse
import urllib.request

try:
    _ENGINE_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _ENGINE_DIR = os.path.dirname(os.path.abspath(sys.argv[0])) if sys.argv else ""
if _ENGINE_DIR and _ENGINE_DIR not in sys.path:
    sys.path.append(_ENGINE_DIR)


def lit(v):
    return "NULL" if v is None else "'" + str(v).replace("'", "''") + "'"


def diff_columns(selected, live_fields):
    """Pure comparison: (missing, added). selected = contract columns;
    live_fields = source field names."""
    live = set(live_fields)
    sel = list(selected)
    missing = [c for c in sel if c not in live]
    added = sorted(live - set(sel))
    return missing, added


def sfdc_describe_fields(scope, conn, obj, dbutils):
    def secret(suffix):
        return dbutils.secrets.get(scope=scope, key=f"sfdc_{conn}_{suffix}")

    login_host = secret("login_host") or "login.salesforce.com"
    body = urllib.parse.urlencode(
        {
            "grant_type": "refresh_token",
            "client_id": secret("client_id"),
            "client_secret": secret("client_secret"),
            "refresh_token": secret("refresh_token"),
        }
    ).encode()
    req = urllib.request.Request(f"https://{login_host}/services/oauth2/token", data=body, method="POST")
    with urllib.request.urlopen(req) as r:
        tok = json.loads(r.read().decode())
    instance = tok.get("instance_url") or secret("instance_url")
    req2 = urllib.request.Request(
        f"{instance}/services/data/v60.0/sobjects/{obj}/describe",
        headers={"Authorization": f"Bearer {tok['access_token']}"},
    )
    with urllib.request.urlopen(req2) as r:
        desc = json.loads(r.read().decode())
    return [f["name"] for f in desc["fields"]]


def main():
    from pyspark.sql import SparkSession

    from spec_reader import load_rows

    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True)
    p.add_argument("--spec-table", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--secret-scope", default="pipeline_factory")
    p.add_argument("--catalog", default="workspace")
    p.add_argument("--schema", default="ctl")
    args = p.parse_args()

    spark = SparkSession.builder.getOrCreate()
    dbutils = __import__("pyspark.dbutils", fromlist=["DBUtils"]).DBUtils(spark)
    ctl = f"`{args.catalog}`.`{args.schema}`"

    # policy from the control plane
    policy = "warn"
    try:
        rows = spark.sql(
            f"SELECT drift_policy FROM {ctl}.batch_config WHERE source = {lit(args.source)}"
        ).collect()
        if rows and rows[0]["drift_policy"]:
            policy = rows[0]["drift_policy"]
    except Exception as e:
        print(f"warn: batch_config read failed ({e}) — policy=warn")

    active, _ = load_rows(spark, args.spec_table, args.source)
    hard_fail = False
    for row in active:
        src = dict(row["source_details"] or {})
        fmt = row["source_format"]
        selected = list(row["select_columns"] or [])
        obj = src.get("source_object")
        conn = src.get("connection") or f"{args.source}_sample"
        if fmt != "lakeflow_connect" or not obj:
            continue  # only SaaS describe implemented today
        try:
            live = sfdc_describe_fields(args.secret_scope, conn, obj, dbutils)
        except Exception as e:
            print(f"warn: describe unavailable for {obj} ({str(e)[:120]}) — drift check skipped")
            continue
        missing, added = diff_columns(selected, live)
        for col, kind in [(c, "missing") for c in missing] + [(c, "added") for c in added]:
            spark.sql(
                f"""INSERT INTO {ctl}.drift_events
                (source, entity, run_id, kind, column_name, policy, detected_at)
                VALUES ({lit(args.source)}, {lit(row['entity'])}, {lit(args.run_id)},
                        {lit(kind)}, {lit(col)}, {lit(policy)}, current_timestamp())"""
            )
        if missing:
            print(f"DRIFT {row['entity']}: MISSING columns {missing} (policy={policy})")
            if policy == "fail":
                hard_fail = True
        if added:
            print(f"drift {row['entity']}: new source columns {added[:10]}{'…' if len(added) > 10 else ''}")
        if not missing and not added:
            print(f"ok {row['entity']}: schema matches contract selection")

    if hard_fail:
        raise SystemExit("schema drift: contract-selected columns missing at source and drift_policy=fail")


if __name__ == "__main__":
    main()
