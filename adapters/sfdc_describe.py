"""Salesforce schema DISCOVERY (describe-only) — NOT an ingestion path.

Ingestion is owned by the Lakeflow Connect managed Salesforce connector
(ADR-009). This adapter exists for one job: fetch the true field list for
Interface Contract schema discovery when the app's server-side describe is
unavailable (fallback path).

Credentials come from a Databricks secret scope (never params/code):
  {scope}/sfdc_{conn}_client_id | _client_secret | _refresh_token | _instance_url | _login_host

Prints JSON: {"objects": [{"name", "fields": [{name, type, nullable, length, picklist_values}]}]}
"""

import argparse
import json
import urllib.parse
import urllib.request

SKIP_TYPES = {"address", "location", "base64", "complexvalue"}


def http_post_form(url, data):
    body = urllib.parse.urlencode(data).encode()
    req = urllib.request.Request(url, data=body, method="POST")
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"POST {url} -> {e.code}: {e.read().decode()[:500]}") from None


def http_get(url, token):
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"GET {url[:120]} -> {e.code}: {e.read().decode()[:500]}") from None


def normalize_field(f):
    return {
        "name": f["name"],
        "type": f.get("type"),
        "nullable": f.get("nillable", True),
        "length": f.get("length"),
        "picklist_values": [v["value"] for v in (f.get("picklistValues") or []) if v.get("active")],
        "calculated": f.get("calculated") is True,
        "compound": f.get("type") in SKIP_TYPES,
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--secret-scope", required=True)
    p.add_argument("--conn", required=True)
    p.add_argument("--objects-json", required=True, help='["Account","Contact"]')
    args = p.parse_args()

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    dbutils = __import__("pyspark.dbutils", fromlist=["DBUtils"]).DBUtils(spark)

    def secret(suffix):
        return dbutils.secrets.get(scope=args.secret_scope, key=f"sfdc_{args.conn}_{suffix}")

    login_host = secret("login_host") or "login.salesforce.com"
    tok = http_post_form(
        f"https://{login_host}/services/oauth2/token",
        {
            "grant_type": "refresh_token",
            "client_id": secret("client_id"),
            "client_secret": secret("client_secret"),
            "refresh_token": secret("refresh_token"),
        },
    )
    access_token = tok["access_token"]
    instance_url = tok.get("instance_url") or secret("instance_url")

    # persist rotated refresh token (orgs may rotate on every grant)
    new_rt = tok.get("refresh_token")
    if new_rt:
        try:
            from databricks.sdk import WorkspaceClient

            WorkspaceClient().secrets.put_secret(
                scope=args.secret_scope,
                key=f"sfdc_{args.conn}_refresh_token",
                string_value=new_rt,
            )
        except Exception as e:  # non-fatal
            print(f"warn: could not persist rotated refresh token: {e}")

    out = []
    for obj in json.loads(args.objects_json):
        desc = http_get(f"{instance_url}/services/data/v60.0/sobjects/{obj}/describe", access_token)
        out.append({"name": obj, "fields": [normalize_field(f) for f in desc["fields"]]})
        print(f"described {obj}: {len(desc['fields'])} fields")

    print(json.dumps({"objects": out}))


if __name__ == "__main__":
    main()
