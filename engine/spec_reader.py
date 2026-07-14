"""Shared dataflow-spec access for the ingestion framework.

The metadata contract: rows in {catalog}.{schema}.dataflow_spec (written by
Pipeline Factory via MERGE, tombstoned via is_active — never deleted, ADR-010).
The engine and recon job read ONLY through these helpers so validation and
tombstone semantics stay in one place.
"""

import json

ENGINE_VERSION = "1.2.1"


def semver_tuple(v):
    try:
        return tuple(int(p) for p in str(v).strip().split(".")[:3])
    except ValueError:
        return (0, 0, 0)


def check_min_version(row_min, engine_version=ENGINE_VERSION):
    """True when the engine is new enough for the spec row."""
    if not row_min:
        return True
    return semver_tuple(engine_version) >= semver_tuple(row_min)


def parse_transforms(column_transforms_json):
    """column_transforms JSON -> list of dicts:
    {name, target, type, transform, compare: {enabled, normalize, tolerance}}"""
    cols = json.loads(column_transforms_json or "[]")
    out = []
    for c in cols:
        cmp_ = c.get("compare") or {}
        out.append(
            {
                "name": c["name"],
                "target": c["target"],
                "type": c.get("type"),
                "transform": c.get("transform") or None,
                "compare": {
                    "enabled": cmp_.get("enabled", True),
                    "normalize": cmp_.get("normalize"),
                    "tolerance": cmp_.get("tolerance"),
                },
            }
        )
    return out


def parse_dq(dq_json):
    """data_quality_expectations JSON -> {"expect": {}, "expect_or_drop": {}, "expect_or_fail": {}}
    Constraints are full boolean SQL predicates over TARGET column names."""
    dq = json.loads(dq_json or "{}")
    return {
        "expect": dict(dq.get("expect") or dq.get("expect_or_warn") or {}),
        "expect_or_drop": dict(dq.get("expect_or_drop") or {}),
        "expect_or_fail": dict(dq.get("expect_or_fail") or {}),
    }


def parse_keys(crosswalk_keys_json):
    """crosswalk_keys JSON -> list of {source, target}."""
    return json.loads(crosswalk_keys_json or "[]")


def build_compare_exprs(transforms):
    """Per-column diff conditions for reconciliation. Returns
    [(target_col, cond_sql)] where cond_sql is TRUE when values differ.

    Mirrors the engine's transform semantics: the transformed source expression
    is compared against the target column; a normalize expression is applied to
    BOTH sides only when it actually references `value` (LLMs sometimes emit a
    bare word like "lowercase" -> invalid SQL); numeric tolerance beats
    null-safe equality when present.
    """
    out = []
    for c in transforms:
        if not c["compare"]["enabled"]:
            continue
        src_expr = c["transform"] or f"src.`{c['name']}`"
        tgt_expr = f"tgt.`{c['target']}`"
        norm = c["compare"]["normalize"]
        if norm and "value" in norm:
            src_expr = norm.replace("value", f"({src_expr})")
            tgt_expr = norm.replace("value", tgt_expr)
        tol = c["compare"]["tolerance"]
        if tol is not None:
            cond = (
                f"ABS(COALESCE(CAST(({src_expr}) AS DOUBLE),0) - "
                f"COALESCE(CAST({tgt_expr} AS DOUBLE),0)) > {tol}"
            )
        else:
            cond = f"NOT (({src_expr}) <=> {tgt_expr})"
        out.append((c["target"], cond))
    return out


def load_rows(spark, spec_table, group):
    """All spec rows for a dataflow group -> (active, tombstoned).

    Every pipeline init logs the tombstoned ids so a dataset disappearing from
    the graph is always attributable (ADR-010).
    """
    rows = spark.read.table(spec_table).where(f"dataflow_group = '{group}'").collect()
    active = [r for r in rows if r["is_active"]]
    tombstoned = [r for r in rows if not r["is_active"]]
    if tombstoned:
        print(
            "tombstoned (excluded from graph): "
            + ", ".join(r["dataflow_id"] for r in tombstoned)
        )
    bad = [
        r["dataflow_id"]
        for r in active
        if not check_min_version(r["framework_min_version"])
    ]
    if bad:
        raise RuntimeError(
            f"engine {ENGINE_VERSION} is older than framework_min_version required by: {bad}"
        )
    return active, tombstoned
