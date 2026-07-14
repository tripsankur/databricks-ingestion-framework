import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "engine"))

from spec_reader import (  # noqa: E402
    build_compare_exprs,
    check_min_version,
    parse_dq,
    parse_keys,
    parse_transforms,
    semver_tuple,
)


def _t(name="src_col", target="tgt_col", transform=None, compare=None):
    return {"name": name, "target": target, "transform": transform, "compare": compare or {}}


def test_semver_ordering():
    assert semver_tuple("1.2.3") == (1, 2, 3)
    assert semver_tuple("2.0") == (2, 0)
    assert check_min_version("1.0.0", engine_version="1.0.0")
    assert check_min_version("0.9.0", engine_version="1.0.0")
    assert not check_min_version("2.0.0", engine_version="1.0.0")
    assert check_min_version(None)
    assert check_min_version("")


def test_parse_transforms_defaults():
    cols = parse_transforms(json.dumps([_t()]))
    assert cols[0]["compare"] == {"enabled": True, "normalize": None, "tolerance": None}
    assert cols[0]["transform"] is None


def test_parse_dq_accepts_warn_alias_and_missing():
    dq = parse_dq(json.dumps({"expect_or_warn": {"a": "x > 0"}, "expect_or_fail": {"b": "y IS NOT NULL"}}))
    assert dq["expect"] == {"a": "x > 0"}
    assert dq["expect_or_fail"] == {"b": "y IS NOT NULL"}
    assert dq["expect_or_drop"] == {}
    assert parse_dq(None) == {"expect": {}, "expect_or_drop": {}, "expect_or_fail": {}}


def test_parse_keys():
    assert parse_keys(json.dumps([{"source": "id", "target": "sid"}]))[0]["source"] == "id"
    assert parse_keys(None) == []


def test_compare_null_safe_default():
    [(col, cond)] = build_compare_exprs(parse_transforms(json.dumps([_t()])))
    assert col == "tgt_col"
    assert cond == "NOT ((src.`src_col`) <=> tgt.`tgt_col`)"


def test_compare_uses_transform_expression():
    [(_, cond)] = build_compare_exprs(
        parse_transforms(json.dumps([_t(transform="UPPER(src.`src_col`)")]))
    )
    assert "UPPER(src.`src_col`)" in cond


def test_compare_tolerance_beats_equality():
    [(_, cond)] = build_compare_exprs(
        parse_transforms(json.dumps([_t(compare={"tolerance": 0.01})]))
    )
    assert "ABS(" in cond and "> 0.01" in cond


def test_compare_normalize_requires_value_reference():
    # bare word (LLM quirk) must be ignored — would be invalid SQL
    [(_, cond)] = build_compare_exprs(
        parse_transforms(json.dumps([_t(compare={"normalize": "lowercase"})]))
    )
    assert "lowercase" not in cond
    # a real normalize referencing `value` is applied to BOTH sides
    [(_, cond2)] = build_compare_exprs(
        parse_transforms(json.dumps([_t(compare={"normalize": "LOWER(value)"})]))
    )
    assert cond2.count("LOWER(") == 2


def test_compare_disabled_column_excluded():
    out = build_compare_exprs(
        parse_transforms(json.dumps([_t(compare={"enabled": False}), _t(name="b", target="tb")]))
    )
    assert [c for c, _ in out] == ["tb"]


def test_engine_version_matches_version_file():
    from spec_reader import ENGINE_VERSION
    v = (Path(__file__).resolve().parents[1] / "VERSION").read_text().strip()
    assert v == ENGINE_VERSION


def test_run_logger_lit_escaping():
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "engine"))
    from run_logger import lit
    assert lit(None) == "NULL"
    assert lit("o'brien") == "'o''brien'"
    assert lit(42) == "'42'"


def test_drift_diff_columns():
    from drift_check import diff_columns
    missing, added = diff_columns(["Id", "Name", "Gone"], ["Id", "Name", "NewCol", "Other"])
    assert missing == ["Gone"]
    assert added == ["NewCol", "Other"]
    m2, a2 = diff_columns(["Id"], ["Id"])
    assert m2 == [] and a2 == []
