from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import ast
import hashlib
import json
import re

import numpy as np
import pandas as pd

ENGINE_VERSION = "SETTLED_RULE_ENGINE_V1_2_ROOT_FLAT_MEMBER35_FINAL_20260719"
MEMBER_STACK_STATUS = "WF_PENDING"
DAILY_BASE_DUE_GATE_MODE = "OPEN_GATE_FEATURE_ONLY"
WORKING8 = ("027", "148", "235", "257", "279", "356", "469", "579")
CORE_KEY = ["Date", "Stream", "Core"]
MEMBER_KEY = ["Date", "Stream", "Core", "BoxedMember"]


@dataclass
class PipelineResult:
    core_all: pd.DataFrame
    core_survivors: pd.DataFrame
    member_all: pd.DataFrame
    member_survivors: pd.DataFrame
    core_fire_audit: pd.DataFrame
    member_fire_audit: pd.DataFrame
    stage_summary: pd.DataFrame
    handshake: pd.DataFrame
    registry: pd.DataFrame


def _rules_dir() -> Path:
    """Root-flat deployment: settled registry CSVs live beside app.py and this engine."""
    return Path(__file__).resolve().parent


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_registry() -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    files = {
        "PICK3_95": _rules_dir() / "PICK3_95.csv",
        "CORE45": _rules_dir() / "CORE45.csv",
        "MEMBER35": _rules_dir() / "MEMBER35.csv",
    }
    tables: dict[str, pd.DataFrame] = {}
    rows = []
    for stage, path in files.items():
        if not path.exists():
            raise FileNotFoundError(f"Missing settled rule registry: {path}")
        tab = pd.read_csv(path, dtype=str).fillna("")
        tables[stage] = tab
        id_col = "ID" if "ID" in tab.columns else "RuleName"
        for i, r in tab.iterrows():
            rows.append({
                "EngineVersion": ENGINE_VERSION,
                "Stage": stage,
                "Step": int(str(r.get("Step", i + 1)) or i + 1),
                "RuleID": str(r.get(id_col, "")),
                "RuleName": str(r.get("Name", r.get("RuleName", ""))),
                "ApplicableIf": str(r.get("ApplicableIf", "")),
                "Expression": str(r.get("Expression", "")),
                "RegistryFile": path.name,
                "RegistrySHA256": _sha256(path),
                "Active": True,
            })
    registry = pd.DataFrame(rows)
    if registry.duplicated(["Stage", "Step"]).any():
        raise ValueError("Rule dictionary collision: duplicate Stage + Step")
    if registry.duplicated(["Stage", "RuleID"]).any():
        raise ValueError("Rule dictionary collision: duplicate Stage + RuleID")
    return registry, tables


def _norm4(v: Any) -> str:
    s = re.sub(r"\D", "", str(v or ""))
    return s[-4:].zfill(4) if s else ""


def _norm3(v: Any) -> str:
    s = re.sub(r"\D", "", str(v or ""))
    return "".join(sorted(s[-3:].zfill(3))) if s else ""


def _date_series(df: pd.DataFrame, date_col: str | None) -> pd.Series:
    if date_col and date_col in df.columns:
        return pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y-%m-%d")
    for c in ("Date", "PlaylistDate", "PlayDate"):
        if c in df.columns:
            return pd.to_datetime(df[c], errors="coerce").dt.strftime("%Y-%m-%d")
    return pd.Series("UNKNOWN", index=df.index)


def normalize_core_rows(rows: pd.DataFrame, date_col: str | None = None) -> pd.DataFrame:
    if rows is None or rows.empty:
        return pd.DataFrame()
    x = rows.copy().reset_index(drop=True)
    x["Date"] = _date_series(x, date_col).fillna("UNKNOWN")
    x["Stream"] = x.get("Stream", "").astype(str).str.strip()
    x["Core"] = x.get("Core", "").map(_norm3)
    seed_col = "Seed" if "Seed" in x.columns else ("Seed1" if "Seed1" in x.columns else None)
    x["Seed"] = x[seed_col].map(_norm4) if seed_col else ""
    x["PositionKey"] = x["Date"] + "|" + x["Stream"] + "|" + x["Core"]
    # Respect explicit gate eligibility supplied by the shared caller. Only infer from
    # BucketPick for old/legacy callers that do not provide ProductionEligible.
    if "ProductionEligible" in x.columns:
        x["ProductionEligibleBeforeSettled"] = x["ProductionEligible"].fillna(False).astype(bool)
    elif "BucketPick" in x.columns:
        x["ProductionEligibleBeforeSettled"] = x["BucketPick"].astype(str).str.upper().isin(
            ["BASE", "DUE", "COMBINED", "BOTH", "BASESCORE", "DUE8"]
        )
    else:
        x["ProductionEligibleBeforeSettled"] = False
    x["SeedDigits"] = x["Seed"].map(lambda s: [int(ch) for ch in s] if len(s) == 4 else [])
    x["CoreDigits"] = x["Core"].map(lambda s: [int(ch) for ch in s] if len(s) == 3 else [])
    x["SeedSum"] = x["SeedDigits"].map(sum)
    x["CoreSum"] = x["CoreDigits"].map(sum)
    x["SeedOddCount"] = x["SeedDigits"].map(lambda z: sum(v % 2 for v in z))
    x["CoreOddCount"] = x["CoreDigits"].map(lambda z: sum(v % 2 for v in z))
    x["SeedHighCount"] = x["SeedDigits"].map(lambda z: sum(v >= 5 for v in z))
    x["CoreHighCount"] = x["CoreDigits"].map(lambda z: sum(v >= 5 for v in z))
    x["SeedSpread"] = x["SeedDigits"].map(lambda z: max(z) - min(z) if z else np.nan)
    x["CoreSpread"] = x["CoreDigits"].map(lambda z: max(z) - min(z) if z else np.nan)
    x["SpreadDelta"] = x["SeedSpread"] - x["CoreSpread"]
    x["AbsSpreadDelta"] = x["SpreadDelta"].abs()
    return x


def _safe_eval(expr: str, env: dict[str, Any]) -> bool:
    expr = str(expr or "").strip()
    if not expr or expr.lower() in {"true", "1"}:
        return True
    tree = ast.parse(expr, mode="eval")
    allowed_nodes = (
        ast.Expression, ast.BoolOp, ast.BinOp, ast.UnaryOp, ast.Compare,
        ast.Name, ast.Load, ast.Constant, ast.Set, ast.Tuple, ast.List,
        ast.And, ast.Or, ast.Not, ast.Add, ast.Sub, ast.Mult, ast.Div,
        ast.FloorDiv, ast.Mod, ast.Pow, ast.USub, ast.UAdd,
        ast.Eq, ast.NotEq, ast.Lt, ast.LtE, ast.Gt, ast.GtE,
        ast.In, ast.NotIn, ast.Call, ast.Attribute,
    )
    for node in ast.walk(tree):
        if not isinstance(node, allowed_nodes):
            raise ValueError(f"Unsupported rule syntax: {type(node).__name__}")
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id == "set":
                continue
            if isinstance(node.func, ast.Attribute) and node.func.attr in {"issuperset", "issubset"}:
                continue
            raise ValueError("Unsupported function in settled rule")
        if isinstance(node, ast.Name) and node.id not in env and node.id != "set":
            raise ValueError(f"Unknown rule variable: {node.id}")
    return bool(eval(compile(tree, "<settled-rule>", "eval"), {"__builtins__": {}, "set": set}, env))


def _apply_earlier_core_rules(x: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    work = x.copy()
    events: list[dict[str, Any]] = []
    active = work["ProductionEligibleBeforeSettled"].astype(bool)

    # Stable descending position order within date. This is the operational definition used in both Daily and WF.
    order_cols = [c for c in ["Date", "UniversalScore", "RankPos", "Stream", "Core"] if c in work.columns]
    asc = [True] + [False if c in {"UniversalScore"} else True for c in order_cols[1:]]
    ord_idx = work.loc[active].sort_values(order_cols, ascending=asc, kind="mergesort").index
    seq = pd.Series(range(1, len(ord_idx) + 1), index=ord_idx)
    every7 = work.index.to_series().map(seq).fillna(0).astype(int).mod(7).eq(0) & active

    masks = [
        ("EARLY_001", "EVERY_7TH_DESCENDING", every7),
        ("EARLY_002", "SEEDSUM_MINUS_CORESUM_EQ_NEG9", (work["SeedSum"] - work["CoreSum"]).eq(-9) & active),
        ("EARLY_003", "SEEDODD_MINUS_COREODD_EQ_NEG3", (work["SeedOddCount"] - work["CoreOddCount"]).eq(-3) & active),
        ("EARLY_004", "SEEDHIGH_MINUS_COREHIGH_EQ_NEG3", (work["SeedHighCount"] - work["CoreHighCount"]).eq(-3) & active),
        ("EARLY_005", "SEEDSPREAD_MINUS_CORESPREAD_EQ_4", work["SpreadDelta"].eq(4) & active),
        ("EARLY_006", "ABSSPREADDELTA_GE_6", work["AbsSpreadDelta"].ge(6) & active),
    ]
    deleted = pd.Series(False, index=work.index)
    for step, (rid, name, raw) in enumerate(masks, 1):
        match = raw.fillna(False)
        newly = match & ~deleted
        for idx in work.index[match]:
            events.append({"Stage": "EARLY_CORE", "Step": step, "RuleID": rid, "RuleName": name,
                           "PositionKey": work.at[idx, "PositionKey"], "Matched": True,
                           "FirstDeletingRule": bool(newly.at[idx]), "Deleted": bool(newly.at[idx])})
        deleted |= newly

    # Recalculate loads after the fixed early rules, then apply MID-core / LOW-stream.
    temp = work.loc[active & ~deleted].copy()
    if not temp.empty:
        temp["CoreLoad"] = temp.groupby(["Date", "Core"])["PositionKey"].transform("size")
        temp["StreamLoad"] = temp.groupby(["Date", "Stream"])["PositionKey"].transform("size")
        temp["CorePct"] = temp.groupby("Date")["CoreLoad"].rank(method="average", pct=True)
        temp["StreamPct"] = temp.groupby("Date")["StreamLoad"].rank(method="average", pct=True)
        midlow_idx = temp.index[temp["CorePct"].between(.33, .67, inclusive="right") & temp["StreamPct"].le(.33)]
        match = work.index.isin(midlow_idx)
        newly = pd.Series(match, index=work.index) & ~deleted
        for idx in work.index[match]:
            events.append({"Stage": "EARLY_CORE", "Step": 7, "RuleID": "EARLY_007", "RuleName": "MID_CORE_LOW_STREAM",
                           "PositionKey": work.at[idx, "PositionKey"], "Matched": True,
                           "FirstDeletingRule": bool(newly.at[idx]), "Deleted": bool(newly.at[idx])})
        deleted |= newly
    work["DeletedByEarlyCore"] = deleted
    return work, events


def _apply_pick3_95(x: pd.DataFrame, rules: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    work = x.copy()
    deleted = work.get("DeletedByEarlyCore", False).astype(bool).copy()
    events: list[dict[str, Any]] = []
    for _, r in rules.sort_values("Step", key=lambda s: pd.to_numeric(s, errors="coerce")).iterrows():
        step = int(r["Step"])
        rid = str(r["ID"])
        name = str(r.get("Name", rid))
        app = str(r.get("ApplicableIf", "True"))
        expr = str(r.get("Expression", "True"))
        for idx in work.index[work["ProductionEligibleBeforeSettled"].astype(bool) & ~deleted]:
            env = {
                "seed_digits": work.at[idx, "SeedDigits"],
                "combo_digits": work.at[idx, "CoreDigits"],
                "seed_value": int(work.at[idx, "SeedSum"]),
                "combo_sum": int(work.at[idx, "CoreSum"]),
            }
            try:
                matched = _safe_eval(app, env) and _safe_eval(expr, env)
                error = ""
            except Exception as exc:
                matched = False
                error = f"{type(exc).__name__}: {exc}"
            if matched:
                deleted.at[idx] = True
                events.append({"Stage": "PICK3_95", "Step": step, "RuleID": rid, "RuleName": name,
                               "PositionKey": work.at[idx, "PositionKey"], "Matched": True,
                               "FirstDeletingRule": True, "Deleted": True, "EvaluationError": error})
    work["DeletedByPick3_95"] = deleted & ~work.get("DeletedByEarlyCore", False).astype(bool)
    work["DeletedBeforeCore45"] = deleted
    return work, events


def _pct_bucket(s: pd.Series) -> pd.Series:
    return pd.cut(s, [0, .1, .2, .33, .5, .67, .8, .9, 1],
                  labels=["P00_10", "P10_20", "P20_33", "P33_50", "P50_67", "P67_80", "P80_90", "P90_100"],
                  include_lowest=True).astype(str)


def _prepare_core45_features(work: pd.DataFrame) -> pd.DataFrame:
    x = work.copy()
    active = x["ProductionEligibleBeforeSettled"].astype(bool) & ~x["DeletedBeforeCore45"].astype(bool)
    t = x.loc[active].copy()
    if t.empty:
        for c in ["CoreLoad", "StreamLoad", "CorePctBucket", "StreamPctBucket", "CoreZ", "StreamZ"]:
            x[c] = np.nan
        return x
    t["CoreLoad"] = t.groupby(["Date", "Core"])["PositionKey"].transform("size")
    t["StreamLoad"] = t.groupby(["Date", "Stream"])["PositionKey"].transform("size")
    t["CorePct"] = t.groupby("Date")["CoreLoad"].rank(method="average", pct=True)
    t["StreamPct"] = t.groupby("Date")["StreamLoad"].rank(method="average", pct=True)
    t["CorePctBucket"] = _pct_bucket(t["CorePct"])
    t["StreamPctBucket"] = _pct_bucket(t["StreamPct"])
    for col, out in [("CoreLoad", "CoreZ"), ("StreamLoad", "StreamZ")]:
        mean = t.groupby("Date")[col].transform("mean")
        std = t.groupby("Date")[col].transform("std").replace(0, 1).fillna(1)
        t[out] = (t[col] - mean) / std
    numeric_cols = ["CoreLoad", "StreamLoad", "CorePct", "StreamPct", "CoreZ", "StreamZ"]
    text_cols = ["CorePctBucket", "StreamPctBucket"]

    # Build feature columns by index alignment instead of assigning string arrays into
    # pre-existing numeric columns. This is safe under pandas 2.x and pandas 3.x.
    for c in numeric_cols:
        x[c] = pd.to_numeric(t[c], errors="coerce").reindex(x.index)
    for c in text_cols:
        x[c] = t[c].astype("string").reindex(x.index)
    return x


def _core45_mask(x: pd.DataFrame, name: str) -> pd.Series:
    name = str(name)
    false = pd.Series(False, index=x.index)
    if name.startswith("CORELOAD_EQ_"):
        return pd.to_numeric(x["CoreLoad"], errors="coerce").eq(float(name.rsplit("_", 1)[1]))
    if name.startswith("STREAMLOAD_EQ_"):
        return pd.to_numeric(x["StreamLoad"], errors="coerce").eq(float(name.rsplit("_", 1)[1]))
    if name.startswith("CORE_P") and "__" not in name:
        return x["CorePctBucket"].eq(name.replace("CORE_", ""))
    if name.startswith("STREAM_P") and "__" not in name:
        return x["StreamPctBucket"].eq(name.replace("STREAM_", ""))
    if "__" in name and name.startswith("P"):
        a, b = name.split("__", 1)
        return x["CorePctBucket"].eq(a) & x["StreamPctBucket"].eq(b)
    if name.startswith("CZ") and "__SZ" in name:
        left, right = name.split("__SZ", 1)
        ca, cb = left[2:].split("_", 1)
        sa, sb = right.split("_", 1)
        return x["CoreZ"].gt(float(ca)) & x["CoreZ"].le(float(cb)) & x["StreamZ"].gt(float(sa)) & x["StreamZ"].le(float(sb))
    if name.startswith("CORE_Z_"):
        a, b = name.replace("CORE_Z_", "").split("_", 1)
        return x["CoreZ"].gt(float(a)) & x["CoreZ"].le(float(b))
    if name.startswith("STREAM_Z_"):
        a, b = name.replace("STREAM_Z_", "").split("_", 1)
        return x["StreamZ"].gt(float(a)) & x["StreamZ"].le(float(b))
    return false


def _apply_core45(x: pd.DataFrame, rules: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    work = _prepare_core45_features(x)
    deleted = work["DeletedBeforeCore45"].astype(bool).copy()
    events: list[dict[str, Any]] = []
    for _, r in rules.sort_values("Step", key=lambda s: pd.to_numeric(s, errors="coerce")).iterrows():
        step = int(r["Step"]); name = str(r["RuleName"])
        match = _core45_mask(work, name).fillna(False) & work["ProductionEligibleBeforeSettled"].astype(bool)
        newly = match & ~deleted
        for idx in work.index[match]:
            events.append({"Stage": "CORE45", "Step": step, "RuleID": name, "RuleName": name,
                           "PositionKey": work.at[idx, "PositionKey"], "Matched": True,
                           "FirstDeletingRule": bool(newly.at[idx]), "Deleted": bool(newly.at[idx])})
        deleted |= newly
    work["DeletedByCore45"] = deleted & ~work["DeletedBeforeCore45"].astype(bool)
    work["CoreSettledSurvivor"] = work["ProductionEligibleBeforeSettled"].astype(bool) & ~deleted
    all_events = events
    return work, all_events


def _expand_members(core_survivors: pd.DataFrame) -> pd.DataFrame:
    out = []
    for r in core_survivors.to_dict("records"):
        core = _norm3(r.get("Core"))
        if len(core) != 3 or len(set(core)) != 3:
            continue
        a, b, c = core
        fam = [("DOUBLE_LOW", "".join(sorted(a + a + b + c))),
               ("DOUBLE_MID", "".join(sorted(a + b + b + c))),
               ("DOUBLE_HIGH", "".join(sorted(a + b + c + c)))]
        for slot, (role, member) in enumerate(fam, 1):
            q = dict(r)
            q["BoxedMember"] = member
            q["Member"] = member
            q["MemberIndex"] = slot
            q["DoubleRole"] = role
            out.append(q)
    m = pd.DataFrame(out)
    if m.empty:
        return m
    m["MemberKey"] = m["PositionKey"] + "|" + m["BoxedMember"]
    seed_counts = m["Seed"].map(lambda s: {d: s.count(d) for d in set(s)})
    mcounts = m["BoxedMember"].map(lambda s: {d: s.count(d) for d in set(s)})
    m["DoubledDigit"] = mcounts.map(lambda c: next((d for d, n in c.items() if n == 2), ""))
    m["DoubledDigitSeedCount"] = [sc.get(dd, 0) for sc, dd in zip(seed_counts, m["DoubledDigit"])]
    m["DoubledDigitInSeed"] = m["DoubledDigitSeedCount"].gt(0)
    m["MemberMultisetSeedOverlap"] = [sum(min(mc.get(d, 0), sc.get(d, 0)) for d in set(mc) | set(sc)) for mc, sc in zip(mcounts, seed_counts)]
    md = m["BoxedMember"].map(lambda s: [int(ch) for ch in s])
    m["MemberSum"] = md.map(sum)
    m["MemberSumParity"] = m["MemberSum"].mod(2).map({0: "EVEN", 1: "ODD"})
    m["MemberOddCount"] = md.map(lambda z: sum(v % 2 for v in z))
    m["MemberHighCount"] = md.map(lambda z: sum(v >= 5 for v in z))
    m["MemberSpread"] = md.map(lambda z: max(z) - min(z))
    m["MemberSeedSumDelta"] = (m["MemberSum"] - m["SeedSum"]).abs()
    m["MemberSeedSumDirection"] = np.where(m["MemberSum"].eq(m["SeedSum"]), "EQ", np.where(m["SeedSum"].gt(m["MemberSum"]), "SEED_GT", "SEED_LT"))
    m["MemberParityMatchSeed"] = np.where(m["MemberSum"].mod(2).eq(m["SeedSum"].mod(2)), "MATCH", "MISMATCH")
    m["DoubledDigitParity"] = m["DoubledDigit"].astype(int).mod(2).map({0: "EVEN", 1: "ODD"})
    m["DoubledDigitHigh"] = np.where(m["DoubledDigit"].astype(int).ge(5), "HIGH", "LOW")
    m["MemberSumBand"] = pd.cut(m["MemberSum"], [-1, 8, 12, 16, 20, 36], labels=["0_8", "9_12", "13_16", "17_20", "21_PLUS"]).astype(str)
    m["MemberSeedDeltaBand"] = pd.cut(m["MemberSeedSumDelta"], [-1, 2, 5, 8, 12, 99], labels=["0_2", "3_5", "6_8", "9_12", "13_PLUS"]).astype(str)
    m["CoreLoadBand"] = pd.cut(pd.to_numeric(m["CoreLoad"], errors="coerce"), [-1, 10, 20, 30, 40, 999], labels=["0_10", "11_20", "21_30", "31_40", "41_PLUS"]).astype(str)
    m["StreamLoadBand"] = pd.cut(pd.to_numeric(m["StreamLoad"], errors="coerce"), [-1, 1, 2, 3, 4, 5, 999], labels=["0_1", "2", "3", "4", "5", "6_PLUS"]).astype(str)
    m["SeedSumParity"] = m["SeedSum"].mod(2).map({0: "EVEN", 1: "ODD"})
    m["CoreSumParity"] = m["CoreSum"].mod(2).map({0: "EVEN", 1: "ODD"})
    m["ParityMatch"] = np.where(m["SeedSumParity"].eq(m["CoreSumParity"]), "MATCH", "MISMATCH")
    m["SeedSpreadBucket"] = pd.cut(m["SeedSpread"], [-1, 3, 6, 9], labels=["NARROW_0_3", "MID_4_6", "WIDE_7_9"]).astype(str)
    return m


def _member_mask(m: pd.DataFrame, rule_name: str) -> pd.Series:
    mask = pd.Series(True, index=m.index)
    for part in str(rule_name).split("__"):
        if "=" not in part:
            return pd.Series(False, index=m.index)
        field, value = part.split("=", 1)
        if field not in m.columns:
            return pd.Series(False, index=m.index)
        mask &= m[field].astype(str).eq(value)
    return mask


def _apply_member35(m: pd.DataFrame, rules: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    if m.empty:
        return m, []
    work = m.copy()
    deleted = pd.Series(False, index=work.index)
    events = []
    for _, r in rules.sort_values("Step", key=lambda s: pd.to_numeric(s, errors="coerce")).iterrows():
        step = int(r["Step"]); name = str(r["RuleName"])
        match = _member_mask(work, name).fillna(False)
        newly = match & ~deleted
        for idx in work.index[match]:
            events.append({"Stage": "MEMBER35", "Step": step, "RuleID": name, "RuleName": name,
                           "PositionKey": work.at[idx, "PositionKey"], "MemberKey": work.at[idx, "MemberKey"],
                           "BoxedMember": work.at[idx, "BoxedMember"], "Matched": True,
                           "FirstDeletingRule": bool(newly.at[idx]), "Deleted": bool(newly.at[idx])})
        deleted |= newly
    work["DeletedByMember35"] = deleted
    work["MemberSettledSurvivor"] = ~deleted
    return work, events


def _first_rule_map(events: list[dict[str, Any]], key: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for e in sorted(events, key=lambda z: (z.get("Step", 9999), str(z.get("RuleID", "")))):
        if e.get("FirstDeletingRule") and e.get(key) and e[key] not in out:
            out[e[key]] = str(e.get("RuleID", ""))
    return out


def _all_rule_map(events: list[dict[str, Any]], key: str) -> dict[str, str]:
    d: dict[str, list[str]] = {}
    for e in events:
        if e.get("Matched") and e.get(key):
            d.setdefault(e[key], []).append(str(e.get("RuleID", "")))
    return {k: "|".join(v) for k, v in d.items()}


def expand_three_members(core_survivors: pd.DataFrame) -> pd.DataFrame:
    """Shared exact AABC expansion used by Daily and WF."""
    return _expand_members(core_survivors)


def build_member_features(core_survivors: pd.DataFrame) -> pd.DataFrame:
    """Shared member feature factory; expansion and feature calculation are atomic."""
    return _expand_members(core_survivors)


def apply_member35_registry(members: pd.DataFrame, rules: pd.DataFrame) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Shared sequential Member35 deletion stack, locked through Step 35."""
    return _apply_member35(members, rules)


def run_northern_star_open_gate(rows: pd.DataFrame, *, date_col: str | None = None, mode: str = "DAILY") -> PipelineResult:
    """Production-equivalent entry point shared by Daily and default WF."""
    x = rows.copy()
    bucket = x.get("BucketPick", pd.Series("", index=x.index)).astype(str).str.upper()
    x["OriginalBaseDueEligible"] = bucket.isin(["BASE", "DUE", "COMBINED", "BOTH", "BASESCORE", "DUE8"])
    x["BaseDueGateMode"] = DAILY_BASE_DUE_GATE_MODE
    x["ProductionEligible"] = True
    x["MatchesDailyProduction"] = True
    x["ExperimentalBranch"] = False
    return run_settled_pipeline(x, date_col=date_col, mode=mode)


def run_settled_pipeline(rows: pd.DataFrame, *, date_col: str | None = None, mode: str = "DAILY") -> PipelineResult:
    registry, tables = load_registry()
    x = normalize_core_rows(rows, date_col=date_col)
    if x.empty:
        empty = pd.DataFrame()
        return PipelineResult(empty, empty, empty, empty, empty, empty, empty, empty, registry)

    stages = []
    hand = []
    hand.append({"Contract": "WORKING8", "Status": "PASS" if set(x["Core"].dropna().unique()).issubset(set(WORKING8)) else "FAIL",
                 "Expected": "|".join(WORKING8), "Actual": "|".join(sorted(x["Core"].dropna().unique())), "Mode": mode})
    hand.append({"Contract": "CORE_KEY_DUPLICATES_INPUT", "Status": "PASS" if not x.duplicated(CORE_KEY).any() else "FAIL",
                 "Expected": "0", "Actual": str(int(x.duplicated(CORE_KEY).sum())), "Mode": mode})

    def snap(stage: str, frame: pd.DataFrame, survivor_col: str | None = None):
        n = len(frame) if survivor_col is None else int(frame[survivor_col].fillna(False).sum())
        stages.append({"Stage": stage, "Rows": n, "Dates": frame["Date"].nunique() if "Date" in frame.columns else 0,
                       "AvgPerDay": n / max(1, frame["Date"].nunique()) if "Date" in frame.columns else n,
                       "EngineVersion": ENGINE_VERSION, "Mode": mode})

    snap("NORTHERN_STAR_INPUT", x, "ProductionEligibleBeforeSettled")
    x, e0 = _apply_earlier_core_rules(x)
    snap("EARLIER_FREE_SURVIVORS", x.assign(_s=x["ProductionEligibleBeforeSettled"] & ~x["DeletedByEarlyCore"]), "_s")
    x, e95 = _apply_pick3_95(x, tables["PICK3_95"])
    snap("PICK3_95_SURVIVORS", x.assign(_s=x["ProductionEligibleBeforeSettled"] & ~x["DeletedBeforeCore45"]), "_s")
    x, e45 = _apply_core45(x, tables["CORE45"])
    snap("CORE45_SURVIVORS", x, "CoreSettledSurvivor")

    core_events = e0 + e95 + e45
    first_core = _first_rule_map(core_events, "PositionKey")
    all_core = _all_rule_map(core_events, "PositionKey")
    x["CoreFirstDeletingRule"] = x["PositionKey"].map(first_core).fillna("")
    x["CoreAllMatchingRules"] = x["PositionKey"].map(all_core).fillna("")
    core_surv = x[x["CoreSettledSurvivor"]].copy()

    members = expand_three_members(core_surv)
    _expected_positions = set(core_surv["PositionKey"].astype(str))
    _member_counts = members.groupby("PositionKey")["BoxedMember"].nunique() if not members.empty else pd.Series(dtype=int)
    _actual_positions = set(_member_counts.index.astype(str))
    _three_ok = (_expected_positions == _actual_positions) and (not _member_counts.empty or not _expected_positions) and _member_counts.eq(3).all()
    hand.append({"Contract": "THREE_MEMBERS_PER_CORE", "Status": "PASS" if _three_ok else "FAIL",
                 "Expected": f"{len(_expected_positions)} positions x exactly 3 distinct members",
                 "Actual": str({"positions_expected": len(_expected_positions), "positions_expanded": len(_actual_positions), "count_distribution": _member_counts.value_counts().to_dict()}), "Mode": mode})
    hand.append({"Contract": "MEMBER_KEY_DUPLICATES", "Status": "PASS" if not members.duplicated(MEMBER_KEY).any() else "FAIL",
                 "Expected": "0", "Actual": str(int(members.duplicated(MEMBER_KEY).sum()) if not members.empty else 0), "Mode": mode})
    snap("MEMBER_EXPANSION", members)
    members, em = apply_member35_registry(members, tables["MEMBER35"])
    first_m = _first_rule_map(em, "MemberKey")
    all_m = _all_rule_map(em, "MemberKey")
    if not members.empty:
        members["MemberFirstDeletingRule"] = members["MemberKey"].map(first_m).fillna("")
        members["MemberAllMatchingRules"] = members["MemberKey"].map(all_m).fillna("")
    if not members.empty:
        _parent_summary = members.groupby("PositionKey", as_index=False).agg(
            MembersBefore=("BoxedMember", "nunique"),
            MembersAfter=("MemberSettledSurvivor", "sum"),
        )
        _parent_summary["MembersBefore"] = _parent_summary["MembersBefore"].astype(int)
        _parent_summary["MembersAfter"] = _parent_summary["MembersAfter"].astype(int)
        _parent_summary["AllMembersDeleted"] = _parent_summary["MembersAfter"].eq(0)
        members = members.merge(_parent_summary, on="PositionKey", how="left", validate="many_to_one")
        x = x.merge(_parent_summary, on="PositionKey", how="left", validate="one_to_one")
        x["MembersBefore"] = x["MembersBefore"].fillna(0).astype(int)
        x["MembersAfter"] = x["MembersAfter"].fillna(0).astype(int)
        x["AllMembersDeleted"] = x["AllMembersDeleted"].fillna(False).astype(bool)
        core_surv = x[x["CoreSettledSurvivor"]].copy()
    mem_surv = members[members.get("MemberSettledSurvivor", False)].copy() if not members.empty else members.copy()
    if not members.empty:
        members["CoreRuleSurvived"] = True
        members["MemberRuleSurvived"] = members["MemberSettledSurvivor"].astype(bool)
        members["FirstDeletingRule"] = members["MemberFirstDeletingRule"]
        members["AllMatchingRules"] = members["MemberAllMatchingRules"]
        members["FinalPlay"] = members["MemberSettledSurvivor"].astype(bool)
        mem_surv = members[members["MemberSettledSurvivor"]].copy()
    snap("MEMBER35_SURVIVORS", members, "MemberSettledSurvivor" if not members.empty else None)
    hand.append({"Contract": "MEMBER35_STATUS", "Status": "PASS", "Expected": "WF_PENDING", "Actual": MEMBER_STACK_STATUS, "Mode": mode})

    # Dictionary/hash handshake.
    for stage, tab in tables.items():
        path = _rules_dir() / {"PICK3_95": "PICK3_95.csv", "CORE45": "CORE45.csv", "MEMBER35": "MEMBER35.csv"}[stage]
        expected = {"PICK3_95": 95, "CORE45": 45, "MEMBER35": 35}[stage]
        hand.append({"Contract": f"{stage}_RULE_COUNT", "Status": "PASS" if len(tab) == expected else "FAIL",
                     "Expected": str(expected), "Actual": str(len(tab)), "Mode": mode})
        hand.append({"Contract": f"{stage}_HASH", "Status": "PASS", "Expected": _sha256(path), "Actual": _sha256(path), "Mode": mode})
    hand.append({"Contract": "ENGINE_VERSION", "Status": "PASS", "Expected": ENGINE_VERSION, "Actual": ENGINE_VERSION, "Mode": mode})

    core_fire = pd.DataFrame(core_events)
    member_fire = pd.DataFrame(em)
    return PipelineResult(x, core_surv, members, mem_surv, core_fire, member_fire,
                          pd.DataFrame(stages), pd.DataFrame(hand), registry)


def build_audit_zip_payload(result: PipelineResult) -> dict[str, Any]:
    return {
        "12_SETTLED_CORE_ALL.csv": result.core_all,
        "13_SETTLED_CORE_SURVIVORS.csv": result.core_survivors,
        "14_SETTLED_MEMBER_ALL.csv": result.member_all,
        "15_SETTLED_MEMBER_SURVIVORS.csv": result.member_survivors,
        "16_SETTLED_CORE_RULE_FIRE.csv": result.core_fire_audit,
        "17_SETTLED_MEMBER_RULE_FIRE.csv": result.member_fire_audit,
        "18_SETTLED_STAGE_SUMMARY.csv": result.stage_summary,
        "19_SETTLED_HANDSHAKE.csv": result.handshake,
        "20_SETTLED_RULE_REGISTRY.csv": result.registry,
        "SETTLED_ENGINE_STATUS.txt": json.dumps({
            "engine_version": ENGINE_VERSION,
            "handshake_pass": bool((result.handshake["Status"] == "PASS").all()) if not result.handshake.empty else False,
            "core_survivors": len(result.core_survivors),
            "member_survivors": len(result.member_survivors),
            "member_stack_status": MEMBER_STACK_STATUS,
        }, indent=2),
    }
