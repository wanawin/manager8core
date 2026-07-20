from __future__ import annotations

import io
import re
import zipfile
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

BUILD = "MEMBER_RULE_ENGINE_V1"
WORKING8 = ["027", "148", "235", "257", "279", "356", "469", "579"]


def norm_core(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    if not digits:
        return ""
    return "".join(sorted(digits.zfill(3)[-3:]))


def norm_seed(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    return digits.zfill(4)[-4:] if digits else ""


def box_member(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    if not digits:
        return ""
    return "".join(sorted(digits.zfill(4)[-4:]))


def canonical_members(core: Any) -> list[str]:
    c = norm_core(core)
    if len(c) != 3 or len(set(c)) != 3:
        return []
    return sorted("".join(sorted(c + repeated)) for repeated in c)


def canonical_member_index(core: Any, member: Any) -> int | None:
    boxed = box_member(member)
    members = canonical_members(core)
    try:
        return members.index(boxed)
    except ValueError:
        return None


def portable_seed_traits(seed: Any) -> dict[str, float]:
    s = norm_seed(seed)
    if len(s) != 4:
        return {}
    d = [int(x) for x in s]
    outer_sum = d[0] + d[3]
    inner_sum = d[1] + d[2]
    digitset = int("".join(sorted(set(s))))
    fib_digits = {0, 1, 2, 3, 5, 8}
    return {
        "GP_FEATURE_0059": float(d[0] * d[3]),  # verified exact alias of tf_outer_product
        "tf_outer_product": float(d[0] * d[3]),
        "tf_mirror_pos2_pos3": float(int(d[1] + d[2] == 9)),
        "tf_fib_count": float(sum(1 for x in d if x in fib_digits)),
        "tf_seed_digitset": float(digitset),
        "tf_abs_outer_minus_inner": float(abs(outer_sum - inner_sum)),
    }


def _compare(value: float, operator: str, threshold1: float, threshold2: float | None) -> bool:
    if pd.isna(value):
        return False
    op = str(operator).strip().lower()
    if op == "ge":
        return value >= threshold1
    if op == "gt":
        return value > threshold1
    if op == "le":
        return value <= threshold1
    if op == "lt":
        return value < threshold1
    if op == "eq":
        return value == threshold1
    if op == "between":
        return threshold2 is not None and threshold1 <= value <= threshold2
    raise ValueError(f"Unsupported member rule operator: {operator}")


def load_rules(path_or_file: Any) -> pd.DataFrame:
    if path_or_file is None:
        return pd.DataFrame()
    if hasattr(path_or_file, "read"):
        data = path_or_file.read()
        if hasattr(path_or_file, "seek"):
            path_or_file.seek(0)
        rules = pd.read_csv(io.BytesIO(data) if isinstance(data, bytes) else io.StringIO(data))
    else:
        rules = pd.read_csv(Path(path_or_file))
    required = {
        "rule_id", "status", "core", "feature", "operator", "threshold1",
        "threshold2", "play_pair", "omit_member", "priority",
    }
    missing = sorted(required - set(rules.columns))
    if missing:
        raise ValueError("Member rule CSV missing columns: " + ", ".join(missing))
    rules = rules.copy()
    rules["core"] = rules["core"].map(norm_core)
    rules["omit_member"] = pd.to_numeric(rules["omit_member"], errors="raise").astype(int)
    rules["priority"] = pd.to_numeric(rules["priority"], errors="coerce").fillna(999999).astype(int)
    rules["threshold1"] = pd.to_numeric(rules["threshold1"], errors="raise")
    rules["threshold2"] = pd.to_numeric(rules["threshold2"], errors="coerce")
    rules = rules[rules["status"].astype(str).str.upper().isin(["ACTIVE", "VALIDATED"])]
    if rules["rule_id"].duplicated().any():
        dup = rules.loc[rules["rule_id"].duplicated(False), "rule_id"].tolist()
        raise ValueError(f"Duplicate member rule IDs: {dup}")
    return rules.sort_values(["core", "priority", "rule_id"]).reset_index(drop=True)


def _winner_from_row(row: pd.Series) -> str:
    for col in ["WinnerResult", "winner_result", "Result", "actual_result"]:
        if col in row.index and pd.notna(row.get(col)) and str(row.get(col)).strip():
            return str(row.get(col)).split("|")[0].strip()
    return ""


def apply_member_rules(core_rows: pd.DataFrame, rules: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if core_rows is None or core_rows.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    out_rows: list[dict[str, Any]] = []
    fire_rows: list[dict[str, Any]] = []
    input_df = core_rows.copy()
    seed_col = "SeedUsedForPlayDate" if "SeedUsedForPlayDate" in input_df.columns else "Seed"
    if seed_col not in input_df.columns:
        raise ValueError("Member engine requires SeedUsedForPlayDate or Seed.")
    if "Core" not in input_df.columns or "Stream" not in input_df.columns:
        raise ValueError("Member engine requires Core and Stream.")

    for idx, row in input_df.iterrows():
        core = norm_core(row.get("Core"))
        seed = norm_seed(row.get(seed_col))
        members = canonical_members(core)
        traits = portable_seed_traits(seed)
        applicable = rules[rules["core"] == core].copy()
        fired: list[dict[str, Any]] = []
        for _, rr in applicable.iterrows():
            feature = str(rr["feature"])
            value = traits.get(feature, np.nan)
            did_fire = _compare(value, rr["operator"], float(rr["threshold1"]), None if pd.isna(rr["threshold2"]) else float(rr["threshold2"]))
            fire_rows.append({
                "InputRow": idx,
                "PlayDate": row.get("PlayDate", ""),
                "HistoryThrough": row.get("HistoryThrough", ""),
                "Stream": str(row.get("Stream", "")),
                "Core": core,
                "Seed": seed,
                "RuleID": rr["rule_id"],
                "Feature": feature,
                "FeatureValue": value,
                "Operator": rr["operator"],
                "Threshold1": rr["threshold1"],
                "Threshold2": rr["threshold2"],
                "RuleFired": bool(did_fire),
                "RecommendedPair": rr["play_pair"],
                "OmitCanonicalMember": int(rr["omit_member"]),
            })
            if did_fire:
                fired.append(rr.to_dict())

        selected: dict[str, Any] | None = None
        conflict = False
        if fired:
            pairs = {str(r["play_pair"]) for r in fired}
            if len(pairs) == 1:
                selected = sorted(fired, key=lambda r: (int(r["priority"]), str(r["rule_id"])))[0]
            else:
                ordered = sorted(
                    fired,
                    key=lambda r: (
                        -float(r.get("untouched_support", 0) or 0),
                        -float(r.get("untouched_preservation", 0) or 0),
                        int(r["priority"]),
                        str(r["rule_id"]),
                    ),
                )
                if len(ordered) >= 2:
                    k0 = (float(ordered[0].get("untouched_support", 0) or 0), float(ordered[0].get("untouched_preservation", 0) or 0))
                    k1 = (float(ordered[1].get("untouched_support", 0) or 0), float(ordered[1].get("untouched_preservation", 0) or 0))
                    if k0 == k1:
                        conflict = True
                    else:
                        selected = ordered[0]
                else:
                    selected = ordered[0]

        omitted = None if selected is None else int(selected["omit_member"])
        pair = "ALL THREE / NO ELIMINATION" if selected is None else str(selected["play_pair"])
        kept_indices = [0, 1, 2] if omitted is None else [i for i in [0, 1, 2] if i != omitted]
        kept_members = [members[i] for i in kept_indices] if len(members) == 3 else []

        winner = _winner_from_row(row)
        winner_boxed = box_member(winner) if winner else ""
        winner_idx = canonical_member_index(core, winner_boxed) if winner_boxed else None
        preserved = True if winner_idx is None or omitted is None else winner_idx != omitted
        loss_rules = ""
        if winner_idx is not None and not preserved:
            loss_rules = str(selected["rule_id"]) if selected else ""

        out = row.to_dict()
        out.update({
            "MemberRuleCountAvailable": int(len(applicable)),
            "MemberRulesFiredCount": int(len(fired)),
            "MemberRulesFired": "|".join(str(r["rule_id"]) for r in fired),
            "MemberNoRuleFired": len(fired) == 0,
            "MemberRuleConflict": bool(conflict),
            "MemberSelectedRuleID": "" if selected is None else str(selected["rule_id"]),
            "MemberRecommendedPair": pair,
            "MemberOmittedCanonicalIndex": "" if omitted is None else omitted,
            "MemberKeptCanonicalIndices": "|".join(map(str, kept_indices)),
            "MemberCanonical0": members[0] if len(members) == 3 else "",
            "MemberCanonical1": members[1] if len(members) == 3 else "",
            "MemberCanonical2": members[2] if len(members) == 3 else "",
            "MemberRecommendedBoxedMembers": "|".join(kept_members),
            "WinnerBoxedMember": winner_boxed,
            "WinnerCanonicalIndex": "" if winner_idx is None else winner_idx,
            "MemberWinnerPreserved": bool(preserved),
            "MemberLossCausingRuleIDs": loss_rules,
            "MemberDecisionReason": (
                "CONFLICT_KEEP_ALL" if conflict else
                "NO_RULE_FIRED_KEEP_ALL" if selected is None else
                f"RULE_{selected['rule_id']}_OMIT_CM{omitted}"
            ),
        })
        out_rows.append(out)

    decisions = pd.DataFrame(out_rows)
    fire_audit = pd.DataFrame(fire_rows)
    if fire_audit.empty:
        summary = pd.DataFrame(columns=["RuleID", "RowsEvaluated", "FireCount", "NoFireCount", "FireRate"])
    else:
        summary = fire_audit.groupby("RuleID", as_index=False).agg(
            RowsEvaluated=("RuleFired", "size"),
            FireCount=("RuleFired", "sum"),
        )
        summary["NoFireCount"] = summary["RowsEvaluated"] - summary["FireCount"]
        summary["FireRate"] = summary["FireCount"] / summary["RowsEvaluated"]
    return decisions, fire_audit, summary


def member_results_zip(decisions: pd.DataFrame, fire_audit: pd.DataFrame, summary: pd.DataFrame, rules: pd.DataFrame) -> bytes:
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("00_MEMBER_BUILD.txt", BUILD)
        z.writestr("01_MEMBER_DECISIONS.csv", decisions.to_csv(index=False))
        z.writestr("02_MEMBER_RULE_FIRE_AUDIT.csv", fire_audit.to_csv(index=False))
        z.writestr("03_MEMBER_RULE_FIRE_SUMMARY.csv", summary.to_csv(index=False))
        losses = decisions[~decisions["MemberWinnerPreserved"].fillna(True).astype(bool)] if "MemberWinnerPreserved" in decisions.columns else pd.DataFrame()
        z.writestr("04_MEMBER_WINNER_LOSS_LEDGER.csv", losses.to_csv(index=False))
        nofire = decisions[decisions["MemberNoRuleFired"].fillna(False).astype(bool)] if "MemberNoRuleFired" in decisions.columns else pd.DataFrame()
        z.writestr("05_MEMBER_NO_RULE_FIRED.csv", nofire.to_csv(index=False))
        z.writestr("06_ACTIVE_MEMBER_RULES.csv", rules.to_csv(index=False))
    return bio.getvalue()
