from __future__ import annotations
import time
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
try:
    import polars as pl
except Exception:
    pl = None
try:
    import duckdb
except Exception:
    duckdb = None
# pk4_northern_star_app_2026-02-04_v41.py
# Streamlit app: Pick 4 "Northern Star" core stream ranking + Rare/Ultra-Rare engines (AAAB+AABB, AAAA)
# Notes:
# - Designed to work with LotteryPost-style exports (tab .txt or .csv) that include Date, State, Game, Results.
# - Ignores Wild Ball / Fireball / multipliers by extracting the first 4 digits like "1-2-3-4".
# - Excludes Maryland by default (toggle in sidebar).


APP_VERSION = "v51.45H (Legacy Matrix Intact + Complete WF Member Handoff + Polars/DuckDB)"

CHANGE_LOG_V51 = """v51 — SeedTraits + Cadence + AllCores Cache-Only (built from v50, NO regressions)

✅ Added:
- Seed Traits (positive + negative) autoload + optional upload; soft scoring applied to:
  - Northern Lights UniversalScore
  - Core scoring (Northern Star + Core View helper)
- Cadence scoring (windowed 180/365) as a soft, configurable boost (no hard filters)
- Northern Star tab (restores Rare Engine + Ultra-Rare engine outputs in UI)
- Global all-cores RankPos percentile map (cache-only) + per-core maps remain distinct
- “Select all cores” button for multi-core selection (cache building / batch tools)

✅ Fixed (signature/return + robustness):
- Added PctStrength alias to percentile map output (back-compat)
- Northern Lights: position strength now resolves via RankPos (not Stream) to avoid empty maps
- Bucket recommendations now include back-compat metadata keys (top_n / due_ranks / etc)
- All Cores mode in Northern Lights is now STRICT cache-only; refuses live compute if any core cache missing

Notes:
- No sections removed or disabled. New functionality is additive and defaults are conservative.
- v51.14 adds a separate Working 8 preset and a CSV-driven member pair-rule layer with firing, no-fire, conflict, and winner-loss audits.
"""


WORKING8_CORE_SET = ["027", "148", "235", "257", "279", "356", "469", "579"]
DAILY_BASE_DUE_GATE_MODE = "OPEN_GATE_FEATURE_ONLY"
BASE_DUE_GATE_MODE = DAILY_BASE_DUE_GATE_MODE
WF_GATE_MODES = ("OPEN_GATE_FEATURE_ONLY", "CURRENT_HARD_GATE", "AUDIT_FLIPPED_GATE")

# Core presets (family IDs) shown in the UI. Keep this list additive.
# These are the cores you and I have explicitly worked on so far.
CORE_PRESETS = [
    "012",
    "013",
    "016",
    "017",
    "018",
    "019",
    "023",
    "024",
    "025",
    "027",
    "028",
    "029",
    "035",
    "038",
    "046",
    "048",
    "056",
    "059",
    "067",
    "068",
    "078",
    "129",
    "134",
    "135",
    "138",
    "145",
    "146",
    "149",
    "167",
    "168",
    "169",
    "178",
    "179",
    "236",
    "238",
    "239",
    "245",
    "246",
    "249",
    "256",
    "257",
    "258",
    "278",
    "279",
    "345",
    "348",
    "357",
    "358",
    "359",
    "378",
    "379",
    "389",
    "456",
    "457",
    "458",
    "459",
    "468",
    "479",
    "489",
    "567",
    "568",
    "579",
    "589",
    "679",
    "689",
    "789",
]


# Compatibility: the legacy 'old app' core set (kept for quick selection)
OLD_APP_CORE_SET = ['016', '017', '018', '019', '023', '024', '025', '027', '028', '029', '038', '046', '048', '056', '059', '067', '068', '078', '129', '135', '145', '146', '149', '167', '168', '169', '179', '236', '238', '239', '245', '246', '249', '257', '258', '278', '279', '345', '348', '357', '359', '378', '379', '389', '457', '459', '489', '567', '579', '589', '679', '689', '789']
# --- Optional: "Trigger Map" weighting for a fixed 39-play list (soft boost, never an elimination) ---
# This is intentionally conservative: it only adds a small score nudge to prioritize certain plays per-stream
# based on the previous winner in that same stream.
TRIGGER_PLAYLIST_39 = [
    "3389","3889","3899",
    "0013","0113","0133","0019","0119","0199",
    "1145","1445","1455","1147","1447","1477","1149","1499","1449",
    "1136","1336","1366",
    "1667","1167","1677","1169","1669","1699",
    "3356","3566","3556","3367","3667","3677",
    "5567","5667","5677","6679","6779","6799",
]

# Override trigger: previous winner contains >=3 digits from {7,8,9,0}
_TRIGGER_OVERRIDE_SET = set("7890")

# Default decision tree: bucket by last digit of previous winner
TRIGGER_BY_PREV_LAST = {
    "0": ["3556","3899","1677","5677","3677"],
    "1": ["5567","0113","1499","1699","1167"],
    "2": ["1699","1677","3566","1667","1149"],
    "3": ["6679","1167","0019","3566","1669"],
    "4": ["1366","1667","3356","1455"],
    "5": ["0133","1147","1136","1445","1145"],
    "6": ["1449","0199","3356","3367","3556"],
    "7": ["3677","1145","0013","1447","1169"],
    "8": ["1366","1149","3389","1669","5667"],
    "9": ["1445","1149","6679","1669","1169"],
}

TRIGGER_OVERRIDE_EMPHASIS = ["3889","3677","1169","3899","3556","6679"]

def trigger_map_emphasis(prev_result_4d: str) -> list[str]:
    """Return a (possibly empty) ordered emphasis list for the Trigger Map."""
    s = re.sub(r"\D", "", str(prev_result_4d or ""))[:4]
    if len(s) != 4:
        return []
    # Override: >=3 digits from 7/8/9/0
    if sum(1 for ch in s if ch in _TRIGGER_OVERRIDE_SET) >= 3:
        return list(TRIGGER_OVERRIDE_EMPHASIS)
    return list(TRIGGER_BY_PREV_LAST.get(s[-1], []))

def trigger_map_boost(play_4d: str, prev_result_4d: str, *, boost_points: float = 2.0) -> float:
    """Soft boost points for a play given the previous result in the stream."""
    if not play_4d:
        return 0.0
    p = re.sub(r"\D", "", str(play_4d)).zfill(4)[-4:]
    emph = trigger_map_emphasis(prev_result_4d)
    if not emph:
        return 0.0
    return float(boost_points) if p in set(emph) else 0.0


import re

def _safe_int(x):
    """Convert x to int if possible; returns None if not."""
    if x is None:
        return None
    if isinstance(x, int):
        return int(x)
    try:
        import numpy as _np
        if isinstance(x, (_np.integer,)):
            return int(x)
    except Exception:
        pass
    if isinstance(x, str):
        m = re.search(r"\d+", x)
        if not m:
            return None
        try:
            return int(m.group(0))
        except Exception:
            return None
    try:
        return int(x)
    except Exception:
        return None

import math
import itertools
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Iterable, Any

import numpy as np
import pandas as pd

def _safe_pd_concat(objs, *args, **kwargs):
    """Concatenate frames without propagating unsafe pandas metadata.

    Some legacy result frames carry DataFrame/PipelineResult objects in ``attrs``.
    Pandas compares attrs during concat finalization, which can trigger the
    ambiguous DataFrame truth-value exception.  This helper preserves all rows,
    columns, dtypes, and ordering while clearing metadata only on shallow input
    copies.  The legacy matrix calculations themselves are untouched.
    """
    if objs is None:
        return pd.DataFrame()
    if isinstance(objs, (pd.DataFrame, pd.Series)):
        seq = [objs]
    else:
        seq = list(objs)
    clean = []
    for obj in seq:
        if isinstance(obj, (pd.DataFrame, pd.Series)):
            cp = obj.copy(deep=False)
            try:
                cp.attrs = {}
            except Exception:
                pass
            clean.append(cp)
        else:
            clean.append(obj)
    return pd.concat(clean, *args, **kwargs)

def _wf_nonempty_list(value) -> list:
    """Normalize UI/session selections without evaluating DataFrames as booleans."""
    if value is None:
        return []
    if isinstance(value, pd.DataFrame):
        if value.empty:
            return []
        return value.iloc[:, 0].dropna().astype(str).tolist()
    if isinstance(value, pd.Series):
        return value.dropna().astype(str).tolist()
    if isinstance(value, (list, tuple, set, pd.Index, np.ndarray)):
        return list(value)
    return [value]

def _wf_first_nonempty_selection(*values) -> list:
    for value in values:
        normalized = _wf_nonempty_list(value)
        if len(normalized) > 0:
            return normalized
    return []

def _wf_scalar_bool(value, default: bool = False) -> bool:
    """Convert only scalar values to bool; never evaluate a Series/DataFrame truth value."""
    if isinstance(value, (pd.DataFrame, pd.Series, np.ndarray, list, tuple, set, dict)):
        return default
    if value is None or value is pd.NA:
        return default
    try:
        if pd.isna(value):
            return default
    except Exception:
        pass
    return bool(value)

def _to_dataframe(obj) -> pd.DataFrame:
    """Best-effort conversion for Streamlit display; prevents 'dict has no dtype' crashes."""
    if isinstance(obj, pd.DataFrame):
        return obj
    if obj is None:
        return pd.DataFrame()
    if isinstance(obj, dict):
        # Prefer a single-row DF for dicts
        return pd.DataFrame([obj])
    try:
        return pd.DataFrame(obj)
    except Exception:
        return pd.DataFrame({"value": [str(obj)]})

import streamlit as st

def _arrow_safe_dataframe(value):
    """Return a Streamlit/PyArrow-safe dataframe without altering rows.

    Pandas allows duplicate column labels; PyArrow does not. Keep the first
    occurrence in original order and normalize duplicate index-level names.
    """
    df = _to_dataframe(value).copy()
    if not df.columns.is_unique:
        df = df.loc[:, ~df.columns.duplicated(keep="first")].copy()
    try:
        if isinstance(df.index, pd.MultiIndex):
            names = list(df.index.names)
            seen = set()
            fixed = []
            for i, name in enumerate(names):
                base = str(name) if name not in (None, "") else f"index_{i}"
                candidate = base
                n = 2
                while candidate in seen or candidate in set(map(str, df.columns)):
                    candidate = f"{base}_{n}"
                    n += 1
                fixed.append(candidate)
                seen.add(candidate)
            df.index = df.index.set_names(fixed)
        elif df.index.name is not None and str(df.index.name) in set(map(str, df.columns)):
            df.index = df.index.rename(f"{df.index.name}_index")
    except Exception:
        pass
    return df

def _safe_st_dataframe(value, *args, **kwargs):
    """Single guarded path for every Streamlit dataframe display."""
    return st.dataframe(_arrow_safe_dataframe(value), *args, **kwargs)

from ns_core_set_lab import render_core_set_lab
from member_rule_engine import load_rules as load_member_pair_rules, apply_member_rules
from settled_rules_engine import (
    run_settled_pipeline, run_northern_star_open_gate, build_audit_zip_payload,
    ENGINE_VERSION as SETTLED_ENGINE_VERSION, MEMBER_STACK_STATUS
)

# --- Safety init (prevents NameError when UI blocks are skipped) ---
member_track: bool = False

import hashlib
import datetime
import json
from functools import lru_cache

# ---- Safe defaults to prevent NameError during first render ----
cfg = None  # set later after window selection
exclude_md = True  # default behavior: exclude Maryland unless user toggles off
map_file = None  # backward-compatible alias set in sidebar

# Tab containers (assigned after tabs() is created; keep placeholders to avoid NameError)
_t_nl = None
_t_ns = None
_t_core = None
_t_bt = None

# ---- Rerun helper (must be defined early; used by core-selection buttons) ----
def _rerun() -> None:
    """Compatibility rerun across Streamlit versions."""
    try:
        # Streamlit >= 1.30
        st.rerun()
        return
    except Exception:
        pass
    try:
        # Older Streamlit
        st.experimental_rerun()
        return
    except Exception:
        pass
    # Last resort: no-op (should not happen on Streamlit Cloud)
    return






# -------------------------
# Parsing + helpers
# -------------------------

# -------------------------
# Disk baseline cache (optional, keeps runs fast)
# -------------------------
from pathlib import Path as _Path

DISK_CACHE_DIR = _Path("pk4_baseline_cache")

DISK_PCT_DIR = DISK_CACHE_DIR / "pctmaps"
DISK_PCT_DIR.mkdir(parents=True, exist_ok=True)

def _pctmap_path(core: str, window_days: int) -> Path:
    safe_core = core.replace("/", "_")
    return DISK_PCT_DIR / f"rankpos_pct_{safe_core}_{window_days}d.csv"

def _save_pctmap_to_disk(core: str, window_days: int, pct_df: pd.DataFrame, asof_last_date: str) -> None:
    if pct_df is None or pct_df.empty:
        return
    out = pct_df.copy()
    out.insert(0, "core", core)
    out.insert(1, "window_days", int(window_days))
    out.insert(2, "asof_last_date", asof_last_date)
    out.to_csv(_pctmap_path(core, window_days), index=False)

def _load_pctmap_from_disk(core: str, window_days: int, expected_last_date: Optional[str] = None) -> Optional[pd.DataFrame]:
    p = _pctmap_path(core, window_days)
    if not p.exists():
        return None
    try:
        df = pd.read_csv(p)
    except Exception:
        return None
    if expected_last_date is not None and "asof_last_date" in df.columns:
        # Keep only if matches current history last date (prevents stale/inaccurate maps)
        if str(df["asof_last_date"].iloc[0]) != str(expected_last_date):
            return None
    return df

def build_allcores_rankpos_pctmap(
    cores_list: List[str],
    window_days: int,
    expected_last_date: Optional[str],
    cache_only: bool = True,
    df_all: Optional[pd.DataFrame] = None,
    stream_rankings_df: Optional[pd.DataFrame] = None,
    family_counts_df: Optional[pd.DataFrame] = None,
    struct_counts_df: Optional[pd.DataFrame] = None,
    cfg: Optional["RankConfig"] = None,
) -> Tuple[Optional[pd.DataFrame], List[str]]:
    """Aggregate RankPos->HitCount across many cores and return a percentile map.

    If cache_only=True, requires baseline cache for every core; if any is missing/outdated,
    returns None (caller decides how to handle).
    """
    frames = []
    missing = []
    for core in cores_list:
        if cache_only:
            ss, _pos_df, _meta = _load_baseline_from_disk(core, window_days, expected_last_date=expected_last_date)
            if ss is None or ss.empty:
                missing.append(str(core).zfill(3))
                continue
        else:
            if cfg is None:
                cfg = RankConfig()
                cfg.window_days = window_days
            ss = compute_stream_stats(df_all, core, window_days=window_days, exclude_md=False)
        if ss is None or ss.empty or "RankPos" not in ss.columns:
            continue
        # Keep minimal columns
        if "HitsWindow" in ss.columns:
            frames.append(ss[["RankPos", "HitsWindow"]].copy())
        elif "HitCount" in ss.columns:
            tmp = ss[["RankPos", "HitCount"]].copy()
            tmp = tmp.rename(columns={"HitCount": "HitsWindow"})
            frames.append(tmp)

    if missing:
        return None, missing
    if not frames:
        return pd.DataFrame(), []

    comb = _safe_pd_concat(frames, ignore_index=True)
    # Aggregate by RankPos; position_percentile_map will also group, but we keep it clean
    comb = comb.groupby("RankPos", as_index=False)["HitsWindow"].sum()
    pct, _ = position_percentile_map(comb)
    return pct, []
DISK_CACHE_DIR.mkdir(exist_ok=True)

# -------------------------
# Rolling baseline store (optional)
# - Lets the app "self-maintain" a rolling ~3-year history by appending from the 24h file
# - Purges rows older than ~3 years from the newest date in the store
# - Stored on disk as parquet (preferred) or CSV (fallback), plus a small JSON meta file
# -------------------------
BASELINE_STORE_DIR = _Path("pk4_baseline_store")
BASELINE_STORE_DIR.mkdir(exist_ok=True)
BASELINE_STORE_BASE = BASELINE_STORE_DIR / "pk4_allstates_rolling_3y"


def _ensure_list(x):
    """Return x as a list suitable for pandas .isin()."""
    if x is None:
        return []
    if isinstance(x, (list, tuple, set)):
        return list(x)
    # pandas Series / Index
    try:
        import pandas as _pd
        if isinstance(x, (_pd.Series, _pd.Index)):
            return x.tolist()
    except Exception:
        pass
    # scalar -> list
    return [x]

def _coerce_store_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    # Ensure Date is datetime
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df[df["Date"].notna()].copy()
    # Ensure required columns exist (best-effort)
    for c in ["State","Game","Results","Pick4","Structure","Box","Stream"]:
        if c not in df.columns:
            df[c] = None
    return df

def load_baseline_store() -> pd.DataFrame:
    df = _safe_read_table(BASELINE_STORE_BASE)
    if df is None:
        return pd.DataFrame()
    df = _coerce_store_df(df)
    # Recompute derived fields if store was CSV without them
    if not df.empty:
        if "Pick4" not in df.columns or df["Pick4"].isna().all():
            df["Pick4"] = df.get("Results", pd.Series([None]*len(df))).map(extract_pick4_digits)
        if "Structure" not in df.columns or df["Structure"].isna().all():
            df["Structure"] = df["Pick4"].map(structure_of_4)
        if "Box" not in df.columns or df["Box"].isna().all():
            df["Box"] = df["Pick4"].map(box_key)
        if "Stream" not in df.columns or df["Stream"].isna().all():
            df["Stream"] = df["State"].astype(str).str.strip() + " | " + df["Game"].astype(str).str.strip()
        df = df[df["Pick4"].notna()].copy()
    return df

def write_baseline_store(df: pd.DataFrame, note: str = "") -> Tuple[bool, str]:
    df = _coerce_store_df(df)
    ok, path_written = _safe_write_table(df, BASELINE_STORE_BASE)
    meta = _read_meta(BASELINE_STORE_BASE)
    meta.update({
        "updated_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "note": note,
        "rows": int(df.shape[0]) if df is not None else 0,
        "max_date": str(df["Date"].max()) if df is not None and not df.empty else "",
    })
    _write_meta(BASELINE_STORE_BASE, meta)
    return ok, path_written

def purge_to_rolling_3y(df: pd.DataFrame, years: int = 3) -> pd.DataFrame:
    df = _coerce_store_df(df)
    if df.empty:
        return df
    max_date = df["Date"].max()
    # 3-year rolling window; add a small buffer for leap years
    cutoff = pd.Timestamp(max_date) - pd.Timedelta(days=(365*years + 7))
    df2 = df[df["Date"] >= cutoff].copy()
    return df2

def append_from_24h(df_store: pd.DataFrame, df_24h: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    df_store = _coerce_store_df(df_store)
    df_24h = _coerce_store_df(df_24h)
    if df_24h.empty:
        return df_store, 0
    # Only keep rows with the needed fields
    df_24h = df_24h[["Date","State","Game","Results","Pick4","Structure","Box","Stream"]].copy()
    df_store = df_store[["Date","State","Game","Results","Pick4","Structure","Box","Stream"]].copy() if not df_store.empty else df_store

    # Dedup key: Date + State + Game (unique stream-day draw)
    def _key(df):
        return (
            df["Date"].dt.strftime("%Y-%m-%d").astype(str)
            + "|" + df["State"].astype(str).str.strip().str.lower()
            + "|" + df["Game"].astype(str).str.strip().str.lower()
        )
    if df_store.empty:
        out = df_24h.copy()
        return out, int(out.shape[0])

    store_keys = set(_key(df_store).tolist())
    df_24h["_k"] = _key(df_24h)
    new_rows = df_24h[~df_24h["_k"].isin(store_keys)].drop(columns=["_k"]).copy()
    if new_rows.empty:
        return df_store, 0
    out = _safe_pd_concat([df_store, new_rows], ignore_index=True)
    # Final dedup safety
    out["_k"] = _key(out)
    out = out.drop_duplicates(subset=["_k"]).drop(columns=["_k"]).copy()
    return out, int(new_rows.shape[0])

def _cache_key(max_date: pd.Timestamp, rows: int, streams: int, exclude_md: bool, window_days: int, cores: List[str]) -> str:
    # small, stable key so your cache survives restarts
    core_sig = "-".join(cores)
    base = f"{max_date.date()}|{rows}|{streams}|md={int(exclude_md)}|w={window_days}|{core_sig}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]

def _parquet_available() -> bool:
    try:
        import pyarrow  # noqa: F401
        return True
    except Exception:
        try:
            import fastparquet  # noqa: F401
            return True
        except Exception:
            return False

def _safe_write_table(df: pd.DataFrame, path: _Path) -> Tuple[bool, str]:
    """Write as parquet if possible, else as CSV (human readable)."""
    try:
        if _parquet_available():
            df.to_parquet(path.with_suffix(".parquet"), index=False)
            return True, str(path.with_suffix(".parquet"))
        df.to_csv(path.with_suffix(".csv"), index=False)
        return True, str(path.with_suffix(".csv"))
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

def _safe_read_table(path: _Path) -> Optional[pd.DataFrame]:
    # Accept either a "base" path (no suffix) or a fully-qualified .parquet/.csv path.
    try:
        if path.suffix.lower() == ".parquet" and path.exists():
            return pd.read_parquet(path)
        if path.suffix.lower() == ".csv" and path.exists():
            return pd.read_csv(path)

        p_parq = path.with_suffix(".parquet")
        p_csv = path.with_suffix(".csv")
        if p_parq.exists():
            return pd.read_parquet(p_parq)
        if p_csv.exists():
            return pd.read_csv(p_csv)
    except Exception:
        return None
    return None


def _read_meta(path: _Path) -> Dict[str, Any]:
    p = path.with_suffix(".json")
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}

def _write_meta(path: _Path, meta: Dict[str, Any]) -> None:
    p = path.with_suffix(".json")
    p.write_text(json.dumps(meta, indent=2, default=str))

FOUR_DIGITS_RE = re.compile(r"(\d)\s*-\s*(\d)\s*-\s*(\d)\s*-\s*(\d)")


def _bytes_of_upload(uploaded) -> bytes:
    if uploaded is None:
        return b""
    try:
        return uploaded.getvalue()
    except Exception:
        try:
            return uploaded.read()
        except Exception:
            return b""

def file_fingerprint(uploaded) -> str:
    """Stable fingerprint for an uploaded file (used to auto-recompute when data changes)."""
    data = _bytes_of_upload(uploaded)
    if not data:
        return ""
    return hashlib.sha1(data).hexdigest()

def most_recent_date(df: pd.DataFrame) -> Optional[pd.Timestamp]:
    if df is None or df.empty or "Date" not in df.columns:
        return None
    try:
        return pd.to_datetime(df["Date"]).max()
    except Exception:
        return None
def extract_pick4_digits(results: str) -> Optional[str]:
    """Return 4-digit string from LotteryPost 'Results' cell, else None."""
    if results is None or (isinstance(results, float) and np.isnan(results)):
        return None
    m = FOUR_DIGITS_RE.search(str(results))
    if not m:
        # Sometimes results can be plain "1234"
        m2 = re.search(r"\b(\d{4})\b", str(results))
        if m2:
            return m2.group(1)
        return None
    return "".join(m.groups())

def box_key(s: str) -> str:
    return "".join(sorted(s))


from itertools import permutations

def extract_4digit(x: Any) -> Optional[str]:
    """Best-effort normalize to a 4-digit string (used for straight permutation generation)."""
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    s = str(x).strip()
    # If already exactly 4 digits
    m = re.search(r"(?<!\d)(\d{4})(?!\d)", s)
    if m:
        return m.group(1)
    # Try LotteryPost hyphenated format
    return extract_pick4_digits(s)


@lru_cache(maxsize=10000)
def unique_straights_for_box(box4: str) -> tuple[str, ...]:
    """Return unique 4-digit straight permutations for a 4-digit string (digits may repeat).

    Cached because the same box patterns repeat across streams/days.
    """
    box4 = extract_4digit(box4)
    if not box4:
        return tuple()
    return tuple(sorted({"".join(p) for p in permutations(box4, 4)}))

def _value_counts_result(df: pd.DataFrame) -> pd.Series:
    """Safe value_counts for Result column."""
    if df is None or df.empty:
        return pd.Series(dtype=int)
    if "Result" not in df.columns:
        return pd.Series(dtype=int)
    return df["Result"].astype(str).value_counts()

def _get_stream_result_counts(df_all: pd.DataFrame, stream: str) -> pd.Series:
    """Value counts of Result within a single stream."""
    if df_all is None or df_all.empty:
        return pd.Series(dtype=int)
    if "Stream" not in df_all.columns:
        return pd.Series(dtype=int)
    sub = df_all[df_all["Stream"].astype(str) == str(stream)]
    return _value_counts_result(sub)


def structure_of_4(d4: str) -> str:
    """Return AABC / AAAB / AABB / AAAA / ABCD based on counts."""
    from collections import Counter
    c = Counter(d4)
    counts = sorted(c.values(), reverse=True)
    if counts == [4]:
        return "AAAA"
    if counts == [3,1]:
        return "AAAB"
    if counts == [2,2]:
        return "AABB"
    if counts == [2,1,1]:
        return "AABC"
    return "ABCD"

def canonical_core_key(core: str) -> str:
    core = re.sub(r"\D", "", str(core))
    if len(core) == 3:
        return "".join(sorted(core))
    raise ValueError("Core must be 3 digits like 389")

def members_from_core(core_key: str, structure: str | None = None, **kwargs) -> List[str]:
    """Return 4-digit members for a 3-digit core.

    Two calling patterns are supported (backwards-compatible):
      1) members_from_core(core, "AABC") -> returns [AABC, ABBC, ABCC]
      2) members_from_core(core, include_family=True, include_aaab=True, include_aabb=True, include_aaaa=False)
         -> returns a combined, de-duplicated list of requested structures.
    """
    core_key = canonical_core_key(core_key)
    a, b, c = core_key[0], core_key[1], core_key[2]

    def _one(struct: str) -> List[str]:
        if struct == "AABC":
            x, y, z = f"{a}{a}{b}{c}", f"{a}{b}{b}{c}", f"{a}{b}{c}{c}"
        elif struct == "AAAB":
            x, y, z = f"{a}{a}{a}{b}", f"{a}{a}{a}{c}", f"{a}{b}{c}{c}"  # third is ABCC (rare engine uses it)
        elif struct == "AABB":
            x, y, z = f"{a}{a}{b}{b}", f"{a}{a}{c}{c}", f"{b}{b}{c}{c}"
        elif struct == "AAAA":
            x, y, z = f"{a}{a}{a}{a}", f"{b}{b}{b}{b}", f"{c}{c}{c}{c}"
        else:
            raise ValueError(f"Unknown structure: {struct}")
        return [box_key(x), box_key(y), box_key(z)]

    if structure is not None:
        return _one(structure)

    # Legacy / combined-call form
    include_family = bool(kwargs.get("include_family", True))
    include_aaab = bool(kwargs.get("include_aaab", False))
    include_aabb = bool(kwargs.get("include_aabb", False))
    include_aaaa = bool(kwargs.get("include_aaaa", False))

    out: List[str] = []
    if include_family:
        out.extend(_one("AABC"))
    if include_aaab:
        out.extend(_one("AAAB"))
    if include_aabb:
        out.extend(_one("AABB"))
    if include_aaaa:
        out.extend(_one("AAAA"))

    # De-duplicate while preserving order
    seen = set()
    res: List[str] = []
    for m in out:
        if m in seen:
            continue
        seen.add(m)
        res.append(m)
    return res



# ------------------------------------------------------------
# Core member labeling + member-pick prediction (walk-forward)
# ------------------------------------------------------------

@lru_cache(maxsize=4096)
def _core_member_label_map(core_key: str, include_rare: bool = False) -> dict[str, str]:
    """Map a core's member box-keys to human-readable member labels.

    Family (doubles) labels:
      - AABC = double of A (the first digit in sorted core)
      - ABBC = double of B
      - ABCC = double of C

    Rare labels (optional):
      - AAAB, AAAC (triple A with B/C)
      - AABB, AACC, BBCC
      - AAAA_A, AAAA_B, AAAA_C
    """
    core_key = canonical_core_key(core_key)
    a, b, c = core_key[0], core_key[1], core_key[2]

    m: dict[str, str] = {}

    # Family first (priority)
    fam_boxes = members_from_core(core_key, "AABC")
    for bk, lab in zip(fam_boxes, ["AABC", "ABBC", "ABCC"]):
        m.setdefault(str(bk), lab)

    if include_rare:
        # AAAB engine (note: third entry in members_from_core("AAAB") may overlap ABCC)
        aaab_boxes = members_from_core(core_key, "AAAB")
        for bk, lab in zip(aaab_boxes, ["AAAB", "AAAC", "ABCC"]):
            m.setdefault(str(bk), lab)

        aabb_boxes = members_from_core(core_key, "AABB")
        for bk, lab in zip(aabb_boxes, ["AABB", "AACC", "BBCC"]):
            m.setdefault(str(bk), lab)

        aaaa_boxes = members_from_core(core_key, "AAAA")
        for bk, lab in zip(aaaa_boxes, ["AAAA_A", "AAAA_B", "AAAA_C"]):
            m.setdefault(str(bk), lab)

    return m


def core_member_label(core_key: str, winner_4d: str, include_rare: bool = False) -> Optional[str]:
    """Return the member label for a core given a 4-digit winner (string).

    Uses box-key lookup first (fast and stable), then falls back to structure_of_4.
    """
    try:
        w = extract_4digit(winner_4d) or str(winner_4d).strip()
    except Exception:
        w = str(winner_4d).strip()
    if not w:
        return None
    bk = box_key(w)
    m = _core_member_label_map(core_key, include_rare=bool(include_rare))
    if bk in m:
        return m[bk]
    # Fallback (should be rare if winner is a member)
    try:
        return structure_of_4(w)
    except Exception:
        return None


def predict_core_member(
    df_all: pd.DataFrame,
    core_key: str,
    test_date: pd.Timestamp,
    window_days: int,
    *,
    basis: str = "core",
    stream: str | None = None,
    include_rare: bool = False,
) -> dict[str, Any]:
    """Predict which member label is most likely for this core (walk-forward safe).

    Prediction is based ONLY on rows with Date < test_date, restricted to the last `window_days`.

    basis:
      - "core": use all streams (global member distribution for that core)
      - "core_stream": use only that one stream (per-core-per-stream distribution)
    """
    if df_all is None or df_all.empty:
        return {"top1": None, "top2": None, "n": 0, "counts": {}}

    # Window slice: [test_date - window_days, test_date)
    try:
        td = pd.to_datetime(test_date).normalize()
    except Exception:
        td = pd.Timestamp(test_date).normalize()
    start = td - pd.Timedelta(days=int(window_days))

    sub = df_all
    try:
        if "Date" in sub.columns:
            sub = sub[sub["Date"].notna()]
            sub = sub[(sub["Date"] >= start) & (sub["Date"] < td)]
    except Exception:
        pass

    if basis == "core_stream" and stream is not None and "Stream" in sub.columns:
        try:
            sub = sub[sub["Stream"].astype(str) == str(stream)]
        except Exception:
            pass

    label_map = _core_member_label_map(core_key, include_rare=bool(include_rare))
    member_boxes = set(label_map.keys())
    box_col = "BoxKey4" if "BoxKey4" in sub.columns else ("Box" if "Box" in sub.columns else None)

    if box_col is None:
        # As a last resort, compute box keys on the fly
        try:
            tmp = sub.copy()
            tmp["_bk4"] = tmp["Result"].astype(str).map(box_key)
            box_col = "_bk4"
            sub = tmp
        except Exception:
            return {"top1": None, "top2": None, "n": 0, "counts": {}}

    try:
        hit_rows = sub[sub[box_col].astype(str).isin(member_boxes)]
    except Exception:
        hit_rows = pd.DataFrame()

    if hit_rows is None or hit_rows.empty:
        return {"top1": None, "top2": None, "n": 0, "counts": {}}

    # Map box keys -> labels (fast), then count
    labs = hit_rows[box_col].astype(str).map(label_map)
    counts = labs.value_counts(dropna=True)
    if counts.empty:
        return {"top1": None, "top2": None, "n": 0, "counts": {}}

    top = counts.index.tolist()
    top1 = top[0] if len(top) >= 1 else None
    top2 = top[1] if len(top) >= 2 else None
    return {
        "top1": top1,
        "top2": top2,
        "n": int(counts.sum()),
        "counts": counts.to_dict(),
    }



def _member_last_label(
    df_all: pd.DataFrame,
    core_key: str,
    test_date: pd.Timestamp,
    window_days: int,
    *,
    stream: str | None = None,
) -> tuple[Optional[str], int]:
    """Return LAST observed family-member label (AABC/ABBC/ABCC) for a core in the lookback window.
    Returns (label, n_hits_in_window). Walk-forward safe (uses Date < test_date only).
    """
    if df_all is None or df_all.empty:
        return (None, 0)
    td = pd.to_datetime(test_date).normalize()
    start = td - pd.Timedelta(days=int(window_days))
    sub = df_all
    if "Date" in sub.columns:
        sub = sub[sub["Date"].notna()]
        sub = sub[(sub["Date"] >= start) & (sub["Date"] < td)]
    if stream is not None and "Stream" in sub.columns:
        sub = sub[sub["Stream"].astype(str) == str(stream)]
    label_map = _core_member_label_map(core_key, include_rare=False)
    member_boxes = set(label_map.keys())
    box_col = "BoxKey4" if "BoxKey4" in sub.columns else ("Box" if "Box" in sub.columns else None)
    if box_col is None:
        tmp = sub.copy()
        tmp["_bk4"] = tmp["Result"].astype(str).map(box_key)
        box_col = "_bk4"
        sub = tmp
    hit_rows = sub[sub[box_col].astype(str).isin(member_boxes)].copy()
    if hit_rows.empty:
        return (None, 0)
    hit_rows = hit_rows.sort_values("Date")
    last_bk = hit_rows.iloc[-1][box_col]
    lab = label_map.get(str(last_bk))
    return (lab if lab in ("AABC", "ABBC", "ABCC") else None, int(len(hit_rows)))


def _seed_for_stream_asof(df_all: pd.DataFrame, stream: str, asof_date: pd.Timestamp) -> Optional[str]:
    """Most recent 4-digit result for a stream strictly before asof_date."""
    if df_all is None or df_all.empty:
        return None
    td = pd.to_datetime(asof_date).normalize()
    sub = df_all[(df_all["Date"] < td) & (df_all["Stream"].astype(str) == str(stream))].copy()
    if sub.empty:
        return None
    sub = sub.sort_values("Date")
    return str(sub.iloc[-1].get("Result", "")).strip() or None


def _seed_traits_for_core_stream(
    df_all: pd.DataFrame,
    core_key: str,
    stream: str,
    asof_date: pd.Timestamp,
) -> dict[str, str]:
    """Compute the standard seed-trait fields used in the seed-traits CSVs, for rulecards."""
    seed = _seed_for_stream_asof(df_all, stream, asof_date)
    if not seed:
        return {}
    seed = extract_4digit(seed) or seed
    digs = [int(ch) for ch in str(seed).zfill(4) if ch.isdigit()]
    if len(digs) != 4:
        return {}
    core_digs = set(str(core_key).zfill(3))
    ssum = sum(digs)
    spread = max(digs) - min(digs)
    even_cnt = sum(1 for d in digs if d % 2 == 0)
    high_cnt = sum(1 for d in digs if d >= 5)
    traits = {
        "seed_structure": structure_of_4(str(seed).zfill(4)),
        "seed_spread": ("<=2" if spread <= 2 else ("3-5" if spread <= 5 else ">=6")),
        "seed_even_count": str(even_cnt),
        "seed_high_count": str(high_cnt),
        "seed_sum_mod2": str(ssum % 2),
        "seed_sum_mod3": str(ssum % 3),
        # Sliding 4-sum band: (sum-1) to (sum+2) matches labels like 3-6, 11-14, etc.
        "seed_sum_range4_best": f"{ssum-1}-{ssum+2}",
        "seed_first_in_core": ("yes" if str(seed).zfill(4)[0] in core_digs else "no"),
        "seed_last_in_core": ("yes" if str(seed).zfill(4)[-1] in core_digs else "no"),
        "overlap_unique": str(len(set(str(seed).zfill(4)) & core_digs)),
        "seed_contains_core_pair": ("yes" if len(set(str(seed).zfill(4)) & core_digs) >= 2 else "no"),
    }
    # grid_last5_core_digits: how many of the core digits appear in the last-5 union digits for this stream (as of asof_date)
    try:
        sub = df_all[(df_all["Stream"].astype(str) == str(stream)) & (df_all["Date"] < pd.to_datetime(asof_date).normalize())].copy()
        sub = sub.sort_values("Date").tail(5)
        union = set("".join(sub["Result"].astype(str).tolist()))
        cnt = len(core_digs & union)
        if cnt == 3:
            traits["grid_last5_core_digits"] = "3"
        elif cnt >= 2:
            traits["grid_last5_core_digits"] = ">=2"
        else:
            traits["grid_last5_core_digits"] = str(cnt)
    except Exception:
        pass
    return traits


def _pick_best_seed_trait_rule(traits_pos_df: pd.DataFrame, core_key: str) -> Optional[tuple[str, str]]:
    """From the positive seed-traits CSV, pick the single highest-lift (trait, value) for this core."""
    if traits_pos_df is None or traits_pos_df.empty:
        return None
    ck = int(str(core_key).zfill(3))
    sub = traits_pos_df.copy()
    # core_family column in these CSVs is numeric (e.g., 12 for 012)
    sub = sub[sub["core_family"].astype(int) == ck]
    if sub.empty:
        return None
    sub = sub.sort_values(["lift", "trait_hits"], ascending=[False, False])
    r = sub.iloc[0]
    return (str(r.get("trait","")).strip(), str(r.get("value","")).strip())


def _member_mode_from_trait(
    df_all: pd.DataFrame,
    core_key: str,
    test_date: pd.Timestamp,
    window_days: int,
    trait_name: str,
    trait_value: str,
    *,
    stream: str | None = None,
) -> Optional[str]:
    """Within the walk-forward window, among hits where (trait==value) at the seed, return the MODE member label."""
    if not trait_name or trait_value is None:
        return None
    td = pd.to_datetime(test_date).normalize()
    start = td - pd.Timedelta(days=int(window_days))
    # Build stream-day transitions: seed = last result before day, winner = day's result.
    sub = df_all
    if "Date" not in sub.columns:
        return None
    sub = sub[sub["Date"].notna()].copy()
    sub = sub[(sub["Date"] >= start) & (sub["Date"] < td)]
    if stream is not None:
        sub = sub[sub["Stream"].astype(str) == str(stream)]
    if sub.empty:
        return None
    label_map = _core_member_label_map(core_key, include_rare=False)
    member_boxes = set(label_map.keys())
    # Keep only rows where the RESULT is a member of this core
    box_col = "BoxKey4" if "BoxKey4" in sub.columns else ("Box" if "Box" in sub.columns else None)
    if box_col is None:
        sub = sub.copy()
        sub["_bk4"] = sub["Result"].astype(str).map(box_key)
        box_col = "_bk4"
    hit = sub[sub[box_col].astype(str).isin(member_boxes)].copy()
    if hit.empty:
        return None
    # Compute trait per row based on the *seed* for that stream at that date (as-of that date)
    vals = []
    for _, r in hit.iterrows():
        s = str(r.get("Stream",""))
        d = pd.to_datetime(r.get("Date")).normalize()
        t = _seed_traits_for_core_stream(df_all, core_key, s, d).get(trait_name)
        vals.append(t)
    hit["_trait_val"] = vals
    hit = hit[hit["_trait_val"].astype(str) == str(trait_value)]
    if hit.empty:
        return None
    labs = hit[box_col].astype(str).map(label_map)
    vc = labs.value_counts()
    if vc.empty:
        return None
    top = vc.index.tolist()[0]
    return top if top in ("AABC","ABBC","ABCC") else None


def _member_prediction_variants(
    df_all: pd.DataFrame,
    traits_pos_df: pd.DataFrame,
    core_key: str,
    test_date: pd.Timestamp,
    window_days: int,
    *,
    stream: str,
    basis: str,
    min_stream_hits_for_last: int = 3,
) -> dict[str, Optional[str]]:
    """Compute member Top1 predictions under multiple strategies, walk-forward safe."""
    # MODE (same as existing predictor)
    mp_mode = predict_core_member(df_all, core_key, test_date, window_days, basis=("core_stream" if basis=="core_stream" else "core"), stream=(stream if basis=="core_stream" else None), include_rare=False)
    pred_mode = mp_mode.get("top1")
    # LAST(global)
    last_g, _ = _member_last_label(df_all, core_key, test_date, window_days, stream=None)
    # LAST(stream)
    last_s, n_s = _member_last_label(df_all, core_key, test_date, window_days, stream=stream)
    # Hierarchical LAST: use LAST(stream) if enough samples, else LAST(global), else MODE
    pred_last_h = None
    if last_s is not None and n_s >= int(min_stream_hits_for_last):
        pred_last_h = last_s
    elif last_g is not None:
        pred_last_h = last_g
    else:
        pred_last_h = pred_mode
    # Seed-structure override
    pred_seed_ovr = pred_last_h
    try:
        seed = _seed_for_stream_asof(df_all, stream, test_date)
        if seed:
            sstruct = structure_of_4(extract_4digit(seed) or str(seed).zfill(4))
            override = {"AAAB": "AABC", "AABB": "ABBC", "AAAA": "ABCC"}.get(str(sstruct))
            if override in ("AABC","ABBC","ABCC"):
                pred_seed_ovr = override
    except Exception:
        pass
    # Trait-lift override: use best (trait,value) for this core, then MODE among past hits where that trait fires
    pred_trait_ovr = pred_last_h
    best = _pick_best_seed_trait_rule(traits_pos_df, core_key)
    if best is not None:
        tname, tval = best
        # if the current seed's trait matches, apply the member-mode for that trait value
        cur_traits = _seed_traits_for_core_stream(df_all, core_key, stream, test_date)
        if cur_traits.get(tname) == tval:
            m = _member_mode_from_trait(df_all, core_key, test_date, window_days, tname, tval, stream=(stream if basis=="core_stream" else None))
            if m in ("AABC","ABBC","ABCC"):
                pred_trait_ovr = m
    return {
        "MODE": pred_mode,
        "LAST_GLOBAL": last_g,
        "LAST_HIER": pred_last_h,
        "SEED_OVERRIDE": pred_seed_ovr,
        "TRAIT_OVERRIDE": pred_trait_ovr,
    }

def try_read_tablelike(uploaded) -> pd.DataFrame:
    """
    Accept .csv or LotteryPost tab .txt.
    Expected columns (any case): Date, State, Game, Results (or Result/Winning Numbers).
    """
    if uploaded is None:
        return pd.DataFrame()

    name = getattr(uploaded, "name", "") or ""
    # try csv first
    try:
        df = pd.read_csv(uploaded)
        if df.shape[1] == 1:
            raise ValueError("Looks like 1-column; try tab.")
    except Exception:
        uploaded.seek(0)
        df = pd.read_csv(uploaded, sep="\t", header=None)
        # try to name columns if 4+ cols
        if df.shape[1] >= 4:
            df = df.iloc[:, :4]
            df.columns = ["Date", "State", "Game", "Results"]
        else:
            # fallback
            df.columns = [f"col_{i}" for i in range(df.shape[1])]

    # normalize column names
    colmap = {c.lower().strip(): c for c in df.columns}
    def pick(*cands):
        for c in cands:
            if c in colmap:
                return colmap[c]
        return None

    date_col = pick("date")
    state_col = pick("state")
    game_col = pick("game")
    results_col = pick("results", "result", "winning numbers", "winning_numbers", "winningnumbers")

    if date_col is None or state_col is None or game_col is None or results_col is None:
        # best-effort: if there are exactly 4 columns, assume those
        if df.shape[1] >= 4:
            df = df.iloc[:, :4].copy()
            df.columns = ["Date", "State", "Game", "Results"]
        else:
            raise ValueError("Could not detect Date/State/Game/Results columns.")
    else:
        df = df.rename(columns={
            date_col: "Date",
            state_col: "State",
            game_col: "Game",
            results_col: "Results",
        })

    # parse date
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df[df["Date"].notna()].copy()

    # parse results
    df["Pick4"] = df["Results"].map(extract_pick4_digits)
    df = df[df["Pick4"].notna()].copy()
    # compatibility aliases used in other modules
    df["Result"] = df["Pick4"]

    df["Structure"] = df["Pick4"].map(structure_of_4)
    df["Box"] = df["Pick4"].map(box_key)
    df["BoxKey4"] = df["Box"]
    df["Stream"] = df["State"].astype(str).str.strip() + " | " + df["Game"].astype(str).str.strip()
    return df



def try_read_picklist(uploaded) -> pd.DataFrame:
    """
    Accept a simple list of Pick4 numbers (previous-day file) in .txt or .csv form.
    Extracts 4-digit sequences anywhere in the file.
    Returns a dataframe with: Result, Pick4, Box, BoxKey4, Structure.
    """
    if uploaded is None:
        return pd.DataFrame()

    try:
        raw = uploaded.read()
    except Exception:
        raw = None

    # Reset pointer for possible re-reads by caller
    try:
        uploaded.seek(0)
    except Exception:
        pass

    if raw is None:
        return pd.DataFrame()

    if isinstance(raw, bytes):
        try:
            s = raw.decode("utf-8", errors="ignore")
        except Exception:
            s = str(raw)
    else:
        s = str(raw)

    # Common case: one number per line, but we accept any separators
    nums = re.findall(r"(?<!\d)(\d{4})(?!\d)", s)
    if not nums:
        # try to read as a one-column csv
        try:
            uploaded.seek(0)
        except Exception:
            pass
        try:
            df1 = pd.read_csv(uploaded, header=None)
            flat = []
            for v in df1.iloc[:, 0].astype(str).tolist():
                flat += re.findall(r"(?<!\d)(\d{4})(?!\d)", v)
            nums = flat
        except Exception:
            nums = []

    nums = [str(n).zfill(4) for n in nums if n is not None]
    # de-dupe while preserving order
    seen = set()
    out = []
    for n in nums:
        if n not in seen:
            seen.add(n)
            out.append(n)

    df = pd.DataFrame({"Result": out})
    if df.empty:
        return df
    df["Pick4"] = df["Result"]
    df["Structure"] = df["Pick4"].map(structure_of_4)
    df["Box"] = df["Pick4"].map(box_key)
    df["BoxKey4"] = df["Box"]
    return df


# -------------------------
# Stats + ranking
# -------------------------

@dataclass
class RankConfig:
    # History window used for rank stats (default 180, switchable to 365 in UI)
    window_days: int = 180

    # Bucket method:
    # - Top 'top_base' by BaseScore (HitsPerWeek)
    # - From base ranks due_from_rank..due_to_rank, take Top 'top_due' by DueIndex (DaysSinceLastHit)
    top_base: int = 12
    due_from_rank: int = 13
    due_to_rank: int = 60
    top_due: int = 8

    # Display / scoring knobs (kept as soft signals)
    max_master_rows: int = 120
    max_final_rows: int = 300
    include_24h_signals: bool = True
    pos_strength_weight: float = 0.25
    seed_core_key: str = "core"  # reserved for future compatibility

    # Back-compat aliases (older UI keys)
    @property
    def top12(self) -> int:
        return int(self.top_base)

    @property
    def due_ranks(self):
        return (int(self.due_from_rank), int(self.due_to_rank))

@property
def top_n(self) -> int:
    """Legacy alias: some older builds referenced RankConfig.top_n."""
    return int(self.top_base)

@property
def base_top_n(self) -> int:
    """Legacy alias for the Top bucket size."""
    return int(self.top_base)

@property
def due_top_n(self) -> int:
    """Legacy alias for the Due bucket size."""
    return int(self.top_due)


def within_last_days(df: pd.DataFrame, days: int) -> pd.DataFrame:
    if df.empty:
        return df
    max_date = df["Date"].max()
    cutoff = max_date - pd.Timedelta(days=days)
    return df[df["Date"] >= cutoff].copy()

def compute_core_hits(df: pd.DataFrame, core: str, structures: Iterable[str]) -> pd.DataFrame:
    """
    Return df subset containing only rows that are hits for the core, for the chosen structures.
    We match by Box membership, so order doesn't matter.
    """
    core = canonical_core_key(core)
    boxes = set()
    for s in structures:
        for mem in members_from_core(core, s):
            boxes.add(box_key(mem))
    return df[df["Box"].isin(boxes)].copy()

def stream_summary(df_all: pd.DataFrame, df_hits: pd.DataFrame, window_days: int) -> pd.DataFrame:
    """
    For each stream, compute:
    - draws_window, hits_window, hits_per_week_window
    - days_since_last_hit_window (based on df_hits within full history)
    """
    if df_all.empty:
        return pd.DataFrame(columns=[
            "Stream","DrawsWindow","HitsWindow","HitsPerWeek","LastHitDate","DaysSinceLastHit"
        ])

    dfw = within_last_days(df_all, window_days)
    max_date = df_all["Date"].max()

    draws = dfw.groupby("Stream").size().rename("DrawsWindow")
    hitsw = within_last_days(df_hits, window_days).groupby("Stream").size().rename("HitsWindow")

    # last hit date from full history (not just window) for "due"
    last_hit = df_hits.groupby("Stream")["Date"].max().rename("LastHitDate")

    out = _safe_pd_concat([draws, hitsw, last_hit], axis=1).fillna({"HitsWindow":0})
    out["HitsWindow"] = out["HitsWindow"].astype(int)
    out["DrawsWindow"] = out["DrawsWindow"].astype(int)

    weeks = max(window_days / 7.0, 1e-9)
    out["HitsPerWeek"] = out["HitsWindow"] / weeks

    out["DaysSinceLastHit"] = (max_date - out["LastHitDate"]).dt.days
    out.loc[out["LastHitDate"].isna(), "DaysSinceLastHit"] = 0

    out = out.reset_index().sort_values(["HitsPerWeek","HitsWindow"], ascending=False)
    out["RankPos"] = np.arange(1, len(out)+1)
    
    # Derived ranking columns (for bucket picks + backtest)
    # BaseScoreRank: same as RankPos (1 = strongest recent strength)
    if "RankPos" in out.columns and "BaseScoreRank" not in out.columns:
        out["BaseScoreRank"] = out["RankPos"]
    # BaseScore: a simple continuous strength proxy (used for sorting/UX)
    if "BaseScore" not in out.columns:
        out["BaseScore"] = out.get("HitsPerWeek", 0.0)
    # DueIndex: "how due" a stream is (days since last hit)
    if "DueIndex" not in out.columns:
        out["DueIndex"] = out.get("DaysSinceLastHit", 0)
    # DueIndexRank: 1 = most due (largest DueIndex)
    if "DueIndexRank" not in out.columns:
        try:
            _di = pd.to_numeric(out["DueIndex"], errors="coerce").fillna(-1)
            out["DueIndexRank"] = (-_di).rank(method="dense", ascending=True).astype(int)
        except Exception:
            out["DueIndexRank"] = out["BaseScoreRank"] if "BaseScoreRank" in out.columns else range(1, len(out) + 1)

    return out

def position_percentile_map(df_rankpos: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Create a percentile map over RankPos (1..78) using hit counts.

    Input must have at least:
      - RankPos (int)
      - HitsWindow (int/float) OR HitCount (int/float)

    Returns:
      (pos_df, meta)

    pos_df columns include:
      - RankPos
      - HitCount
      - HitCountPctile (0-100)
      - PctStrength (alias of HitCountPctile; back-compat)
      - HitSharePct (percent of total hits at this RankPos)
      - CumuHitSharePct (cumulative share across RankPos ascending)

    Notes:
      - If multiple rows share the same RankPos (e.g., aggregating many cores),
        they are summed first.
    """
    empty_cols = [
        "RankPos", "HitCount", "HitCountPctile", "PctStrength", "HitSharePct", "CumuHitSharePct",
        "HitShare", "CumHitShare",
    ]
    if df_rankpos is None or df_rankpos.empty:
        return pd.DataFrame(columns=empty_cols), {"total_hits": 0.0, "rows": 0}

    pos = df_rankpos.copy()

    # Normalize column name
    if "HitCount" not in pos.columns and "HitsWindow" in pos.columns:
        pos = pos.rename(columns={"HitsWindow": "HitCount"})
    if "HitCount" not in pos.columns:
        # Best effort: try common alternatives
        for alt in ["Hits", "Count", "Hit_Count"]:
            if alt in pos.columns:
                pos = pos.rename(columns={alt: "HitCount"})
                break

    if "RankPos" not in pos.columns or "HitCount" not in pos.columns:
        return pd.DataFrame(columns=empty_cols), {"total_hits": 0.0, "rows": 0}

    # Aggregate by RankPos (important for ALL-CORES maps)
    pos = pos.groupby("RankPos", as_index=False)["HitCount"].sum()

    # Sort by RankPos for consistent cumulative share
    pos["RankPos"] = pos["RankPos"].astype(int)
    pos = pos.sort_values("RankPos").reset_index(drop=True)

    # Percentile rank by hit count (ties handled by average rank)
    pos["HitCountPctile"] = pos["HitCount"].rank(pct=True) * 100.0

    total_hits = float(pos["HitCount"].sum())
    denom = total_hits if total_hits != 0.0 else 1.0
    pos["HitSharePct"] = (pos["HitCount"] / denom) * 100.0
    pos["CumuHitSharePct"] = pos["HitSharePct"].cumsum()

    # Back-compat aliases used in older UI text + newer tie-break key
    pos["HitShare"] = pos["HitSharePct"]
    pos["CumHitShare"] = pos["CumuHitSharePct"]
    pos["PctStrength"] = pos["HitCountPctile"]

    meta = {
        "total_hits": total_hits,
        "rows": int(pos.shape[0]),
    }
    return pos, meta


# -------------------------
# Seed Traits (positive/negative) + Cadence (v51)
# -------------------------
def _read_local_or_uploaded_csv(uploaded_file, local_path: str) -> pd.DataFrame:
    """Read CSV from uploaded file-like or a local repo path. Returns empty df on failure."""
    try:
        if uploaded_file is not None:
            try:
                uploaded_file.seek(0)
            except Exception:
                pass
            return pd.read_csv(uploaded_file)
    except Exception:
        pass
    try:
        if local_path:
            _lp = Path(local_path)
            if not _lp.is_absolute():
                _lp = Path(__file__).resolve().parent / _lp
            if _lp.exists():
                return pd.read_csv(_lp)
    except Exception:
        pass
    return pd.DataFrame()

def _read_local_or_uploaded_text(uploaded_file, local_path: str) -> str:
    try:
        if uploaded_file is not None:
            try:
                uploaded_file.seek(0)
            except Exception:
                pass
            return uploaded_file.read().decode('utf-8', errors='ignore') if hasattr(uploaded_file, 'read') else str(uploaded_file)
    except Exception:
        pass
    try:
        if local_path and os.path.exists(local_path):
            return open(local_path, 'r', encoding='utf-8', errors='ignore').read()
    except Exception:
        pass
    return ""

def _build_traits_lookup(df: pd.DataFrame) -> Dict[int, Dict[str, Dict[str, float]]]:
    """lookup[core_family][trait][value] -> lift"""
    lookup: Dict[int, Dict[str, Dict[str, float]]] = {}
    if df is None or df.empty:
        return lookup
    # normalize columns
    cols = {c.lower(): c for c in df.columns}
    core_col = cols.get("core_family", None)
    trait_col = cols.get("trait", None)
    val_col = cols.get("value", None)
    lift_col = cols.get("lift", None)
    if not (core_col and trait_col and val_col and lift_col):
        return lookup
    for _, r in df.iterrows():
        try:
            core = int(r[core_col])
        except Exception:
            continue
        trait = str(r[trait_col]).strip()
        val = str(r[val_col]).strip()
        try:
            lift = float(r[lift_col])
        except Exception:
            lift = 1.0
        if not trait:
            continue
        lookup.setdefault(core, {}).setdefault(trait, {})[val] = lift
    return lookup

def _seed_sum_range4_labels(seed_sum: int) -> List[str]:
    labels = []
    for start in (seed_sum, seed_sum-1, seed_sum-2, seed_sum-3):
        if start < 0:
            continue
        end = start + 3
        if end > 36:  # pick4 max sum
            continue
        labels.append(f"{start}-{end}")
    return labels

def _seed_spread_bucket(spread: int) -> str:
    if spread <= 2:
        return "<=2"
    if spread >= 6:
        return ">=6"
    return "3-5"

def _count_high_digits(digits: List[int]) -> int:
    # Pick-4 convention in this app: high digits are 5–9
    return sum(1 for d in digits if d >= 5)

def _seed_contains_core_pair(seed: str, core: str) -> str:
    seed = str(seed)
    core = canonical_core_key(core)
    core_digits = list(core)
    pairs = set()
    for i in range(len(core_digits)):
        for j in range(len(core_digits)):
            if i == j:
                continue
            pairs.add(core_digits[i] + core_digits[j])
    adj = [seed[i:i+2] for i in range(len(seed)-1)]
    return "yes" if any(a in pairs for a in adj) else "no"

def _feature_values_for_seed(seed: str, core: str, last5_union_digits: Optional[set] = None) -> Dict[str, List[str]]:
    """Calculate every base and extra core-trait feature used by the four root CSV libraries."""
    seed = re.sub(r"\D", "", str(seed or "")).zfill(4)[-4:]
    core = canonical_core_key(core)
    digits = [int(ch) for ch in seed]
    core_set = set(core)
    seed_sum = sum(digits)
    even_ct = sum(1 for d in digits if d % 2 == 0)
    high_ct = sum(1 for d in digits if d >= 5)
    spread = max(digits) - min(digits)
    overlap = len(set(seed) & core_set)
    overlap_vals = ["3", ">=2"] if overlap == 3 else ([">=2"] if overlap == 2 else [str(overlap)])
    grid_overlap = len(set(last5_union_digits or set()) & core_set)
    grid_vals = ["3", ">=2"] if grid_overlap == 3 else ([">=2"] if grid_overlap == 2 else [str(grid_overlap)])
    # Extra-library definitions.
    sum_band = "0-9" if seed_sum <= 9 else ("10-17" if seed_sum <= 17 else ("18-26" if seed_sum <= 26 else "27-36"))
    band = lambda d: "0-3" if d <= 3 else ("4-6" if d <= 6 else "7-9")
    spread_band = "0-3" if spread <= 3 else ("4-6" if spread <= 6 else "7-9")
    from collections import Counter
    cnt = Counter(digits)
    repeats = sorted(str(d) for d,n in cnt.items() if n >= 2)
    repeat_vals = repeats if repeats else ["none"]
    primes = {2,3,5,7}
    prime_ct = sum(d in primes for d in digits)
    unique_ct = len(set(digits))
    # VTrac convention: 0/5=5, 1/6=1, 2/7=2, 3/8=3, 4/9=4.
    vmap = {0:5,5:5,1:1,6:1,2:2,7:2,3:3,8:3,4:4,9:4}
    vtrac_unique = len({vmap[d] for d in digits})
    consecutive_pairs = sum(1 for i in range(3) if abs(digits[i]-digits[i+1]) == 1)
    # Mirror pair means a positional digit pair sums to 9 (0-9,1-8,...).
    mirror_any = "yes" if any(digits[i] + digits[j] == 9 for i in range(4) for j in range(i+1,4)) else "no"
    feats: Dict[str, List[str]] = {
        "seed_structure": [structure_of_4(seed)],
        "seed_even_count": [str(even_ct)],
        "seed_high_count": [str(high_ct)],
        "seed_spread": [_seed_spread_bucket(spread)],
        "seed_sum_mod2": ["even" if seed_sum % 2 == 0 else "odd", str(seed_sum % 2)],
        "seed_sum_mod3": [str(seed_sum % 3)],
        "seed_sum_range4_best": _seed_sum_range4_labels(seed_sum),
        "seed_sum_range4_worst": _seed_sum_range4_labels(seed_sum),
        "overlap_unique": overlap_vals,
        "seed_contains_core_pair": [_seed_contains_core_pair(seed, core)],
        "seed_first_in_core": ["yes" if seed[0] in core_set else "no"],
        "seed_last_in_core": ["yes" if seed[-1] in core_set else "no"],
        "grid_last5_core_digits": grid_vals,
        "seed_sum_band": [sum_band],
        "seed_sum_lastdigit": [str(seed_sum % 10)],
        "seed_repeat_digit": repeat_vals,
        "seed_prime_count": [str(prime_ct)],
        "seed_unique_count": [str(unique_ct)],
        "seed_vtrac_unique": [str(vtrac_unique)],
        "seed_lead_band": [band(digits[0])],
        "seed_trail_band": [band(digits[-1])],
        "seed_spread_band": [spread_band],
        "seed_consecutive_pairs": [str(consecutive_pairs)],
        "seed_mirror_pairs_any": [mirror_any],
    }
    return feats

def compute_seed_traits_score(
    core: str,
    seed: Optional[str],
    stream: Optional[str],
    *,
    pos_lookup: Dict[int, Dict[str, Dict[str, float]]],
    neg_lookup: Dict[int, Dict[str, Dict[str, float]]],
    last5_union_digits_by_stream: Optional[Dict[str, set]] = None,
    cap: float = 2.0,
) -> Tuple[float, List[Tuple[str, str, float, str]]]:
    """Return (net_score, matches). net_score is sum((lift-1) pos) - sum((lift-1) neg), capped."""
    if seed is None:
        return 0.0, []
    core = canonical_core_key(core)
    try:
        core_num = int(core)
    except Exception:
        core_num = int(core.lstrip('0') or 0)
    last5_union = None
    if stream and last5_union_digits_by_stream and stream in last5_union_digits_by_stream:
        last5_union = last5_union_digits_by_stream.get(stream)
    feats = _feature_values_for_seed(str(seed), core, last5_union_digits=last5_union)

    matches: List[Tuple[str, str, float, str]] = []
    score = 0.0
    for trait, vals in feats.items():
        for val in vals:
            # positive
            liftp = pos_lookup.get(core_num, {}).get(trait, {}).get(val)
            if liftp is not None:
                delta = float(liftp) - 1.0
                score += delta
                matches.append((trait, val, float(liftp), "+"))
            # negative
            liftn = neg_lookup.get(core_num, {}).get(trait, {}).get(val)
            if liftn is not None:
                # Negative libraries contain lifts below 1.0.  Their penalty is
                # the shortfall from neutral, not (lift - 1), which is negative.
                penalty = max(0.0, 1.0 - float(liftn))
                score -= penalty
                matches.append((trait, val, float(liftn), "-"))
    # Cap for safety
    score = max(-cap, min(cap, score))
    return float(score), matches

def compute_cadence_score(days_since_last_hit: float, mean_gap_days: float) -> float:
    """Soft cadence score in [0,1]. 0 = not due vs cadence, 1 = very due."""
    try:
        d = float(days_since_last_hit)
    except Exception:
        return 0.0
    try:
        g = float(mean_gap_days)
    except Exception:
        g = 0.0
    if g <= 0:
        return 0.0
    ratio = d / g
    # Map ratio: 1.0 -> 0, 3.0 -> 1 (cap)
    val = (ratio - 1.0) / 2.0
    if val < 0:
        return 0.0
    if val > 1:
        return 1.0
    return float(val)


def get_position_percentiles_cached(core: str, window_days: int, stream_stats: pd.DataFrame) -> pd.DataFrame:
    """Cache position percentile maps per core/window so UI tweaks don't constantly recompute.
    Cache is automatically cleared when input data changes or when the user clicks 'Recompute percentile maps now'.
    """
    cache: Dict[str, pd.DataFrame] = st.session_state.get("pos_map_cache", {})
    data_hash = st.session_state.get("data_hash_all", "")
    key = f"{core}|{window_days}|{data_hash}"

    if key in cache:
        return cache[key]

    pos_map, _ = position_percentile_map(stream_stats)
    cache[key] = pos_map
    st.session_state["pos_map_cache"] = cache

    if not st.session_state.get("recompute_token"):
        st.session_state["recompute_token"] = datetime.datetime.now().isoformat(timespec="seconds")

    return pos_map

def bucket_recommendations(
    stream_stats: pd.DataFrame,
    cfg: Optional[RankConfig] = None,
    *,
    top_n: Optional[int] = None,
    due_n: Optional[int] = None,
) -> Dict[str, pd.DataFrame]:
    """Build Northern Star buckets from a stream_stats table.

    Returns a dict with **multiple key aliases** for compatibility:
      - Top12BaseScore / Top12  -> top base-score bucket
      - Due8                   -> due bucket
      - Combined               -> merged bucket
      - base_top / due_top / combined -> lists of stream labels (for meta)

    `top_n` / `due_n` are accepted as legacy keyword overrides.
    """
    if cfg is None:
        cfg = RankConfig()

    base_n = int(top_n) if top_n is not None else int(getattr(cfg, "top_base", 12))
    due_take = int(due_n) if due_n is not None else int(getattr(cfg, "top_due", 8))

    # Normalize expected columns so this helper works with:
    #  - stream_summary() output (RankPos, HitsPerWeek, DaysSinceLastHit, ...)
    #  - legacy bucket tables (BaseRank/DueRank)
    #  - future member-level tables (Pick/Member columns)
    df = stream_stats.copy()

    # Base rank
    if "BaseScoreRank" not in df.columns:
        if "BaseRank" in df.columns:
            df["BaseScoreRank"] = df["BaseRank"]
        elif "RankPos" in df.columns:
            df["BaseScoreRank"] = df["RankPos"]
        elif "HitsPerWeek" in df.columns:
            _hpw = pd.to_numeric(df["HitsPerWeek"], errors="coerce").fillna(0.0)
            df["BaseScoreRank"] = (-_hpw).rank(method="dense", ascending=True).astype(int)
        else:
            df["BaseScoreRank"] = range(1, len(df) + 1)

    # RankPos (for display ordering)
    if "RankPos" not in df.columns:
        df["RankPos"] = df["BaseScoreRank"]

    # Due index
    if "DueIndex" not in df.columns:
        if "DaysSinceLastHit" in df.columns:
            df["DueIndex"] = pd.to_numeric(df["DaysSinceLastHit"], errors="coerce")
        else:
            df["DueIndex"] = 0

    # Due rank
    if "DueIndexRank" not in df.columns:
        if "DueRank" in df.columns:
            df["DueIndexRank"] = df["DueRank"]
        else:
            _di = pd.to_numeric(df["DueIndex"], errors="coerce").fillna(-1)
            # 1 = most due (largest DueIndex)
            df["DueIndexRank"] = (-_di).rank(method="dense", ascending=True).astype(int)

    # Ensure Stream exists for downstream logic
    if "Stream" not in df.columns and "stream" in df.columns:
        df["Stream"] = df["stream"]
    due_lo = int(getattr(cfg, "due_from_rank", 13))
    due_hi = int(getattr(cfg, "due_to_rank", 60))

    # Defensive: tolerate missing columns; caller should validate upstream.
    if stream_stats is None or len(df) == 0:
        empty = pd.DataFrame()
        return {
            "Top12BaseScore": empty,
            "Top12": empty,
            "Due8": empty,
            "Combined": empty,
            "base_top": [],
            "due_top": [],
            "combined": [],
        }

    base_df = df.sort_values("BaseScoreRank", ascending=True).head(base_n)
    due_pool = df[
        (df["BaseScoreRank"] >= due_lo) & (df["BaseScoreRank"] <= due_hi)
    ].sort_values("DueIndexRank", ascending=True)
    due_df = due_pool.head(due_take)

    combined_df = _safe_pd_concat([base_df, due_df], ignore_index=True).drop_duplicates(subset=["Stream"], keep="first")
    combined_df = combined_df.sort_values("RankPos", ascending=True)

    base_streams = base_df["Stream"].tolist() if "Stream" in base_df.columns else []
    due_streams = due_df["Stream"].tolist() if "Stream" in due_df.columns else []
    combined_streams = combined_df["Stream"].tolist() if "Stream" in combined_df.columns else []

    return {
        "Top12BaseScore": base_df,
        "Top12": base_df,
        "Due8": due_df,
        "Combined": combined_df,
        "base_top": base_streams,
        "due_top": due_streams,
        "combined": combined_streams,
    }

def build_northern_star_bucket_meta(
    stream_stats: pd.DataFrame,
    cfg: RankConfig,
    *,
    seed_core_key: str = "",
    include_24h: bool = False,
    df_24: pd.DataFrame | None = None,
    core: str = "",
) -> List[Dict[str, Any]]:
    """Compatibility helper used by some older app revisions.

    Returns a list of per-stream bucket metadata rows (one dict per stream).
    This intentionally mirrors the data shape consumed by the Northern Lights master playlist.
    """
    if stream_stats is None or not isinstance(stream_stats, pd.DataFrame) or stream_stats.empty:
        return []

    # Pre-compute which streams are in which bucket for this core.
    rec = bucket_recommendations(stream_stats, cfg)
    base_streams = set(rec.get("base_top", []))
    due_streams = set(rec.get("due_top", []))

    rows: List[Dict[str, Any]] = []
    for stream in stream_stats["Stream"].tolist():
        try:
            rows.append(
                build_northern_star_buckets(
                    stats_df=stream_stats,
                    stream=stream,
                    top_n=cfg.top_base,
                    due_ranks=(cfg.due_from_rank, cfg.due_to_rank),
                    seed_core_key=seed_core_key,
                    include_24h=include_24h,
                    df_24=df_24,
                    core=core,
                    base_streams=base_streams,
                    due_streams=due_streams,
                )
            )
        except TypeError:
            # Oldest signature (no precomputed sets)
            rows.append(
                build_northern_star_buckets(
                    stats_df=stream_stats,
                    stream=stream,
                    top_n=cfg.top_base,
                    due_ranks=(cfg.due_from_rank, cfg.due_to_rank),
                    seed_core_key=seed_core_key,
                    include_24h=include_24h,
                    df_24=df_24,
                    core=core,
                )
            )
    return rows

def top_dense_positions(pos_map, top_n: int = 10, top_k_positions: int | None = None):
    """Return the top-N rank positions (1..76) that collectively hold the most winners."""
    if pos_map is None or pos_map.empty:
        return []
    tmp = pos_map.copy()
    # Defensive: sometimes RankPos can come back as strings; coerce to numeric
    tmp["RankPos_num"] = pd.to_numeric(tmp["RankPos"], errors="coerce")
    tmp = tmp.dropna(subset=["RankPos_num"])
    if tmp.empty:
        return []
    tmp["RankPos_num"] = tmp["RankPos_num"].astype(int)
    counts = tmp.groupby("RankPos_num")["HitCount"].sum().sort_values(ascending=False).head(int(top_n))
    return [int(x) for x in counts.index.tolist()]

def engine_cluster_positions(
    df_24h_core_hits: pd.DataFrame,
    base_stats: pd.DataFrame,
    top_n: int = 10,
    use_rank_col: str = "RankPos",
) -> list[int]:
    """Return the *clustered* top-N rank positions for a 24h core-hit sample.

    Robust against empty inputs, NaNs, and callers accidentally passing Series/dicts.
    Always returns a Python list (possibly empty).
    """
    if df_24h_core_hits is None or len(df_24h_core_hits) == 0:
        return []

    if base_stats is None:
        return []

    # Normalize base_stats to a DataFrame with a numeric rank column.
    if not isinstance(base_stats, pd.DataFrame):
        try:
            base_stats = pd.DataFrame(base_stats)
        except Exception:
            return []

    if use_rank_col not in base_stats.columns:
        return []

    rank_input = base_stats[use_rank_col]
    if isinstance(rank_input, pd.DataFrame):
        # if a list-like column selector was passed, take first column
        rank_input = rank_input.iloc[:, 0]
    rank_series = pd.to_numeric(rank_input, errors="coerce")
    rank_map = pd.DataFrame({"RankPos": rank_series}).dropna().sort_values("RankPos")
    if rank_map.empty:
        return []

    # Candidate rank positions actually observed in the 24h hit set
    try:
        hit_ranks = pd.to_numeric(df_24h_core_hits.get("RankPos"), errors="coerce").dropna().astype(int).tolist()
    except Exception:
        hit_ranks = []

    if not hit_ranks:
        return []

    # Keep only ranks that exist in base_stats map
    rank_set = set(rank_map["RankPos"].astype(int).tolist())
    hit_ranks = [int(r) for r in hit_ranks if int(r) in rank_set]
    if not hit_ranks:
        return []

    # Find a "dense cluster" around the most common local neighborhood.
    hit_ranks_sorted = sorted(hit_ranks)

    best_window = None
    best_score = -1
    span = 12  # neighborhood width

    for anchor in hit_ranks_sorted:
        lo = anchor
        hi = anchor + span
        members = [r for r in hit_ranks_sorted if lo <= r <= hi]
        score = len(members)
        if score > best_score:
            best_score = score
            best_window = (lo, hi)

    if best_window is None:
        return []

    lo, hi = best_window
    clustered = [r for r in rank_map["RankPos"].astype(int).tolist() if lo <= r <= hi]
    # Return up to top_n, but keep as list[int]
    return clustered[: max(1, int(top_n))]


def evaluate_rare_engine(
    df_all: pd.DataFrame,
    core: str,
    df_24h: pd.DataFrame | None = None,
    enable_r1: bool = True,
    enable_r2: bool = True,
    enable_r3: bool = True,
    enable_r4: bool = True,
    window_days_recent: int = 180,
) -> Tuple[pd.DataFrame, Dict[str, object]]:
    if df_24h is None:
        df_24h = pd.DataFrame()
    """
    Rare Engine checks (AAAB + AABB together):
      R1: stream is in top 20% for combined AAAB+AABB baseline rate (full history).
      R2: stream is in top 20% for combined AAAB+AABB rate in last 180 days.
      R3: the last 24h map contains ≥3 AAAB/AABB hits across ≥3 distinct streams (global condition).
      R4: last 24h AAAB/AABB hits cluster into Top-10 RankPos, and stream RankPos is in that set.
    Trigger: at least 3 of enabled checks True.

    Returns:
      - per-stream table with booleans and trigger
      - summary dict with thresholds and cluster sets
    """
    if df_all.empty:
        return pd.DataFrame(), {"error":"No history loaded."}

    core = canonical_core_key(core)
    df_hits_all = compute_core_hits(df_all, core, structures=["AAAB","AABB"])

    # Baseline stream stats
    base_stats = stream_summary(df_all, df_hits_all, window_days=min(365*5, int((df_all["Date"].max()-df_all["Date"].min()).days) or 365))
    # But we want baseline based on full history span; use hits per week in that span:
    span_days = max(int((df_all["Date"].max()-df_all["Date"].min()).days), 1)
    base_stats["HitsPerWeek_full"] = base_stats["HitsWindow"] / (span_days/7.0)
    base_stats = base_stats.sort_values(["HitsPerWeek_full","HitsWindow"], ascending=False).reset_index(drop=True)
    base_stats["RankPos_full"] = np.arange(1, len(base_stats)+1)

    # Recent stats (180d)
    recent_stats = stream_summary(df_all, df_hits_all, window_days=window_days_recent)

    # Thresholds
    def pct_threshold(series: pd.Series, pct: float) -> float:
        vals = series.dropna().values
        if len(vals)==0:
            return float("nan")
        return float(np.quantile(vals, pct))

    # R1 top 20% based on HitsPerWeek_full
    thr_r1 = pct_threshold(base_stats["HitsPerWeek_full"], 0.80)

    # R2 top 20% based on recent HitsPerWeek
    thr_r2 = pct_threshold(recent_stats["HitsPerWeek"], 0.80)

    # R3 global condition from 24h file: ≥3 hits across ≥3 distinct streams
    df_24h_core_hits = pd.DataFrame()
    top10_cluster = []
    r3_global = False
    if df_24h is not None and not df_24h.empty:
        df_24h_core_hits = compute_core_hits(df_24h, core, structures=["AAAB","AABB"])
        n_hits_24h = int(len(df_24h_core_hits))
        n_streams_24h = int(df_24h_core_hits["Stream"].nunique()) if n_hits_24h else 0
        r3_global = (n_hits_24h >= 3) and (n_streams_24h >= 3)
        # R4 cluster based on 24h file (Top-10 RankPos positions by 24h frequency)
        top10_cluster = engine_cluster_positions(
            df_24h_core_hits,
            base_stats.rename(columns={"RankPos_full":"RankPos"}).assign(RankPos=lambda d: d["RankPos"]),
            top_n=10,
        )

    # Merge per stream
    out = pd.DataFrame({"Stream": base_stats["Stream"]})
    out = out.merge(base_stats[["Stream","HitsPerWeek_full","RankPos_full"]], on="Stream", how="left")
    out = out.merge(recent_stats[["Stream","HitsPerWeek","DaysSinceLastHit","RankPos"]].rename(columns={"RankPos":"RankPos_recent"}), on="Stream", how="left")

    out["R1_top20_baseline"] = out["HitsPerWeek_full"] >= thr_r1 if enable_r1 else False
    out["R2_top20_recent"] = out["HitsPerWeek"] >= thr_r2 if enable_r2 else False
    out["R3_24h_has_3plus_across_3streams"] = r3_global if enable_r3 else False
    out["R4_24h_cluster_top10pos"] = out["RankPos_full"].isin(top10_cluster) if enable_r4 else False

    enabled_cols = [c for c, en in [
        ("R1_top20_baseline", enable_r1),
        ("R2_top20_recent", enable_r2),
        ("R3_24h_has_3plus_across_3streams", enable_r3),
        ("R4_24h_cluster_top10pos", enable_r4),
    ] if en]

    out["ChecksTrue"] = out[enabled_cols].sum(axis=1) if enabled_cols else 0
    out["RareEngine_TRIG"] = out["ChecksTrue"] >= 3 if enabled_cols else False

    out = out.sort_values(["RareEngine_TRIG","ChecksTrue","HitsPerWeek_full"], ascending=[False, False, False]).reset_index(drop=True)

    summary = {
        "thr_r1": thr_r1,
        "thr_r2": thr_r2,
        "r3_global": r3_global,
        "top10_cluster_positions": top10_cluster,
        "n_24h_core_hits": int(len(df_24h_core_hits)) if df_24h is not None else 0,
        "n_24h_core_streams": int(df_24h_core_hits["Stream"].nunique()) if df_24h is not None and not df_24h_core_hits.empty else 0,
        "span_days_full": span_days,
        "enabled_checks": enabled_cols,
    }
    return out, summary

def evaluate_ultra_rare_engine(
    df_all: pd.DataFrame,
    core: str,
    df_24h: pd.DataFrame | None = None,
    enable_q1: bool = True,
    enable_q2: bool = True,
    enable_q3: bool = True,
    enable_q4: bool = True,
    # Some UI call-sites pass this (mirroring the rare engine). We accept it for
    # compatibility. The ultra-rare engine is primarily computed on the full
    # history; when provided, we use it only for optional recent-window fields.
    window_days_recent: int | None = None,
    # Forward-compat: ignore any extra kwargs passed from older/newer UIs.
    **_ignored_kwargs,
) -> Tuple[pd.DataFrame, Dict[str, object]]:
    """
    Ultra-Rare Engine checks (AAAA quads for the core's digits):
      Q1: stream in top 10% for quad baseline rate (full history)
      Q2: days since last quad >= 90th percentile across streams
      Q3: last 24h has at least 1 quad anywhere (for any digit in core)
      Q4: 24h quad hits (for core) cluster into Top-5 RankPos; stream position is in that set
    Trigger: at least 2 of enabled checks True.
    """
    if df_24h is None:
        df_24h = pd.DataFrame()

    if df_all.empty:
        return pd.DataFrame(), {"error":"No history loaded."}

    core = canonical_core_key(core)
    df_hits_all = compute_core_hits(df_all, core, structures=["AAAA"])

    span_days = max(int((df_all["Date"].max()-df_all["Date"].min()).days), 1)
    base_stats = stream_summary(df_all, df_hits_all, window_days=min(365*5, span_days))

    # Optional recent window (used by some UI displays). This is non-breaking: if
    # absent, downstream behavior matches historical-only mode.
    base_stats_recent = None
    if window_days_recent is not None and not df_all.empty and "Date" in df_all.columns:
        try:
            cutoff = df_all["Date"].max() - pd.Timedelta(days=int(window_days_recent))
            df_recent = df_all[df_all["Date"] >= cutoff].copy()
            df_hits_recent = compute_core_hits(df_recent, core, structures=["AAAA"])
            base_stats_recent = stream_summary(df_recent, df_hits_recent, window_days=int(window_days_recent))
        except Exception:
            base_stats_recent = None
    base_stats["HitsPerWeek_full"] = base_stats["HitsWindow"] / (span_days/7.0)
    base_stats = base_stats.sort_values(["HitsPerWeek_full","HitsWindow"], ascending=False).reset_index(drop=True)
    base_stats["RankPos_full"] = np.arange(1, len(base_stats)+1)

    # thresholds
    def pct_threshold(series: pd.Series, pct: float) -> float:
        vals = series.dropna().values
        if len(vals)==0:
            return float("nan")
        return float(np.quantile(vals, pct))

    thr_q1 = pct_threshold(base_stats["HitsPerWeek_full"], 0.90)
    thr_q2 = pct_threshold(base_stats["DaysSinceLastHit"], 0.90)  # DaysSinceLastHit computed from last quad date

    # Q3 global 24h quad exists for any core digit
    q3_global = False
    df_24h_core_hits = pd.DataFrame()
    top5_cluster = []
    if df_24h is not None and not df_24h.empty:
        # any quad in 24h that uses one of core digits
        core_digits = set(core)
        df_24h_quads = df_24h[df_24h["Structure"]=="AAAA"].copy()
        df_24h_quads["quad_digit"] = df_24h_quads["Pick4"].str[0]
        q3_global = df_24h_quads["quad_digit"].isin(core_digits).any()
        df_24h_core_hits = compute_core_hits(df_24h, core, structures=["AAAA"])
        top5_cluster = engine_cluster_positions(df_24h_core_hits, base_stats.rename(columns={"RankPos_full":"RankPos"}).assign(RankPos=lambda d: d["RankPos"]), top_n=5)

    out = pd.DataFrame({"Stream": base_stats["Stream"]})
    out = out.merge(base_stats[["Stream","HitsPerWeek_full","DaysSinceLastHit","RankPos_full"]], on="Stream", how="left")

    out["Q1_top10_baseline"] = out["HitsPerWeek_full"] >= thr_q1 if enable_q1 else False
    out["Q2_due_p90"] = out["DaysSinceLastHit"] >= thr_q2 if enable_q2 else False
    out["Q3_24h_quad_exists"] = q3_global if enable_q3 else False
    out["Q4_24h_cluster_top5pos"] = out["RankPos_full"].isin(top5_cluster) if enable_q4 else False

    enabled_cols = [c for c, en in [
        ("Q1_top10_baseline", enable_q1),
        ("Q2_due_p90", enable_q2),
        ("Q3_24h_quad_exists", enable_q3),
        ("Q4_24h_cluster_top5pos", enable_q4),
    ] if en]

    out["ChecksTrue"] = out[enabled_cols].sum(axis=1) if enabled_cols else 0
    out["UltraRare_TRIG"] = out["ChecksTrue"] >= 2 if enabled_cols else False

    out = out.sort_values(["UltraRare_TRIG","ChecksTrue","HitsPerWeek_full"], ascending=[False, False, False]).reset_index(drop=True)

    summary = {
        "thr_q1": thr_q1,
        "thr_q2": thr_q2,
        "q3_global": q3_global,
        "top5_cluster_positions": top5_cluster,
        "n_24h_core_quad_hits": int(len(df_24h_core_hits)) if df_24h is not None else 0,
        "enabled_checks": enabled_cols,
        "span_days_full": span_days,
    }
    return out, summary


# -------------------------
# UI
# -------------------------


def render_backtest(
    df_all: pd.DataFrame,
    cfg: "RankConfig | None" = None,
    cores_for_cache: "list[str] | None" = None,
    df_24h: "pd.DataFrame | None" = None,
    # backwards-compatible aliases (older call sites)
    cores: "list[str] | None" = None,
    window_days: "int | None" = None,
):
    """Backtest / diagnostics.

    **Walk-forward mode (no-cheat):**
    For each test_date, builds rankings/traps using ONLY rows with Date < test_date, then scores the
    winner(s) that occurred on test_date.

    **Playlist diagnostic mode:**
    Uses the current Northern Lights playlist in-session (helpful for quick validation, but can
    include future leakage if the playlist was built using the full dataset).
    """

    # Normalize args (avoid brittle keyword mismatches across revisions)
    if cfg is None:
        cfg = RankConfig(window_days=int(window_days or 180))
    if cores_for_cache is None:
        cores_for_cache = list(cores or [])

    st.subheader("Backtest (optional)")
    st.caption("Optional diagnostics. Walk-forward mode avoids future leakage by training only on Date < test_date.")

    if df_all is None or getattr(df_all, "empty", True):
        st.warning("Upload an all-states history file to use Backtest.")
        return

    # Ensure Date dtype
    if "Date" in df_all.columns:
        try:
            if not pd.api.types.is_datetime64_any_dtype(df_all["Date"]):
                df_all = df_all.copy()
                df_all["Date"] = pd.to_datetime(df_all["Date"], errors="coerce")
        except Exception:
            pass

    mode = st.radio(
        "Backtest mode",
        ["Walk-forward (no cheating)", "Playlist diagnostic (uses current playlist)"],
        horizontal=True,
        key="bt_mode_v51",
    )

    # Clarify which "view" drives stream selection to prevent confusion.
    if mode.startswith("Walk-forward"):
        st.info("Stream-bucket source: WALK-FORWARD per-core recompute (train on Date < test_date; uses your selected 180/365 window).")
    else:
        st.info("Stream-bucket source: CURRENT Northern Lights playlist on-screen (playlist diagnostic; not walk-forward).")


    if mode.startswith("Walk-forward"):
        # v51.39: walk-forward is production-locked to the exact Working 8.
        _render_backtest_walk_forward(df_all=df_all, cfg=cfg, cores_for_cache=list(WORKING8_CORE_SET))
        return

    # Playlist diagnostic (legacy / quick)
    """Optional diagnostics backtest.

    This evaluates how often the *current* Northern Lights master playlist (streams+cores)
    would have caught a matching family member in those streams over a selected historical range.
    It does **not** change any scoring or ranking output.
    """

    if df_all is None or df_all.empty:
        st.warning("Upload an all-states history file first.")
        return

    # Ensure required columns exist
    if "Date" not in df_all.columns or "Stream" not in df_all.columns:
        st.error("History file is missing required columns (Date, Stream).")
        return

    # Date bounds
    try:
        dmin_ts = pd.to_datetime(df_all["Date"]).min()
        dmax_ts = pd.to_datetime(df_all["Date"]).max()
    except Exception:
        st.error("Could not read Date values from the history file.")
        return

    if pd.isna(dmin_ts) or pd.isna(dmax_ts):
        st.error("History file has no valid dates.")
        return

    dmin = dmin_ts.date()
    dmax = dmax_ts.date()
    default_start = max(dmin, (dmax_ts - pd.Timedelta(days=min(180, max(7, int((dmax_ts - dmin_ts).days * 0.25))))).date())
    date_range = st.date_input(
        "Backtest date range (inclusive)",
        value=(default_start, dmax),
        min_value=dmin,
        max_value=dmax,
        help="This checks historical draws in the selected range against your current playlist picks."
    )
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date, end_date = date_range, date_range

    if start_date > end_date:
        start_date, end_date = end_date, start_date

    # Structures used to define a 'hit' for a core in a stream
    structure_mode = st.selectbox(
        "Match mode (what counts as a hit for a core)",
        options=[
            "AABC only (single-member focus)",
            "Family mode (AABC + ABBC + ABCC)",
            "Rare engine mode (AAAB + AABB)",
            "Ultra-rare mode (AAAA only)",
        ],
        index=1,
        help="This affects only the backtest match check (not your ranking)."
    )
    if structure_mode.startswith("AABC only"):
        structures = ["AABC"]
    elif structure_mode.startswith("Family mode"):
        structures = ["AABC", "ABBC", "ABCC"]
    elif structure_mode.startswith("Rare engine mode"):
        structures = ["AAAB", "AABB"]
    else:
        structures = ["AAAA"]

    # Playlist source
    nl_df = st.session_state.get("nl_df_current")
    if not isinstance(nl_df, pd.DataFrame) or nl_df.empty or "Core" not in nl_df.columns or "Stream" not in nl_df.columns:
        with st.expander("Master playlist not found in session — build it now", expanded=True):
            st.info("Your master playlist isn't cached in the current session. Click below to build it (this can take a bit).")
            if st.button("Build master playlist for backtest", type="primary"):
                nl_df = None  # force rebuild below

    def _build_master_playlist_for_backtest() -> pd.DataFrame:
        cores = [canonical_core_key(c) for c in (cores_for_cache or [])]
        cores = [c for c in cores if c]
        if not cores:
            # Fall back to whatever is selected in session state
            cores = [canonical_core_key(c) for c in st.session_state.get("cores_selected", [])]
            cores = [c for c in cores if c]
        if not cores:
            return pd.DataFrame()

        cache = _load_baseline_from_disk(cfg.window_days)
        cache_ok = False
        if isinstance(cache, dict):
            cached_cores = set(_wf_nonempty_list(cache.get("cores", [])))
            if cached_cores and all(c in cached_cores for c in cores):
                cache_ok = True

        rows = []
        progress = st.progress(0)
        for i, core in enumerate(cores, start=1):
            if cache_ok:
                stream_stats = cache["core_stream_stats"][core]
                pos_map = cache["core_pos_maps"][core]
            else:
                stream_stats = compute_stream_stats(df_all, core_key=core, structures=("AABC",), window_days=cfg.window_days)
                pos_map = pos_map_for_core(df_all, core_key=core, structures=["AABC"])
            bucket_rows = build_northern_star_buckets(stream_stats, pos_map, cfg)
            bucket_rows = bucket_rows.copy()
            bucket_rows["Core"] = core
            rows.append(bucket_rows)
            progress.progress(int(i / max(1, len(cores)) * 100))
        progress.empty()

        if not rows:
            return pd.DataFrame()

        out = _safe_pd_concat(rows, ignore_index=True)

        # Universal score (keep identical to Northern Lights tab)
        out = out.copy()
        out["RecentStrength"] = out["RecentHitRate"] * 100.0
        out["DuePressure"] = out["DueScore"]
        out["PosStrength"] = out["PosPctScore"]
        out["UniversalScore"] = 0.45 * out["RecentStrength"] + 0.35 * out["DuePressure"] + 0.20 * out["PosStrength"]
        out["UniversalRank"] = out["UniversalScore"].rank(ascending=False, method="min").astype(int)

        # Column order preference
        cols_front = [
            "UniversalRank", "UniversalScore", "Core", "Stream",
            "Bucket", "BucketPick", "BaseRank", "DueRank",
            "RecentHitRate", "DueScore", "PosPctScore",
            "DaysSinceLastHit", "HitsWindow", "DrawsWindow"
        ]
        out = out[[c for c in cols_front if c in out.columns] + [c for c in out.columns if c not in cols_front]]
        out = out.sort_values(["UniversalRank", "Core", "Stream"]).reset_index(drop=True)
        return out

    if nl_df is None:
        nl_df = _build_master_playlist_for_backtest()
        if nl_df is not None and not nl_df.empty:
            st.session_state["nl_df_current"] = nl_df

    if not isinstance(nl_df, pd.DataFrame) or nl_df.empty:
        st.warning("No master playlist available to backtest. Build it first (Northern Lights tab or button above).")
        return

    playlist_mode = st.radio(
        "What to backtest",
        options=["Top N overall (by UniversalScore)", "Top 1 per stream (best core per stream)", "All playlist entries"],
        index=0,
        horizontal=True,
    )

    # Build the picks table (Core + Stream)
    nl_df_sorted = nl_df.sort_values(["UniversalScore", "UniversalRank"], ascending=[False, True]).copy()
    if playlist_mode.startswith("Top N overall"):
        max_n = max(1, min(500, int(len(nl_df_sorted))))
        default_n = min(39, max_n)
        top_n = st.slider("Top N entries to play each day", 1, max_n, default_n)
        picks = nl_df_sorted.head(top_n)[["Core", "Stream"]].dropna().drop_duplicates().reset_index(drop=True)
    elif playlist_mode.startswith("Top 1 per stream"):
        picks = (
            nl_df_sorted.sort_values(["Stream", "UniversalScore"], ascending=[True, False])
            .groupby("Stream", as_index=False)
            .head(1)[["Core", "Stream"]]
            .dropna().drop_duplicates()
            .reset_index(drop=True)
        )
    else:
        picks = nl_df_sorted[["Core", "Stream"]].dropna().drop_duplicates().reset_index(drop=True)

    if picks.empty:
        st.warning("No playlist picks available for the selected backtest mode.")
        return

    # Cost settings (optional)
    st.markdown("#### Cost assumptions (optional)")
    colc1, colc2, colc3 = st.columns([1, 1, 2])
    with colc1:
        cost_per_play = st.number_input("Cost per play", min_value=0.0, value=0.25, step=0.05)
    with colc2:
        payout_per_win = st.number_input("Payout per win", min_value=0.0, value=247.50, step=1.0)
    with colc3:
        if structure_mode.startswith("Family mode"):
            default_numbers = 3
        else:
            default_numbers = 1
        numbers_per_pick = st.number_input(
            "Number of box numbers per (Core+Stream) pick",
            min_value=1,
            value=int(default_numbers),
            step=1,
            help="If you play all members of a family core, set this to 3. If you play only one number per pick, set to 1."
        )

    # Filter history to date range
    df_range = df_all.copy()
    df_range["Date"] = pd.to_datetime(df_range["Date"])
    start_ts = pd.to_datetime(start_date)
    end_ts = pd.to_datetime(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    df_range = df_range[(df_range["Date"] >= start_ts) & (df_range["Date"] <= end_ts)]

    if df_range.empty:
        st.warning("No draws found in the selected date range.")
        return

    # Ensure BoxKey4 exists
    if "BoxKey4" not in df_range.columns:
        if "Result" not in df_range.columns:
            st.error("History file is missing Result/BoxKey4 needed for matching.")
            return
        df_range = df_range.copy()
        df_range["BoxKey4"] = df_range["Result"].astype(str).str.zfill(4).map(box_key)

    # Precompute draw counts per stream for opportunity counting
    draws_by_stream = df_range["Stream"].value_counts().to_dict()

    # Evaluate hits per pick
    core_to_streams = picks.groupby("Core")["Stream"].apply(list).to_dict()

    records = []
    total_opportunities = 0
    total_hits = 0
    total_unique_hit_days = set()

    for core, streams in core_to_streams.items():
        if not streams:
            continue
        members = set(members_from_core(core, structures=structures))
        if not members:
            continue

        df_s = df_range[df_range["Stream"].isin(streams)]
        if df_s.empty:
            continue

        df_hits = df_s[df_s["BoxKey4"].isin(members)]
        # opportunities = sum draws for each stream used by this core
        core_opps = sum(int(draws_by_stream.get(s, 0)) for s in streams)
        core_hits = int(len(df_hits))

        total_opportunities += core_opps
        total_hits += core_hits
        total_unique_hit_days.update(df_hits["Date"].dt.date.unique().tolist())

        # Stream-level breakdown
        hits_by_stream = df_hits.groupby("Stream").size().to_dict()
        for s in streams:
            opp = int(draws_by_stream.get(s, 0))
            h = int(hits_by_stream.get(s, 0))
            records.append({

"Core": core,
"ActualMemberLabel": None,
"ActualFamilyMember": None,
"PredMemberTop1": None,
"PredMemberTop2": None,
"MemberHitTop1": None,
"MemberHitTop2": None,
"MemberTrainN": 0,
"TrainCnt_AABC": 0,
"TrainCnt_ABBC": 0,
"TrainCnt_ABCC": 0,
                "Stream": s,
                "Opportunities": opp,
                "Hits": h,
                "HitRate": (h / opp) if opp else 0.0
            })

    if not records:
        st.warning("No matching hits found for the selected settings.")
        return

    bt_df = pd.DataFrame(records).sort_values(["Hits", "HitRate"], ascending=[False, False]).reset_index(drop=True)

    days_in_range = int((end_ts.normalize() - start_ts.normalize()).days) + 1
    plays_per_day = int(len(picks)) * int(numbers_per_pick)
    total_plays = int(total_opportunities) * int(numbers_per_pick)
    est_cost = float(total_plays) * float(cost_per_play)
    est_payout = float(total_hits) * float(payout_per_win)
    est_profit = est_payout - est_cost

    colm1, colm2, colm3, colm4 = st.columns(4)
    colm1.metric("Hits", f"{total_hits}")
    colm2.metric("Unique hit days", f"{len(total_unique_hit_days)} / {days_in_range}")
    colm3.metric("Opportunities", f"{total_opportunities}")
    colm4.metric("Plays/day (assumed)", f"{plays_per_day}")

    st.markdown("#### Estimated cost / payout (using your assumptions)")
    colp1, colp2, colp3 = st.columns(3)
    colp1.metric("Estimated cost", f"${est_cost:,.2f}")
    colp2.metric("Estimated payout", f"${est_payout:,.2f}")
    colp3.metric("Estimated profit", f"${est_profit:,.2f}")

    st.markdown("#### Backtest breakdown (per Core + Stream)")
    _safe_st_dataframe(bt_df, use_container_width=True)

    # Summaries
    st.markdown("#### Summary by core")
    core_sum = bt_df.groupby("Core", as_index=False).agg(
        Opportunities=("Opportunities", "sum"),
        Hits=("Hits", "sum")
    )
    core_sum["HitRate"] = core_sum["Hits"] / core_sum["Opportunities"].replace(0, np.nan)
    core_sum = core_sum.sort_values(["Hits", "HitRate"], ascending=[False, False]).reset_index(drop=True)
    _safe_st_dataframe(core_sum, use_container_width=True)

    # Download
    csv_bytes = bt_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download backtest breakdown (CSV)",
        data=csv_bytes,
        file_name="backtest_core_stream_breakdown.csv",
        mime="text/csv",
        use_container_width=True,
    )


# -------------------------
# SAFETY DEFAULTS (prevent NameError on first load / before actions run)
# These are overwritten later when the relevant UI/actions execute.
# -------------------------
try:
    import pandas as _pd  # already imported as pd later; safe fallback
except Exception:
    _pd = None

out = (_pd.DataFrame() if _pd is not None else None)  # walk-forward output placeholder
member_track = False  # member tracking checkbox state placeholder
cores_for_cache = []  # multi-core list placeholder
cores_for_cache_ms = []  # multiselect placeholder
df_all = (_pd.DataFrame() if _pd is not None else None)
df_24h = (_pd.DataFrame() if _pd is not None else None)

def _build_outputs_zip(files: dict[str, object]) -> bytes:
    """Build an in-memory ZIP from DataFrames, text, or bytes without mutating app state."""
    import io as _io
    import zipfile as _zipfile
    _bio = _io.BytesIO()
    with _zipfile.ZipFile(_bio, "w", _zipfile.ZIP_DEFLATED) as _z:
        for _name, _obj in files.items():
            if _obj is None:
                continue
            if isinstance(_obj, pd.DataFrame):
                _data = _obj.to_csv(index=False).encode("utf-8")
            elif isinstance(_obj, str):
                _data = _obj.encode("utf-8")
            else:
                _data = bytes(_obj)
            _z.writestr(str(_name), _data)
    return _bio.getvalue()


st.set_page_config(page_title="Pick 4 Northern Star", layout="wide", initial_sidebar_state="expanded")

st.title("Pick 4 — Northern Star + Rare Engine (AAAB+AABB) + Ultra‑Rare (AAAA)")
st.markdown(f"**Build:** `{APP_VERSION}`")
st.caption("Selected-core output lock, complete base+extra trait pipeline audit, dated seed/member playlists, and auditable straight shortlist exports enabled. The Lab alone may use the broader core catalog.")

# Safe init for sidebar footer (values are filled after parsing uploads)
last_all = None
last_24 = None
df_all = None
df_24 = None


with st.sidebar:
    st.header("Data")
    master_file = st.file_uploader("All‑states history file (.csv or LotteryPost .txt)", type=["csv","txt"])
    map24_file = st.file_uploader("24h map file (optional, same format)", type=["csv","txt"])

    st.subheader("Startup file audit")
    _root = Path(__file__).resolve().parent
    _startup_files = {
        "Core traits — base positive": "family_seed_traits_DOUBLES_top_positive_EXPANDED.csv",
        "Core traits — base negative": "family_seed_traits_DOUBLES_top_negative_EXPANDED.csv",
        "Core traits — extra positive": "family_seed_traits_DOUBLES_core_extra_positive.csv",
        "Core traits — extra negative": "family_seed_traits_DOUBLES_core_extra_negative.csv",
        "Member pair rules": "member_pair_rules_v1.csv",
    }
    _startup_rows = []
    for _label, _filename in _startup_files.items():
        _exists = (_root / _filename).is_file()
        _startup_rows.append({"Component": _label, "Status": "✅ Found" if _exists else "❌ Missing", "Root filename": _filename})
    _safe_st_dataframe(pd.DataFrame(_startup_rows), hide_index=True, use_container_width=True)
    _missing_root_files = [r["Root filename"] for r in _startup_rows if r["Status"].startswith("❌")]
    if _missing_root_files:
        st.warning("Missing optional/autoload root files: " + ", ".join(_missing_root_files) + ". Manual upload controls remain available where supported.")
    else:
        st.success("All root-level rule and trait files were found. Parsed rule counts and trait reachability appear after history loads and are exported in Download All.")

    st.divider()
    st.subheader("Seed Traits + Cadence (v51)")
    traits_pos_file = st.file_uploader("Seed traits POSITIVE CSV (optional; autoloads if present)", type=["csv"], key="traits_pos_file")
    traits_neg_file = st.file_uploader("Seed traits NEGATIVE CSV (optional; autoloads if present)", type=["csv"], key="traits_neg_file")

    # NEW (additive): optional extra trait tables (core-level + member-level)
    # These are NOT required. If not provided, they are simply ignored.
    traits_pos_extra_file = st.file_uploader(
        "Extra CORE seed traits POSITIVE CSV (optional)",
        type=["csv"],
        key="traits_pos_extra_file",
    )
    traits_neg_extra_file = st.file_uploader(
        "Extra CORE seed traits NEGATIVE CSV (optional)",
        type=["csv"],
        key="traits_neg_extra_file",
    )

    member_traits_pos_file = st.file_uploader(
        "Member seed traits POSITIVE CSV (optional)",
        type=["csv"],
        key="member_traits_pos_file",
    )
    member_traits_neg_file = st.file_uploader(
        "Member seed traits NEGATIVE CSV (optional)",
        type=["csv"],
        key="member_traits_neg_file",
    )
    cadence_md_file = st.file_uploader("Family cadence report (.md optional; autoloads if present)", type=["md","txt"], key="cadence_md_file")

    enable_seed_traits = st.checkbox("Enable Seed Traits boost (soft only)", value=True, key="enable_seed_traits")
    enable_member_seed_overrides = st.checkbox(
        "Enable Member Seed-Trait overrides (soft, after MODE/LAST)",
        value=False,
        key="enable_member_seed_overrides",
    )
    seed_traits_weight = st.slider("Seed Traits weight", 0.0, 1.0, 0.35, 0.05, key="seed_traits_weight")
    enable_cadence = st.checkbox("Enable Cadence boost (soft only)", value=True, key="enable_cadence")
    cadence_weight = st.slider("Cadence weight", 0.0, 1.0, 0.25, 0.05, key="cadence_weight")

    # Keep these weights conservative by default
    due_weight = st.slider("DuePressure weight", 0.0, 1.0, 0.20, 0.05, key="due_weight")
    pos_weight = st.slider("Position-percentile weight", 0.0, 1.0, 0.25, 0.05, key="pos_weight")

    map_file = map24_file  # backward-compatible alias

    exclude_md = st.checkbox("Exclude Maryland (MD)", value=True, help="Optional global exclusion. When enabled (default), MD rows are removed from both the baseline and 24h files before ranking.")
    st.session_state["exclude_md"] = exclude_md

    st.divider()
    st.subheader("Trigger Map (39-play list) — optional boost")
    _apply = st.checkbox("Apply Trigger Map boost", value=False)
    st.session_state["_apply_trigger_map"] = _apply
    _pts = st.slider("Trigger boost points", min_value=0.0, max_value=10.0, value=2.0, step=0.5)
    st.session_state["_trigger_boost_points"] = float(_pts)


    st.divider()
    st.divider()
    with st.expander("Build checklist (do not omit)", expanded=False):
        # --- Live status (auto) ---
        st.markdown("### Live status")
        _sel_now = _wf_first_nonempty_selection(st.session_state.get("cores_for_cache_ms", []), st.session_state.get("selected_cores", []))
        st.write({
            "hardcoded_daily_doubles_cores": len(CORE_PRESETS),
            "selected_cores_now": len(_sel_now),
            "selected_cores_list": _sel_now[:25] + (["…"] if len(_sel_now) > 25 else []),
        })

        st.markdown("""
**A. Core + cache**
- **A1** Multi-core selection (core dropdown + multi-select)
- **A2** Cache Builder: build baseline cache for selected cores
- **A3** Show tabs for all selected cores (optional) in Core view
- **A4** Core ranking percentile map (tie-breaker) in Northern Lights view
- **A5** Bucket method: Top 12 + DueIndex 13–60 (8 picks)
- **A6** Straights module optional last (does not run unless enabled)

**B. Northern Star / Northern Lights**
- **B1** Northern Star (per-core) ranking view
- **B2** Northern Star buckets per core (Base + Due)
- **B3** Northern Lights master playlist (cross-core)
- **B4** Master playlist scoring is deterministic (stable tie-break)
- **B5** Optional Trigger Map boost (39-play list)

**C. Maps / percentiles**
- **C1** Global Northern Star position percentile map (1–78)
- **C2** Per-core percentile map tabs for selected cores

**D. Cadence & behavior (soft-only)**
- **D1** Cadence report integration (soft boost / transparency)
- **D2** Core-specific behavior tables (per-core stats cached)

**E. Self-maintenance**
- **E1** Local rolling ~3-year baseline store (append from 24h)
- **E2** Purge rows older than ~3 years (automatic)
- **E3** Store status panel: rows + date range + last updated
- **E4** One-click store rebuild/reset (safety)

If any item is missing in a build, treat it as a regression and restore it before adding new features.
""")


    st.subheader("Self-update rolling baseline (optional)")
    use_store = st.checkbox("Use local rolling ~3-year baseline store", value=False, help="Keeps a local rolling baseline by appending new rows from the 24h file and purging rows older than ~3 years. This improves speed and keeps your baseline fresh without you manually editing the all-states file.")
    st.session_state["use_store"] = use_store

    if use_store:
        store_df_preview = load_baseline_store()
        store_meta = _read_meta(BASELINE_STORE_BASE)
        store_rows = int(store_meta.get("rows", store_df_preview.shape[0] if store_df_preview is not None else 0) or 0)
        store_max = store_meta.get("max_date", "") or (str(store_df_preview["Date"].max()) if store_df_preview is not None and not store_df_preview.empty else "")

        colA, colB = st.columns(2)
        with colA:
            if st.button("Initialize/overwrite store from uploaded all-states file"):
                if master_file is None:
                    st.warning("Upload the all-states history file first.")
                else:
                    try:
                        master_file.seek(0)
                    except Exception:
                        pass
                    try:
                        df_init = try_read_tablelike(master_file)
                        df_init = purge_to_rolling_3y(df_init, years=3)
                        ok, wrote = write_baseline_store(df_init, note="Initialized from uploaded all-states file (rolling 3y).")
                        st.success(f"Baseline store saved: {wrote}")
                    except Exception as e:
                        st.error(f"Could not initialize store: {e}")
                    try:
                        master_file.seek(0)
                    except Exception:
                        pass
        with colB:
            if st.button("Append 24h file into store (and purge)"):
                if map24_file is None:
                    st.warning("Upload the 24h file first.")
                else:
                    try:
                        map24_file.seek(0)
                    except Exception:
                        pass
                    try:
                        df_new = try_read_tablelike(map24_file)
                        df_store = load_baseline_store()
                        merged, added = append_from_24h(df_store, df_new)
                        merged = purge_to_rolling_3y(merged, years=3)
                        ok, wrote = write_baseline_store(merged, note=f"Appended from 24h file (+{added} new rows), then purged to rolling 3y.")
                        st.success(f"Updated store: +{added} new rows. Saved: {wrote}")
                    except Exception as e:
                        st.error(f"Could not append 24h into store: {e}")
# ---------- Core selection ----------
st.header("Core selection")
st.caption("Working 8 is preloaded. All production outputs are restricted to the selected cores; the Core Set Lab alone may use the broader catalog.")

# Start from the curated preset list, then union with any cores detected from data (if present)
_detected_cores: list[str] = []
try:
    if isinstance(df_all, pd.DataFrame) and "Core" in df_all.columns:
        _det = df_all["Core"].dropna().astype(str).str.extract(r"(\d{1,3})", expand=False).dropna().unique().tolist()
        _det = [str(x).zfill(3) for x in _det]
        _det = [c for c in _det if c.isdigit()]
        _detected_cores = sorted(set(_det))
except Exception:
    _detected_cores = []

available_cores = sorted(set([str(c).zfill(3) for c in CORE_PRESETS] + [str(c).zfill(3) for c in WORKING8_CORE_SET] + _detected_cores))
cores = available_cores  # alias used throughout UI

# Ensure default core exists
default_core = str(getattr(cfg, "default_core", "389")).zfill(3)
if default_core not in available_cores:
    available_cores = [default_core] + available_cores

# Persist and lock the production selection before any core-dependent widget is created.
# v51.38 uses a new multiselect widget key below; this legacy key is now only a
# compatibility mirror and can be safely replaced on every rerun.
st.session_state["cores_for_cache_ms"] = list(WORKING8_CORE_SET)
st.session_state["cores_for_cache"] = list(WORKING8_CORE_SET)

# Core view dropdown (single core). Outside the Lab, choices are locked to the active selection.
_preselected_for_view = [str(c).zfill(3) for c in (_wf_first_nonempty_selection(st.session_state.get("cores_for_cache_ms"), WORKING8_CORE_SET))]
_view_core_options = [c for c in _preselected_for_view if c in cores] or [c for c in WORKING8_CORE_SET if c in cores]
vc_default = str(default_core).zfill(3) if str(default_core).zfill(3) in _view_core_options else (_view_core_options[0] if _view_core_options else '000')
try:
    vc = str(st.session_state.get('view_core', vc_default)).zfill(3)
except Exception:
    vc = vc_default
if vc not in _view_core_options:
    vc = vc_default
view_core = st.selectbox("View selected core", _view_core_options, index=_view_core_options.index(vc) if vc in _view_core_options else 0, key="view_core")
core_for_view = view_core

# Multi-core selection for cache build / batch tools.
# v51.38: use a new widget key so stale Streamlit state from older builds cannot
# silently restore a seven-core selection. The production/backtest selection is
# locked to the exact Working 8. Broader core exploration remains available in
# the Core Set Lab, not in this production selector.
def _normalized_valid_core_list(values: list[str]) -> list[str]:
    seen = set()
    out = []
    for value in values:
        core = str(value).zfill(3)
        if core in cores and core not in seen:
            seen.add(core)
            out.append(core)
    return out

_expected_working8 = list(WORKING8_CORE_SET)
_missing_catalog_working8 = [c for c in _expected_working8 if c not in cores]
if _missing_catalog_working8:
    st.error(
        "FATAL CORE CATALOG ERROR — Working 8 core(s) absent from the selectable catalog: "
        + ", ".join(_missing_catalog_working8)
    )
    st.stop()

# IMPORTANT: this is intentionally a brand-new widget key. Any stale value held
# under the older cores_for_cache_ms key is ignored and overwritten below.
_WORKING8_WIDGET_KEY = "cores_for_cache_ms_v5138"
# Set/reset this widget-owned value only before the widget is instantiated.
# This guarantees 8/8 even if Streamlit Cloud somehow retained a partial value.
if st.session_state.get(_WORKING8_WIDGET_KEY) != _expected_working8:
    st.session_state[_WORKING8_WIDGET_KEY] = list(_expected_working8)

cores_for_cache_ms = st.multiselect(
    "Cores to include for cache building / batch tools",
    options=cores,
    key=_WORKING8_WIDGET_KEY,
    disabled=True,
    help=(
        "Production and walk-forward backtests are locked to the Working 8: "
        + ", ".join(_expected_working8)
    ),
)
cores_for_cache_ms = _normalized_valid_core_list(list(cores_for_cache_ms))

# Mirror the locked selection to the legacy/session keys used elsewhere in the
# app. These are not widget-owned in v51.38, so this is safe.
st.session_state["cores_for_cache_ms"] = list(cores_for_cache_ms)
st.session_state["cores_for_cache"] = list(cores_for_cache_ms)
st.session_state["working8_lock_requested"] = True

_selected_set = set(cores_for_cache_ms)
_expected_set = set(_expected_working8)
_working8_missing = [c for c in _expected_working8 if c not in _selected_set]
_working8_extra = [c for c in cores_for_cache_ms if c not in _expected_set]
_working8_exact = (cores_for_cache_ms == _expected_working8)
st.session_state["working8_exact_pass"] = bool(_working8_exact)

st.caption("Selected: " + (", ".join(cores_for_cache_ms) if cores_for_cache_ms else "none"))
if _working8_exact:
    st.success("WORKING 8 PRECHECK: PASS — 8/8 cores locked: " + ", ".join(_expected_working8))
else:
    st.error(
        "WORKING 8 PRECHECK: FAIL — "
        f"detected {len(cores_for_cache_ms)}/8. "
        f"Missing: {', '.join(_working8_missing) if _working8_missing else 'none'}. "
        f"Extra: {', '.join(_working8_extra) if _working8_extra else 'none'}. "
        "The app is blocked because the locked Working-8 contract did not pass."
    )
    st.stop()

# --- SAFETY: always define cores_for_cache so later UI blocks can't NameError
# (Some sections reference cores_for_cache even if only cores_for_cache_ms exists.)
cores_for_cache = _wf_first_nonempty_selection(st.session_state.get('cores_for_cache'), st.session_state.get('cores_for_cache_ms'))
cores_for_cache = [str(c).zfill(3) for c in cores_for_cache if str(c).zfill(3) in cores]
st.header("Northern Star window")

window_days = st.radio("Window (days)", options=[180, 365], index=0, horizontal=True)
cfg = RankConfig(window_days=window_days)


st.divider()
st.header("Rare Engine trigger — AAAB + AABB")
r1 = st.checkbox("R1: Top‑20% baseline AAAB+AABB", value=True)
r2 = st.checkbox("R2: Top‑20% recent (last window)", value=True)
r3 = st.checkbox("R3: 24h has ≥3 AAAB/AABB hits across ≥3 streams", value=True)
r4 = st.checkbox("R4: 24h cluster ∈ Top‑10 positions", value=True)

st.divider()
st.header("Ultra‑Rare trigger — AAAA")
q1 = st.checkbox("Q1: Top‑10% quad baseline", value=True)
q2 = st.checkbox("Q2: Due pressure ≥ P90", value=True)
q3 = st.checkbox("Q3: 24h quad exists (core digits)", value=True)
q4 = st.checkbox("Q4: 24h cluster ∈ Top‑5 positions", value=True)

st.divider()
straights_opt = st.checkbox("Generate straights shortlist (optional last)", value=False, key="do_straights")


# Load data
use_store = bool(st.session_state.get("use_store", False))

if use_store:
    # Prefer the on-disk rolling store if present
    df_all = load_baseline_store()
    # If the store is empty but the user uploaded a master file, auto-initialize (rolling 3y)
    if (df_all is None or df_all.empty) and master_file is not None:
        try:
            master_file.seek(0)
        except Exception:
            pass
        df_all = try_read_tablelike(master_file)
        df_all = purge_to_rolling_3y(df_all, years=3)
        try:
            write_baseline_store(df_all, note="Auto-initialized store from uploaded all-states file (rolling 3y).")
        except Exception:
            pass
        try:
            master_file.seek(0)
        except Exception:
            pass
else:
    df_all = try_read_tablelike(master_file) if master_file else pd.DataFrame()

prev_picklist = pd.DataFrame()
df_24h = pd.DataFrame()
if map24_file:
    try:
        df_24h = try_read_tablelike(map24_file)
    except Exception:
        # Many users upload a simple pick-list here (one 4-digit number per line).
        # Accept it without crashing; it will NOT be used for 24h engines or baseline self-update.
        try:
            map24_file.seek(0)
        except Exception:
            pass
        try:
            prev_picklist = try_read_picklist(map24_file)
            if not prev_picklist.empty:
                st.info(
                    "Optional 24h/previous-day file detected as a pick-list (not a LotteryPost history export). "
                    "It will be used only for annotation/downranking where applicable."
                )
        except Exception as e:
            st.warning(f"Could not parse optional 24h/previous-day file: {e}")


if exclude_md and not df_all.empty:
    df_all = df_all[df_all["State"].astype(str).str.strip().str.lower() != "maryland"].copy()
if exclude_md and not df_24h.empty:
    df_24h = df_24h[df_24h["State"].astype(str).str.strip().str.lower() != "maryland"].copy()

# Back-compat alias used by the Northern Lights block
df_24 = df_24h


# Auto-clear cached percentile maps when input data changes
if use_store:
    _m = _read_meta(BASELINE_STORE_BASE)
    all_hash = f"store|{_m.get('max_date','')}|{_m.get('rows','')}"
else:
    all_hash = file_fingerprint(master_file)
map_hash = file_fingerprint(map24_file)
if "data_hash_all" not in st.session_state:
    st.session_state["data_hash_all"] = ""
if "data_hash_24h" not in st.session_state:
    st.session_state["data_hash_24h"] = ""

if all_hash and all_hash != st.session_state["data_hash_all"]:
    st.session_state["pos_map_cache"] = {}
    st.session_state["data_hash_all"] = all_hash
    st.session_state["recompute_token"] = ""  # will refresh on next compute
if map_hash != st.session_state["data_hash_24h"]:
    st.session_state["pos_map_cache"] = {}
    st.session_state["data_hash_24h"] = map_hash
    st.session_state["recompute_token"] = ""

# Show data freshness (so the instructions never need updating)
last_all = most_recent_date(df_all)
last_24 = most_recent_date(df_24h)


# ---- v51: Seed Traits + Cadence data (autoload + optional uploads)
DEFAULT_TRAITS_POS_PATH = "family_seed_traits_DOUBLES_top_positive_EXPANDED.csv"
DEFAULT_TRAITS_NEG_PATH = "family_seed_traits_DOUBLES_top_negative_EXPANDED.csv"
DEFAULT_CADENCE_MD_PATH = "family_cadence_report.md"

# Load all four core trait libraries. Uploaded files override matching root files.
DEFAULT_TRAITS_POS_EXTRA_PATH = "family_seed_traits_DOUBLES_core_extra_positive.csv"
DEFAULT_TRAITS_NEG_EXTRA_PATH = "family_seed_traits_DOUBLES_core_extra_negative.csv"
_seed_pos_base = _read_local_or_uploaded_csv(globals().get("traits_pos_file", None), DEFAULT_TRAITS_POS_PATH)
_seed_neg_base = _read_local_or_uploaded_csv(globals().get("traits_neg_file", None), DEFAULT_TRAITS_NEG_PATH)
_seed_pos_extra = _read_local_or_uploaded_csv(globals().get("traits_pos_extra_file", None), DEFAULT_TRAITS_POS_EXTRA_PATH)
_seed_neg_extra = _read_local_or_uploaded_csv(globals().get("traits_neg_extra_file", None), DEFAULT_TRAITS_NEG_EXTRA_PATH)
for _df, _src, _pol in [(_seed_pos_base,"BASE_POSITIVE","POSITIVE"),(_seed_neg_base,"BASE_NEGATIVE","NEGATIVE"),(_seed_pos_extra,"EXTRA_POSITIVE","POSITIVE"),(_seed_neg_extra,"EXTRA_NEGATIVE","NEGATIVE")]:
    if _df is not None and not _df.empty:
        _df["RuleSource"] = _src
        _df["Polarity"] = _pol
        _df["RuleID"] = _df.apply(lambda r: f"{_src}|{int(r.get('core_family',0)):03d}|{r.get('trait','')}|{r.get('value','')}", axis=1)
seed_traits_all_df = _safe_pd_concat([d for d in [_seed_pos_base,_seed_neg_base,_seed_pos_extra,_seed_neg_extra] if d is not None and not d.empty], ignore_index=True) if any(d is not None and not d.empty for d in [_seed_pos_base,_seed_neg_base,_seed_pos_extra,_seed_neg_extra]) else pd.DataFrame()
seed_traits_pos_df = _safe_pd_concat([d for d in [_seed_pos_base,_seed_pos_extra] if d is not None and not d.empty], ignore_index=True) if any(d is not None and not d.empty for d in [_seed_pos_base,_seed_pos_extra]) else pd.DataFrame()
seed_traits_neg_df = _safe_pd_concat([d for d in [_seed_neg_base,_seed_neg_extra] if d is not None and not d.empty], ignore_index=True) if any(d is not None and not d.empty for d in [_seed_neg_base,_seed_neg_extra]) else pd.DataFrame()
# Resolve exact core+trait+value collisions deterministically while preserving a collision audit.
def _resolve_trait_collisions(_df: pd.DataFrame) -> tuple[pd.DataFrame,pd.DataFrame]:
    if _df is None or _df.empty: return pd.DataFrame(), pd.DataFrame()
    _x=_df.copy(); _x["core_family"]=pd.to_numeric(_x["core_family"],errors="coerce").fillna(-1).astype(int); _x["trait"]=_x["trait"].astype(str); _x["value"]=_x["value"].astype(str)
    _x["_lift"]=pd.to_numeric(_x.get("lift",1.0),errors="coerce").fillna(1.0); _x["_hits"]=pd.to_numeric(_x.get("trait_hits",0),errors="coerce").fillna(0)
    _dup=_x[_x.duplicated(["core_family","trait","value"],keep=False)].sort_values(["core_family","trait","value","_hits","_lift"],ascending=[True,True,True,False,False])
    _keep=_x.sort_values(["_hits","_lift"],ascending=[False,False]).drop_duplicates(["core_family","trait","value"],keep="first").drop(columns=["_lift","_hits"])
    return _keep,_dup.drop(columns=["_lift","_hits"],errors="ignore")
seed_traits_pos_df, seed_traits_pos_collision_audit = _resolve_trait_collisions(seed_traits_pos_df)
seed_traits_neg_df, seed_traits_neg_collision_audit = _resolve_trait_collisions(seed_traits_neg_df)
seed_traits_pos_lookup = _build_traits_lookup(seed_traits_pos_df)
seed_traits_neg_lookup = _build_traits_lookup(seed_traits_neg_df)
_IMPLEMENTED_TRAITS = set(_feature_values_for_seed("0123","027",set()).keys())
_trait_names = sorted(set(seed_traits_all_df.get("trait",pd.Series(dtype=str)).astype(str))) if not seed_traits_all_df.empty else []
trait_mapping_audit_df = pd.DataFrame({"TraitName":_trait_names})
if not trait_mapping_audit_df.empty:
    trait_mapping_audit_df["Implemented"] = trait_mapping_audit_df["TraitName"].isin(_IMPLEMENTED_TRAITS)
    _rc=seed_traits_all_df.groupby("trait").size().to_dict(); trait_mapping_audit_df["RuleCount"]=trait_mapping_audit_df["TraitName"].map(_rc).fillna(0).astype(int)
trait_dictionary_audit_df = _safe_pd_concat([seed_traits_pos_collision_audit.assign(PolarityAudit="POSITIVE"), seed_traits_neg_collision_audit.assign(PolarityAudit="NEGATIVE")], ignore_index=True)

cadence_report_text = _read_local_or_uploaded_text(globals().get("cadence_md_file", None), DEFAULT_CADENCE_MD_PATH)

# Precompute per-stream seed + last5 union digits (for Seed Traits feature)
_prev_seed_by_stream: Dict[str, str] = {}
_last5_union_by_stream: Dict[str, set] = {}

try:
    # Determine the most recent 4-digit seed per stream (prefer 24h map if present)
    if df_24h is not None and not df_24h.empty and "Stream" in df_24h.columns and "Result" in df_24h.columns:
        _tmp = df_24h.copy()
        if "Date" in _tmp.columns:
            _tmp = _tmp.sort_values("Date")
        # take last per stream
        _prev = _tmp.groupby("Stream", as_index=False).tail(1)
        _prev_seed_by_stream = dict(zip(_prev["Stream"].astype(str), _prev["Result"].astype(str)))
    if not _prev_seed_by_stream:
        _tmp = df_all.copy()
        _tmp = _tmp.sort_values("Date")
        _prev = _tmp.groupby("Stream", as_index=False).tail(1)
        _prev_seed_by_stream = dict(zip(_prev["Stream"].astype(str), _prev["Result"].astype(str)))

    # last5 union digits per stream from df_all (most recent 5 rows)
    _tmp = df_all.sort_values("Date")
    for s, g in _tmp.groupby("Stream"):
        tail = g.tail(5)
        digs = set("".join(tail["Result"].astype(str).tolist()))
        _last5_union_by_stream[str(s)] = digs
except Exception:
    pass

today = datetime.date.today()



def _wf_local_seed_maps(train_df: pd.DataFrame) -> tuple[dict[str, str], dict[str, set[str]]]:
    """Build leakage-safe previous-seed and last-5 digit maps from training rows only."""
    prev_map: dict[str, str] = {}
    last5_map: dict[str, set[str]] = {}
    if train_df is None or train_df.empty:
        return prev_map, last5_map
    z = train_df.copy()
    z["Date"] = pd.to_datetime(z.get("Date"), errors="coerce")
    z = z.dropna(subset=["Date", "Stream"]).sort_values(["Stream", "Date"], kind="mergesort")
    if "Result" in z.columns:
        last_rows = z.groupby("Stream", sort=False).tail(1)
        prev_map = {
            str(r.Stream): (re.sub(r"\D", "", str(r.Result or ""))[-4:]).zfill(4)
            for r in last_rows.itertuples(index=False)
        }
        for stream, g in z.groupby("Stream", sort=False):
            vals = g.tail(5)["Result"].astype(str).tolist()
            last5_map[str(stream)] = set("".join(re.sub(r"\D", "", v) for v in vals))
    return prev_map, last5_map


def _wf_build_v5126_chart_universe(
    train_df: pd.DataFrame,
    cfg: "RankConfig",
    cores_sel: list[str],
    include_final_boxed: bool = True,
    base_due_gate_mode: str = BASE_DUE_GATE_MODE,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, float]]:
    """Fast leakage-safe v51.26 chart reconstruction for one historical date.

    Each core ranking is calculated once. Northern Star membership is vectorized from that
    same table instead of rebuilding/sorting the table once per stream. The caller may now
    retain the complete nonwinner universe when full-universe export mode is selected.
    """
    timings = {"seed_maps_s": 0.0, "core_rankings_s": 0.0, "northern_lights_s": 0.0, "final_boxed_s": 0.0}
    if train_df is None or train_df.empty or not cores_sel:
        return pd.DataFrame(), pd.DataFrame(), timings

    t0 = time.perf_counter()
    prev_seed_map, last5_map = _wf_local_seed_maps(train_df)
    timings["seed_maps_s"] = time.perf_counter() - t0
    rows: list[dict[str, Any]] = []
    window_days = int(cfg.window_days)
    due_w = float(st.session_state.get("due_weight", 0.20))
    pos_w = float(st.session_state.get("pos_weight", 0.25))
    st_w = float(st.session_state.get("seed_traits_weight", 0.35))
    cad_w = float(st.session_state.get("cadence_weight", 0.25))
    enable_traits = bool(st.session_state.get("enable_seed_traits", True))
    enable_cadence = bool(st.session_state.get("enable_cadence", True))

    t_core = time.perf_counter()

    def _build_one_core(core: str) -> list[dict[str, Any]]:
        core_rows: list[dict[str, Any]] = []
        try:
            stats_df = compute_stream_stats(train_df, core, window_days=window_days)
        except Exception:
            stats_df = pd.DataFrame()
        if stats_df is None or stats_df.empty:
            return core_rows

        s_core = stats_df.copy()
        s_core["Stream"] = s_core["Stream"].astype(str)
        base_order = s_core.sort_values(["HitsPerWeek", "HitsWindow"], ascending=[False, False], kind="mergesort").reset_index(drop=True)
        base_order["_NSBaseRank"] = base_order.index + 1
        base_streams = set(base_order.head(int(cfg.top_base))["Stream"].tolist())
        due_band = base_order[(base_order["_NSBaseRank"] >= int(cfg.due_from_rank)) & (base_order["_NSBaseRank"] <= int(cfg.due_to_rank))].copy()
        if due_band.empty:
            due_streams: set[str] = set()
        else:
            due_band = due_band.sort_values(["DaysSinceLastHit", "HitsPerWeek"], ascending=[False, False], kind="mergesort")
            due_streams = set(due_band.head(int(cfg.top_due))["Stream"].tolist())

        pos_df, _ = position_percentile_map(stats_df)
        pos_strength = {}
        if pos_df is not None and not pos_df.empty:
            strength_col = "PctStrength" if "PctStrength" in pos_df.columns else "HitCountPctile"
            pos_strength = dict(zip(pos_df["RankPos"].astype(int), pd.to_numeric(pos_df[strength_col], errors="coerce").fillna(0.0)))
        total_hits = float(pd.to_numeric(stats_df.get("HitsWindow", 0), errors="coerce").fillna(0).sum())
        mean_gap_days = (window_days / total_hits) if total_hits > 0 else 0.0

        for sr in stats_df.to_dict("records"):
            stream = str(sr.get("Stream", "")).strip()
            in_base = stream in base_streams
            in_due = stream in due_streams
            rankpos = int(sr.get("RankPos", 9999) or 9999)
            seed = prev_seed_map.get(stream, "")
            seed_score = 0.0
            if enable_traits and seed_traits_pos_lookup:
                try:
                    seed_score, _ = compute_seed_traits_score(
                        core, seed, stream,
                        pos_lookup=seed_traits_pos_lookup,
                        neg_lookup=seed_traits_neg_lookup,
                        last5_union_digits_by_stream=last5_map,
                    )
                except Exception:
                    seed_score = 0.0
            hits_pw = float(sr.get("HitsPerWeek", 0.0) or 0.0)
            days_since = float(sr.get("DaysSinceLastHit", 0.0) or 0.0)
            due_pressure = float(days_since if in_due else 0.0)
            pct_strength = float(pos_strength.get(rankpos, 0.0) or 0.0)
            cadence_score = 0.0
            if enable_cadence and mean_gap_days > 0:
                try:
                    cadence_score = float(compute_cadence_score(days_since, mean_gap_days))
                except Exception:
                    cadence_score = 0.0
            universal = (
                hits_pw
                + (min(days_since, 50.0) * 0.01 * due_w)
                + (pct_strength * 0.01 * pos_w)
                + (seed_score * st_w if enable_traits else 0.0)
                + (cadence_score * cad_w if enable_cadence else 0.0)
            )
            bucket_pick = "BASE" if in_base else ("DUE" if in_due else "")
            core_rows.append({
                "Core": core, "Stream": stream, "Seed": seed, "BucketPick": bucket_pick,
                "UniversalScore": float(universal), "HitsPerWeek": hits_pw,
                "HitsWindow": int(sr.get("HitsWindow", 0) or 0),
                "DaysSinceLastHit": days_since, "DuePressure": due_pressure,
                "PctStrength": pct_strength, "SeedTraitsScore": float(seed_score),
                "CadenceScore": float(cadence_score), "RankPos": rankpos,
                "BaseScoreRank": int(sr.get("BaseScoreRank", rankpos) or rankpos),
            })
        return core_rows

    core_list = [str(c).zfill(3) for c in cores_sel]
    max_workers = max(1, min(len(core_list), int(st.session_state.get("bt_workers", min(4, os.cpu_count() or 2)))))
    if max_workers == 1 or len(core_list) <= 1:
        for core in core_list:
            rows.extend(_build_one_core(core))
    else:
        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="wf-core") as pool:
            futures = {pool.submit(_build_one_core, core): core for core in core_list}
            for fut in as_completed(futures):
                try:
                    rows.extend(fut.result())
                except Exception:
                    pass
    timings["core_rankings_s"] = time.perf_counter() - t_core

    if not rows:
        return pd.DataFrame(), pd.DataFrame(), timings
    t_nl = time.perf_counter()
    u = pd.DataFrame(rows)
    u = u.sort_values(
        ["UniversalScore", "HitsPerWeek", "DuePressure", "RankPos", "Stream", "Core"],
        ascending=[False, False, False, True, True, True], kind="mergesort",
    ).reset_index(drop=True)
    u["NorthernLightsRank"] = u.index + 1
    u["NorthernLightsPercentile"] = (u["NorthernLightsRank"] / max(1, len(u)) * 100.0).round(4)
    # Preserve the original Base/Due decision for audit. The WF default is the exact
    # Daily production mode, while comparison branches remain explicitly selectable.
    u["OriginalBaseDueEligible"] = _playable_bucket_mask(u).astype(bool)
    gate_mode = str(base_due_gate_mode or DAILY_BASE_DUE_GATE_MODE)
    if gate_mode not in WF_GATE_MODES:
        gate_mode = DAILY_BASE_DUE_GATE_MODE
    u["BaseDueGateMode"] = gate_mode
    if gate_mode == "CURRENT_HARD_GATE":
        u["ProductionEligible"] = u["OriginalBaseDueEligible"].astype(bool)
    elif gate_mode == "AUDIT_FLIPPED_GATE":
        u["ProductionEligible"] = ~u["OriginalBaseDueEligible"].astype(bool)
    else:
        u["ProductionEligible"] = True
    u["MatchesDailyProduction"] = gate_mode == DAILY_BASE_DUE_GATE_MODE
    u["ExperimentalBranch"] = gate_mode != DAILY_BASE_DUE_GATE_MODE
    u["NorthernStarBucket"] = np.where(
        u["BucketPick"].astype(str).str.upper().eq("BASE"), "BaseScore",
        np.where(u["BucketPick"].astype(str).str.upper().eq("DUE"), "Due8", "Neither")
    )
    u["GlobalEligibleStreamCoreRank"] = pd.NA
    elig = u[u["ProductionEligible"]].copy().sort_values(
        ["UniversalScore", "HitsPerWeek", "DuePressure", "RankPos"],
        ascending=[False, False, False, True], kind="mergesort"
    ).reset_index(drop=True)
    if not elig.empty:
        elig["GlobalEligibleStreamCoreRank"] = elig.index + 1
        erank = {(str(r.Core), str(r.Stream)): int(r.GlobalEligibleStreamCoreRank) for r in elig.itertuples(index=False)}
        u["GlobalEligibleStreamCoreRank"] = [erank.get((str(c), str(stm)), pd.NA) for c, stm in zip(u["Core"], u["Stream"])]
    timings["northern_lights_s"] = time.perf_counter() - t_nl

    boxed = pd.DataFrame()
    settled_result = None
    if include_final_boxed and not u.empty:
        t_box = time.perf_counter()
        settled_input = u.copy()
        settled_input["Date"] = pd.to_datetime(train_df["Date"], errors="coerce").max() + pd.Timedelta(days=1)
        settled_result = (
            run_northern_star_open_gate(settled_input, date_col="Date", mode="WF_PRODUCTION_EQUIVALENT")
            if str(base_due_gate_mode) == DAILY_BASE_DUE_GATE_MODE
            else run_settled_pipeline(settled_input, date_col="Date", mode=f"WF_EXPERIMENTAL_{base_due_gate_mode}")
        )
        member_rows = settled_result.member_survivors.copy()
        if member_rows is not None and not member_rows.empty:
            boxed, _ = _rank_global_boxed_plays(member_rows)
            boxed["FinalPlaylistBudget"] = 50
            boxed["FinalPlaylistBundleSelected"] = True
            boxed["FinalPlaylistBundleRank"] = boxed.get("GlobalBoxedPlayRank", pd.Series(range(1, len(boxed)+1), index=boxed.index))
            boxed["FinalPlaylistSelectionReason"] = "SURVIVED_SHARED_SETTLED_95_45_35"
            boxed["FinalPlaylistBoxedRank"] = boxed.get("GlobalBoxedPlayRank", pd.Series(range(1, len(boxed)+1), index=boxed.index))
            boxed["FinalPlaylistIncluded"] = True
        # Do not store PipelineResult/DataFrames inside attrs; pandas compares attrs during concat.
        u.attrs = {}
        timings["final_boxed_s"] = time.perf_counter() - t_box
    return u, boxed, timings, settled_result

def _render_backtest_walk_forward(df_all: pd.DataFrame, cfg: "RankConfig", cores_for_cache: list[str]) -> None:
    """Walk-forward backtest (no future leakage).

    - For each test_date:
      - train_df = rows with Date < test_date
      - build per-core stream ranking/buckets from train_df ONLY
      - score winner rows on test_date against those buckets
    """
    if df_all is None or df_all.empty:
        st.warning("No data loaded.")
        return
    if "Date" not in df_all.columns or "Stream" not in df_all.columns or "Result" not in df_all.columns:
        st.error("Backtest requires columns: Date, Stream, Result.")
        return

    # v51.39 hard contract: this production walk-forward build always runs the exact
    # Working 8. Do not trust a stale caller/local/session list here. The broader
    # catalog remains available only in the Core Set Lab.
    expected_working8 = list(WORKING8_CORE_SET)
    all_cores = list(expected_working8)

    missing_working8 = [c for c in expected_working8 if c not in all_cores]
    extra_working8 = [c for c in all_cores if c not in expected_working8]
    exact_working8 = (all_cores == expected_working8)
    if exact_working8:
        st.success("BACKTEST CORE CONTRACT: PASS — exact Working 8 loaded (8/8).")
    else:
        st.error(
            "BACKTEST CORE CONTRACT: FAIL — run blocked. "
            f"Detected {len(all_cores)}/8 cores. "
            f"Missing: {', '.join(missing_working8) if missing_working8 else 'none'}. "
            f"Extra: {', '.join(extra_working8) if extra_working8 else 'none'}. "
            "Click Select Working 8 above and confirm PASS before running."
        )

    c1, c2, c3 = st.columns([1.2, 1.2, 1])
    with c1:
        use_all = st.checkbox("Test all selected cores", value=True, key="bt_use_all_cores")
    with c2:
        include_rare = st.checkbox("Include AAAB/AABB/AAAA members", value=False, key="bt_include_rare")
    with c3:
        max_dates = st.number_input("Max test dates", min_value=1, max_value=3650, value=120, step=10, key="bt_max_dates")


    st.markdown("##### Member‑pick tracking (optional)")
    m1, m2 = st.columns([1.1, 1.9])
    with m1:
        member_track = st.checkbox("Track member accuracy (Top1/Top2)", value=False, key="bt_member_track")
    with m2:
        member_basis_label = st.selectbox(
            "Member predictor basis",
            ["Per‑core (all streams)", "Per‑core + stream"],
            index=0,
            key="bt_member_basis",
            help="Per‑core uses all streams to learn which member is most common for that core. Per‑core+stream learns separately per stream (more specific, but fewer samples).",
        )
    member_basis = "core_stream" if member_basis_label.startswith("Per‑core + stream") else "core"

    if not all_cores:
        st.info("Select one or more cores above before running a backtest.")
        return
    if use_all:
        cores_sel = list(all_cores)
    else:
        prior = [str(c).zfill(3) for c in st.session_state.get("bt_cores_sel", []) if str(c).zfill(3) in all_cores]
        cores_sel = st.multiselect(
            "Selected cores to test",
            options=all_cores,
            default=prior or list(all_cores),
            key="bt_cores_sel",
        )

    if not cores_sel:
        st.info("Select at least one core to backtest.")
        return

    # Date range
    dmin = pd.to_datetime(df_all["Date"], errors="coerce").min()
    dmax = pd.to_datetime(df_all["Date"], errors="coerce").max()
    if pd.isna(dmin) or pd.isna(dmax):
        st.error("Could not parse Date values for backtest.")
        return

    default_start = (dmax - pd.Timedelta(days=90)).date() if (dmax - dmin).days > 120 else dmin.date()
    start_date, end_date = st.date_input(
        "Test date range (inclusive)",
        value=(default_start, dmax.date()),
        min_value=dmin.date(),
        max_value=dmax.date(),
        key="bt_date_range",
    )

    start_dt = pd.Timestamp(start_date)
    end_dt = pd.Timestamp(end_date)

    only_hit_days = st.checkbox(
        "Winner-only events/export (faster)",
        value=True,
        key="bt_only_hit_days",
        help="Checked: test only dates with a selected-core winner and export winner rows. Unchecked: test every date and also export the complete Date × Core × Stream chart universe needed for cross-core chart comparisons.",
    )
    st.markdown("##### v51.26 multi-chart audit")
    mc1, mc2 = st.columns([1.4, 1.6])
    with mc1:
        multi_chart_audit = st.checkbox(
            "Compare winner across v51.26 charts",
            value=True,
            key="bt_multi_chart_audit",
            help="Builds the v51.26 chart universe for each test date. Winner-only export is controlled by the checkbox above.",
        )
    with mc2:
        include_final_boxed = st.checkbox(
            "Include Final Boxed Plays chart",
            value=True,
            key="bt_include_final_boxed",
            disabled=not multi_chart_audit,
            help="Small downstream add-on after Northern Lights. It is composite, not an independent signal.",
        )
    if only_hit_days:
        st.caption("Winner-only mode: saves winner locations and summary files.")
    else:
        st.caption("Full-universe mode: saves every Date × Core × Stream ranking row plus winner labels for cross-core comparison. This is larger and slower.")
    base_due_gate_mode = st.selectbox(
        "Base/Due gate mode",
        options=list(WF_GATE_MODES),
        index=list(WF_GATE_MODES).index(DAILY_BASE_DUE_GATE_MODE),
        key="bt_base_due_gate_mode",
        help=(
            "OPEN_GATE_FEATURE_ONLY is the Daily production engine and the WF default. "
            "The hard and flipped modes are comparison-only experiments."
        ),
    )
    if base_due_gate_mode == DAILY_BASE_DUE_GATE_MODE:
        st.success("PRODUCTION-EQUIVALENT WF: defaults match Daily exactly (OPEN_GATE_FEATURE_ONLY).")
    else:
        st.warning(f"EXPERIMENTAL WF BRANCH: {base_due_gate_mode}. This does not match Daily production.")
    st.number_input("Parallel core workers", min_value=1, max_value=max(1, min(8, os.cpu_count() or 2)), value=max(1, min(4, os.cpu_count() or 2)), step=1, key="bt_workers", help="Runs independent core charts concurrently while preserving the exact production formulas.")

    run = st.button(
        "Run walk-forward backtest",
        key="bt_run_btn",
        disabled=not exact_working8,
        help="Enabled only after the exact Working 8 core contract passes.",
    )
    if not exact_working8:
        st.warning("Backtest disabled: exact Working 8 is required for this build.")
        return
    if not run:
        st.info("Click **Run walk-forward backtest** to generate results.")
        return

    # Fast, semantics-preserving range preparation. Legacy matrix builders remain unchanged.
    _wf_prepare_engine = "PANDAS_FALLBACK"
    if pl is not None:
        try:
            _pl_df = pl.from_pandas(df_all, include_index=False)
            _pl_df = _pl_df.filter(
                pl.col("Date").is_not_null()
                & (pl.col("Date") >= pl.lit(start_dt.to_pydatetime()))
                & (pl.col("Date") <= pl.lit(end_dt.to_pydatetime()))
            )
            df = _pl_df.to_pandas()
            _wf_prepare_engine = "POLARS"
        except Exception:
            df = df_all.copy()
            df = df[df["Date"].notna()]
            df = df[(df["Date"] >= start_dt) & (df["Date"] <= end_dt)]
    else:
        df = df_all.copy()
        df = df[df["Date"].notna()]
        df = df[(df["Date"] >= start_dt) & (df["Date"] <= end_dt)]
    if df.empty:
        st.warning("No rows in the selected date range.")
        return

    # Build member->core reverse index
    member_to_cores: dict[str, list[str]] = {}
    for core in cores_sel:
        members = []
        # Always include the core's main family members (AABC/ABBC/ABCC)
        members.extend(members_from_core(core, "AABC"))
        # Optionally include higher-rarity structures for the same core
        if include_rare:
            members.extend(members_from_core(core, "AAAB"))
            members.extend(members_from_core(core, "AABB"))
            members.extend(members_from_core(core, "AAAA"))
        for mem in members:
            member_to_cores.setdefault(box_key(mem), []).append(core)

    # Winners by date. DuckDB performs the stable date ordering when available;
    # the legacy Result/Stream rows themselves are not transformed.
    _wf_lookup_engine = "PANDAS_FALLBACK"
    _wf_group_source = df
    if duckdb is not None:
        try:
            _wf_group_source = duckdb.sql(
                "SELECT * FROM df ORDER BY Date, Stream"
            ).df()
            _wf_lookup_engine = "DUCKDB"
        except Exception:
            _wf_group_source = df
    winners_by_date = {d: g for d, g in _wf_group_source.groupby(pd.to_datetime(_wf_group_source["Date"], errors="coerce").dt.normalize())}
    st.caption(f"WF acceleration — history preparation: {_wf_prepare_engine}; date/winner lookup: {_wf_lookup_engine}; legacy matrix scoring: ORIGINAL FUNCTIONS; settled rules: SHARED ENGINE.")

    # Determine dates to evaluate
    all_dates_sorted = sorted(winners_by_date.keys())
    if only_hit_days:
        candidate_dates = []
        for d in all_dates_sorted:
            g = winners_by_date[d]
            hit = False
            for w in g["Result"].astype(str).tolist():
                if member_to_cores.get(box_key(w.strip()), None):
                    hit = True
                    break
            if hit:
                candidate_dates.append(d)
        dates_to_test = candidate_dates
    else:
        dates_to_test = all_dates_sorted

    if not dates_to_test:
        st.warning("No hit days found for the selected cores in this date range.")
        return

    if len(dates_to_test) > int(max_dates):
        dates_to_test = dates_to_test[-int(max_dates):]

    # Cache per (core, as_of_date, window_days) within this run
    per_core_cache: dict[tuple[str, pd.Timestamp, int], pd.DataFrame] = {}
    per_core_buckets: dict[tuple[str, pd.Timestamp, int], dict] = {}

    # Member-pick prediction cache for this run: (core, test_date, window_days, basis, stream) -> dict
    member_pred_cache: dict[tuple[str, pd.Timestamp, int, str, str | None], dict[str, Any]] = {}

    rows = []
    full_universe_rows: list[pd.DataFrame] = []
    wf_member_all_rows: list[pd.DataFrame] = []
    wf_member_survivor_rows: list[pd.DataFrame] = []
    wf_member_fire_rows: list[pd.DataFrame] = []
    wf_core_all_rows: list[pd.DataFrame] = []
    wf_core_survivor_rows: list[pd.DataFrame] = []
    wf_core_fire_rows: list[pd.DataFrame] = []
    wf_stage_summary_rows: list[pd.DataFrame] = []
    wf_handshake_rows: list[pd.DataFrame] = []
    runtime_rows: list[dict[str, Any]] = []
    # Sort once and use binary-search date boundaries instead of rescanning the full history per test date.
    hist_sorted = df_all.sort_values("Date", kind="mergesort").reset_index(drop=True)
    hist_dates = pd.to_datetime(hist_sorted["Date"], errors="coerce").values.astype("datetime64[ns]")
    bt_progress = st.progress(
        0,
        text=("Preparing winner-only walk-forward audit..." if only_hit_days else "Preparing full chart-universe walk-forward audit..."),
    )
    run_started = time.perf_counter()
    for date_i, test_date in enumerate(dates_to_test, start=1):
        date_started = time.perf_counter()
        cutoff = int(np.searchsorted(hist_dates, np.datetime64(pd.Timestamp(test_date)), side="left"))
        train_df = hist_sorted.iloc[:cutoff]
        if train_df.empty:
            continue

        day_winners = winners_by_date.get(test_date)
        if day_winners is None or day_winners.empty:
            continue

        chart_u, chart_b, stage_times, chart_settled = pd.DataFrame(), pd.DataFrame(), {}, None
        if multi_chart_audit:
            try:
                chart_u, chart_b, stage_times, chart_settled = _wf_build_v5126_chart_universe(
                    train_df=train_df, cfg=cfg, cores_sel=cores_sel,
                    include_final_boxed=bool(include_final_boxed),
                    base_due_gate_mode=str(base_due_gate_mode),
                )
            except Exception as exc:
                import traceback as _traceback
                _wf_trace = _traceback.format_exc()
                _wf_err_dir = Path.cwd() / "OUTPUTS"
                _wf_err_dir.mkdir(parents=True, exist_ok=True)
                (_wf_err_dir / "WF_LAST_ERROR_TRACEBACK.txt").write_text(_wf_trace, encoding="utf-8")
                raise RuntimeError(f"WF chart build failed on {test_date.date()}: {exc}") from exc
        # Preserve the complete shared settled ledgers for every tested date. These are
        # the proof that core/member calculations ran, not merely summary counts.
        if chart_settled is not None:
            _ledger_specs = [
                (chart_settled.member_all, wf_member_all_rows),
                (chart_settled.member_survivors, wf_member_survivor_rows),
                (chart_settled.member_fire_audit, wf_member_fire_rows),
                (chart_settled.core_all, wf_core_all_rows),
                (chart_settled.core_survivors, wf_core_survivor_rows),
                (chart_settled.core_fire_audit, wf_core_fire_rows),
                (chart_settled.stage_summary, wf_stage_summary_rows),
                (chart_settled.handshake, wf_handshake_rows),
            ]
            for _ledger, _target in _ledger_specs:
                if isinstance(_ledger, pd.DataFrame) and not _ledger.empty:
                    _copy = _ledger.copy()
                    _copy.insert(0, "WFTestDate", test_date.date())
                    _target.append(_copy)
        # When winner-only mode is OFF, preserve the complete per-date chart universe.
        # Label each Date × Stream × Core row against the actual winner(s) for that stream/date.
        if (not only_hit_days) and multi_chart_audit and chart_u is not None and not chart_u.empty:
            _fu = chart_u.copy()
            _fu.insert(0, "Date", test_date.date())
            _fu["AsOfMaxDate"] = pd.to_datetime(train_df["Date"], errors="coerce").max().date() if "Date" in train_df.columns else None
            _actual_by_stream: dict[str, list[tuple[str, str]]] = {}
            for _, _awr in day_winners.iterrows():
                _astream = str(_awr.get("Stream", "")).strip()
                _aresult = str(_awr.get("Result", "")).strip()
                _acores = member_to_cores.get(box_key(_aresult), [])
                for _acore in _acores:
                    _actual_by_stream.setdefault(_astream, []).append((str(_acore), _aresult))
            _fu["ActualWinningCores"] = _fu["Stream"].astype(str).map(
                lambda x: "|".join(sorted({c for c, _ in _actual_by_stream.get(str(x), [])}))
            )
            _fu["ActualWinnerResults"] = _fu["Stream"].astype(str).map(
                lambda x: "|".join(sorted({r for _, r in _actual_by_stream.get(str(x), [])}))
            )
            _fu["IsActualWinningCoreForStream"] = [
                str(c) in {wc for wc, _ in _actual_by_stream.get(str(stm), [])}
                for c, stm in zip(_fu["Core"].astype(str), _fu["Stream"].astype(str))
            ]
            if "RankPos" in _fu.columns:
                _fu["StreamTop50ForCore"] = pd.to_numeric(_fu["RankPos"], errors="coerce").le(50)
                _fu["BestCoreRankForStream"] = _fu.groupby("Stream")["RankPos"].transform("min")
                _fu["IsHighestRankingCoreForStream"] = pd.to_numeric(_fu["RankPos"], errors="coerce").eq(
                    pd.to_numeric(_fu["BestCoreRankForStream"], errors="coerce")
                )
            full_universe_rows.append(_fu)
        bt_progress.progress(
            min(1.0, date_i / max(1, len(dates_to_test))),
            text=f"Date {date_i}/{len(dates_to_test)} — {test_date.date()}",
        )

        for _, wr in day_winners.iterrows():
            stream = str(wr.get("Stream", "")).strip()
            winner = str(wr.get("Result", "")).strip()
            wk = box_key(winner)
            hit_cores = member_to_cores.get(wk, [])
            if not hit_cores:
                continue

            for core in hit_cores:
                # Reuse the already-built v51.26 chart row. Do not recompute this core a second time.
                stat_row = None
                if multi_chart_audit and chart_u is not None and not chart_u.empty:
                    _um0 = chart_u[(chart_u["Core"].astype(str) == str(core)) & (chart_u["Stream"].astype(str) == stream)]
                    if not _um0.empty:
                        stat_row = _um0.iloc[0]
                if stat_row is not None:
                    bucket = str(stat_row.get("NorthernStarBucket", "None"))
                    predicted = bool(stat_row.get("ProductionEligible", False))
                else:
                    key = (core, test_date, int(cfg.window_days))
                    if key not in per_core_cache:
                        stats_df = compute_stream_stats(train_df, core, window_days=int(cfg.window_days))
                        per_core_cache[key] = stats_df
                        per_core_buckets[key] = bucket_recommendations(stats_df, cfg)
                    stats_df = per_core_cache[key]
                    buckets = per_core_buckets[key]
                    base_streams = set(buckets["Top12BaseScore"]["Stream"].astype(str).tolist()) if "Top12BaseScore" in buckets else set()
                    due_streams = set(buckets["Due8"]["Stream"].astype(str).tolist()) if "Due8" in buckets else set()
                    predicted = stream in base_streams or stream in due_streams
                    bucket = "Both" if (stream in base_streams and stream in due_streams) else ("BaseScore" if stream in base_streams else ("Due8" if stream in due_streams else "None"))
                    try:
                        stat_row = stats_df.loc[stats_df["Stream"].astype(str) == stream].iloc[0]
                    except Exception:
                        stat_row = None



                # Member labels + walk-forward member-pick prediction (family only: AABC/ABBC/ABCC)

                actual_member_label = core_member_label(core, winner, include_rare=bool(include_rare)) if member_track else None

                actual_family_member = actual_member_label if actual_member_label in ("AABC", "ABBC", "ABCC") else None


                pred_member_top1 = None

                pred_member_top2 = None

                pred_member_n = 0

                train_cnt_aabc = 0

                train_cnt_abbc = 0

                train_cnt_abcc = 0

                member_hit_top1 = None

                member_hit_top2 = None


                if member_track:

                    mk = (

                        str(core),

                        pd.to_datetime(test_date).normalize(),

                        int(cfg.window_days),

                        str(member_basis),

                        (str(stream) if member_basis == "core_stream" else None),

                    )

                    if mk not in member_pred_cache:

                        member_pred_cache[mk] = predict_core_member(

                            df_all,

                            core,

                            pd.to_datetime(test_date).normalize(),

                            window_days=int(cfg.window_days),

                            basis=str(member_basis),

                            stream=(str(stream) if member_basis == "core_stream" else None),

                            include_rare=False,  # compare only AABC/ABBC/ABCC

                        )

                    mp = member_pred_cache.get(mk, {})

                    pred_member_top1 = mp.get("top1")

                    pred_member_top2 = mp.get("top2")

                    pred_member_n = int(mp.get("n") or 0)

                    cnts = mp.get("counts") or {}

                    train_cnt_aabc = int(cnts.get("AABC") or 0)

                    train_cnt_abbc = int(cnts.get("ABBC") or 0)

                    train_cnt_abcc = int(cnts.get("ABCC") or 0)


                    if actual_family_member and pred_member_top1:

                        member_hit_top1 = (actual_family_member == pred_member_top1)

                        member_hit_top2 = (actual_family_member == pred_member_top1) or (pred_member_top2 is not None and actual_family_member == pred_member_top2)

                # Winner location across actual v51.26 ranking stages (winner rows only are retained).
                nl_rank = None
                nl_pct = None
                nl_score = None
                nl_eligible = None
                original_base_due_eligible = None
                eligible_rank = None
                candidate_universe_rows = int(len(chart_u)) if chart_u is not None else 0
                eligible_universe_rows = int(chart_u["ProductionEligible"].sum()) if chart_u is not None and (not chart_u.empty) and "ProductionEligible" in chart_u.columns else 0
                final_boxed_universe_rows = int(len(chart_b)) if chart_b is not None else 0
                final_playlist_selected_boxed_plays = int(chart_b.get("FinalPlaylistIncluded", pd.Series(dtype=bool)).fillna(False).sum()) if chart_b is not None and not chart_b.empty else 0
                final_boxed_rank = None
                final_boxed_tie_rank = None
                final_member_retained = None
                final_member_reason = None
                final_member_rule_ids = None
                final_playlist_included = None
                final_playlist_boxed_rank = None
                final_playlist_bundle_rank = None
                final_playlist_selection_reason = None
                failure_stage = None
                if multi_chart_audit:
                    
                    if chart_u is not None and not chart_u.empty:
                        um = chart_u[(chart_u["Core"].astype(str) == str(core)) & (chart_u["Stream"].astype(str) == stream)]
                        if not um.empty:
                            ur = um.iloc[0]
                            nl_rank = int(ur.get("NorthernLightsRank")) if pd.notna(ur.get("NorthernLightsRank")) else None
                            nl_pct = float(ur.get("NorthernLightsPercentile")) if pd.notna(ur.get("NorthernLightsPercentile")) else None
                            nl_score = float(ur.get("UniversalScore")) if pd.notna(ur.get("UniversalScore")) else None
                            nl_eligible = _wf_scalar_bool(ur.get("ProductionEligible", False), False)
                            original_base_due_eligible = _wf_scalar_bool(ur.get("OriginalBaseDueEligible", nl_eligible), nl_eligible)
                            eligible_rank = int(ur.get("GlobalEligibleStreamCoreRank")) if pd.notna(ur.get("GlobalEligibleStreamCoreRank")) else None
                            if not nl_eligible:
                                if str(base_due_gate_mode).upper() == "AUDIT_FLIPPED_GATE" and original_base_due_eligible:
                                    failure_stage = "STREAM_WAS_BASE_DUE_FLIPPED_OUT"
                                else:
                                    failure_stage = "STREAM_NOT_BASE_DUE"
                    if include_final_boxed and chart_b is not None and not chart_b.empty:
                        bm = chart_b[(chart_b["Core"].astype(str) == str(core)) & (chart_b["Stream"].astype(str) == stream)].copy()
                        if not bm.empty:
                            bm["_BoxKey"] = bm.get("BoxedMember", "").astype(str).map(box_key)
                            win_bm = bm[bm["_BoxKey"] == wk]
                            final_member_retained = not win_bm.empty
                            source_row = win_bm.iloc[0] if not win_bm.empty else bm.iloc[0]
                            final_member_reason = source_row.get("MemberDecisionReason")
                            final_member_rule_ids = source_row.get("MemberRulesFired")
                            if not win_bm.empty:
                                wr = win_bm.iloc[0]
                                final_boxed_rank = int(wr.get("GlobalBoxedPlayRank")) if pd.notna(wr.get("GlobalBoxedPlayRank")) else None
                                final_boxed_tie_rank = int(wr.get("BoxedScoreTieRank")) if pd.notna(wr.get("BoxedScoreTieRank")) else None
                                final_playlist_included = _wf_scalar_bool(wr.get("FinalPlaylistIncluded", False), False)
                                final_playlist_boxed_rank = int(wr.get("FinalPlaylistBoxedRank")) if pd.notna(wr.get("FinalPlaylistBoxedRank")) else None
                                final_playlist_bundle_rank = int(wr.get("FinalPlaylistBundleRank")) if pd.notna(wr.get("FinalPlaylistBundleRank")) else None
                                final_playlist_selection_reason = wr.get("FinalPlaylistSelectionReason")
                                if not final_playlist_included and failure_stage is None:
                                    failure_stage = "WINNING_MEMBER_OUTSIDE_FINAL_50_BUDGET"
                            else:
                                final_playlist_included = False
                                if failure_stage is None:
                                    failure_stage = "WINNING_MEMBER_REMOVED"
                        elif nl_eligible:
                            final_member_retained = False
                            final_playlist_included = False
                            if failure_stage is None:
                                failure_stage = "NO_BOXED_OUTPUT"
                    if failure_stage is None and nl_eligible:
                        gate_mode_now = str(base_due_gate_mode).upper()
                        if gate_mode_now == "AUDIT_OPEN_GATE" and (original_base_due_eligible is False):
                            failure_stage = "RESCUED_FROM_BASE_DUE_GATE"
                        elif gate_mode_now == "AUDIT_FLIPPED_GATE" and (original_base_due_eligible is False):
                            failure_stage = "SURVIVED_FLIPPED_GATE"
                        else:
                            failure_stage = "SURVIVED_V5126"

                rows.append({
                    "Date": test_date.date(),
                    "Stream": stream,
                    "Winner": winner,
                    "Core": core,
                    "Predicted": bool(predicted),
                    "Bucket": bucket,
                    "RankPos": (int(stat_row["RankPos"]) if (stat_row is not None and "RankPos" in stat_row and pd.notna(stat_row["RankPos"])) else None),
                    "BaseScoreRank": (int(stat_row["BaseScoreRank"]) if (stat_row is not None and "BaseScoreRank" in stat_row and pd.notna(stat_row["BaseScoreRank"])) else None),
                    "HitsWindow": (int(stat_row["HitsWindow"]) if (stat_row is not None and "HitsWindow" in stat_row and pd.notna(stat_row["HitsWindow"])) else None),
                    "DaysSinceLastHit": (int(stat_row["DaysSinceLastHit"]) if (stat_row is not None and "DaysSinceLastHit" in stat_row and pd.notna(stat_row["DaysSinceLastHit"])) else None),
                    "AsOfMaxDate": pd.to_datetime(train_df["Date"], errors="coerce").max().date() if "Date" in train_df.columns else None,
                    "ActualMemberLabel": actual_member_label,
                    "ActualFamilyMember": actual_family_member,
                    "PredMemberTop1": pred_member_top1,
                    "PredMemberTop2": pred_member_top2,
                    "MemberHitTop1": member_hit_top1,
                    "MemberHitTop2": member_hit_top2,
                    "MemberTrainN": pred_member_n,
                    "TrainCnt_AABC": train_cnt_aabc,
                    "TrainCnt_ABBC": train_cnt_abbc,
                    "TrainCnt_ABCC": train_cnt_abcc,
                    "NorthernLightsRank": nl_rank,
                    "NorthernLightsPercentile": nl_pct,
                    "NorthernLightsUniversalScore": nl_score,
                    "BaseDueGateMode": str(base_due_gate_mode),
                    "OriginalBaseDueEligible": original_base_due_eligible,
                    "ProductionEligible": nl_eligible,
                    "GlobalEligibleStreamCoreRank": eligible_rank,
                    "CandidateUniverseRows": candidate_universe_rows,
                    "EligibleUniverseRows": eligible_universe_rows,
                    "FinalBoxedUniverseRows": final_boxed_universe_rows,
                    "FinalBoxedWinningMemberRank": final_boxed_rank,
                    "FinalBoxedTop40Included": (bool(final_boxed_rank <= 40) if final_boxed_rank is not None else False),
                    "FinalBoxedTop50Included": (bool(final_boxed_rank <= 50) if final_boxed_rank is not None else False),
                    "FinalBoxedScoreTieRank": final_boxed_tie_rank,
                    "WinningMemberRetained": final_member_retained,
                    "FinalMemberDecisionReason": final_member_reason,
                    "FinalMemberRuleIDs": final_member_rule_ids,
                    "FinalPlaylistIncluded": final_playlist_included,
                    "FinalPlaylistBoxedRank": final_playlist_boxed_rank,
                    "FinalPlaylistBundleRank": final_playlist_bundle_rank,
                    "FinalPlaylistSelectionReason": final_playlist_selection_reason,
                    "FinalPlaylistBudget": 50,
                    "FinalPlaylistSelectedBoxedPlays": final_playlist_selected_boxed_plays,
                    "FailureStage": failure_stage,
                })

        runtime_rows.append({
            "Date": test_date.date(),
            "TotalSeconds": round(time.perf_counter() - date_started, 4),
            **{k: round(float(v), 4) for k, v in (stage_times or {}).items()},
        })

    bt_progress.empty()
    total_runtime_s = time.perf_counter() - run_started
    if not rows:
        diagnostic = pd.DataFrame([{
            "Status": "NO_MATCHING_WINNER_ROWS",
            "SelectedCores": ",".join(cores_sel),
            "StartDate": str(start_date),
            "EndDate": str(end_date),
            "DatesTested": len(dates_to_test),
            "HistoryRows": len(df_all),
            "CandidateWinnerRows": sum(len(winners_by_date.get(d, [])) for d in dates_to_test),
            "MemberMapKeys": len(member_to_cores),
        }])
        st.error("The run completed but found no matching selected-core winners. Download the diagnostic instead of receiving a blank result.")
        _safe_st_dataframe(diagnostic, use_container_width=True, hide_index=True)
        st.download_button("Download no-output diagnostic", diagnostic.to_csv(index=False).encode("utf-8"), "BACKTEST_NO_OUTPUT_DIAGNOSTIC.csv", "text/csv", key="bt_no_output_diag")
        return

    out = pd.DataFrame(rows).sort_values(["Date", "Core", "Predicted"], ascending=[True, True, False])
    full_universe_out = _safe_pd_concat(full_universe_rows, ignore_index=True) if full_universe_rows else pd.DataFrame()
    wf_member_all_out = _safe_pd_concat(wf_member_all_rows, ignore_index=True) if wf_member_all_rows else pd.DataFrame()
    wf_member_survivors_out = _safe_pd_concat(wf_member_survivor_rows, ignore_index=True) if wf_member_survivor_rows else pd.DataFrame()
    wf_member_fire_out = _safe_pd_concat(wf_member_fire_rows, ignore_index=True) if wf_member_fire_rows else pd.DataFrame()
    wf_core_all_out = _safe_pd_concat(wf_core_all_rows, ignore_index=True) if wf_core_all_rows else pd.DataFrame()
    wf_core_survivors_out = _safe_pd_concat(wf_core_survivor_rows, ignore_index=True) if wf_core_survivor_rows else pd.DataFrame()
    wf_core_fire_out = _safe_pd_concat(wf_core_fire_rows, ignore_index=True) if wf_core_fire_rows else pd.DataFrame()
    wf_stage_summary_out = _safe_pd_concat(wf_stage_summary_rows, ignore_index=True) if wf_stage_summary_rows else pd.DataFrame()
    wf_handshake_out = _safe_pd_concat(wf_handshake_rows, ignore_index=True) if wf_handshake_rows else pd.DataFrame()
    if not full_universe_out.empty:
        _fu_sort = [c for c in ["Date", "Stream", "RankPos", "Core"] if c in full_universe_out.columns]
        full_universe_out = full_universe_out.sort_values(_fu_sort, kind="mergesort").reset_index(drop=True)
    # Persist latest backtest output for other panels (e.g., Auto Profit Planner)
    st.session_state["wf_backtest_out"] = out.copy()

    # GUARANTEED OUTPUT FIRST: render, persist, and offer downloads before any optional analysis.
    runtime_df = pd.DataFrame(runtime_rows)
    _quick_core = out.groupby("Core").agg(Total=("Predicted", "size"), Predicted=("Predicted", "sum")).reset_index()
    _quick_stream = out.groupby("Stream").agg(Total=("Predicted", "size"), Predicted=("Predicted", "sum")).reset_index()
    _quick_date = out.groupby("Date").agg(
        WinnerRows=("Core", "size"),
        OriginalGateWinners=("OriginalBaseDueEligible", "sum"),
        Top40WinnerRows=("FinalBoxedTop40Included", "sum"),
        Top50WinnerRows=("FinalBoxedTop50Included", "sum"),
        FinalPlaylistWinnerRows=("FinalPlaylistIncluded", "sum"),
        FinalPlaylistSelectedBoxedPlays=("FinalPlaylistSelectedBoxedPlays", "max"),
        CandidateUniverseRows=("CandidateUniverseRows", "max"),
        EligibleUniverseRows=("EligibleUniverseRows", "max"),
        FinalBoxedUniverseRows=("FinalBoxedUniverseRows", "max"),
    ).reset_index()
    _guaranteed_files = {
        "walkforward_winner_rows.csv": out,
        "summary_by_date.csv": _quick_date,
        "summary_by_core.csv": _quick_core,
        "summary_by_stream.csv": _quick_stream,
        "runtime_by_date.csv": runtime_df,
        "BUILD_INFO.txt": (
            f"BUILD: {APP_VERSION}\n"
            f"CORE CONTRACT: {'PASS' if list(cores_sel) == list(WORKING8_CORE_SET) else 'FAIL'}\n"
            f"CORE COUNT: {len(cores_sel)}\n"
            f"EXPECTED WORKING 8: {', '.join(WORKING8_CORE_SET)}\n"
            f"SELECTED CORES: {', '.join(cores_sel)}\n"
            f"MISSING CORES: {', '.join([c for c in WORKING8_CORE_SET if c not in cores_sel]) or 'none'}\n"
            f"BASE/DUE GATE MODE: {base_due_gate_mode}\nDAILY_WF_PARITY: LOCKED_SHARED_PIPELINE\n"
        ),
    }
    _wf_ledger_exports = {
        "wf_core_all.csv": wf_core_all_out,
        "wf_core_survivors.csv": wf_core_survivors_out,
        "wf_core_rule_fire.csv": wf_core_fire_out,
        "wf_member_all.csv": wf_member_all_out,
        "wf_member_survivors.csv": wf_member_survivors_out,
        "wf_member35_rule_fire.csv": wf_member_fire_out,
        "wf_stage_summary.csv": wf_stage_summary_out,
        "wf_contract_handshake.csv": wf_handshake_out,
    }
    for _name, _frame in _wf_ledger_exports.items():
        if isinstance(_frame, pd.DataFrame) and not _frame.empty:
            _guaranteed_files[_name] = _frame
    _guaranteed_files["WF_LEDGER_README.txt"] = (
        "The WF ZIP includes complete core and member ledgers for every tested date.\n"
        "wf_member_all.csv contains all three expanded members before/after Member35 status.\n"
        "wf_member_survivors.csv is the final boxed-member survivor universe.\n"
        "wf_member35_rule_fire.csv records every matched Member35 rule.\n"
        "FinalPlaylistSelectedBoxedPlays is the count of rows carried into the playable WF playlist.\n"
    )
    if not full_universe_out.empty:
        _guaranteed_files["walkforward_chart_universe.csv"] = full_universe_out
        _guaranteed_files["FULL_UNIVERSE_README.txt"] = (
            "One row per Date × Core × Stream. Use StreamTop50ForCore, BestCoreRankForStream, "
            "IsHighestRankingCoreForStream, and IsActualWinningCoreForStream to answer cross-core ranking questions.\n"
        )
    _guaranteed_zip = _build_outputs_zip(_guaranteed_files)
    _out_dir = Path.cwd() / "OUTPUTS"
    _out_dir.mkdir(parents=True, exist_ok=True)
    _tag = f"{pd.Timestamp(start_date).strftime('%Y%m%d')}_{pd.Timestamp(end_date).strftime('%Y%m%d')}"
    _csv_path = _out_dir / f"WINNER_CHART_AUDIT_{_tag}_v5145H.csv"
    _zip_path = _out_dir / f"WINNER_CHART_AUDIT_{_tag}_v5145H.zip"
    try:
        out.to_csv(_csv_path, index=False)
        _zip_path.write_bytes(_guaranteed_zip)
    except Exception as _save_exc:
        st.warning(f"Results are available below, but automatic OUTPUTS-folder saving failed: {_save_exc}")

    st.success(
        f"BACKTEST COMPLETE — {len(out)} winner rows generated"
        + (f" and {len(full_universe_out):,} full-universe rows exported." if not full_universe_out.empty else ".")
    )
    _safe_st_dataframe(out, use_container_width=True, hide_index=True)
    _dl1, _dl2 = st.columns(2)
    with _dl1:
        st.download_button("Download winner rows CSV", out.to_csv(index=False).encode("utf-8"), f"WINNER_CHART_AUDIT_{_tag}_v5145H.csv", "text/csv", key="bt_guaranteed_csv", use_container_width=True)
    with _dl2:
        st.download_button("Download complete Backtest ZIP", _guaranteed_zip, f"WINNER_CHART_AUDIT_{_tag}_v5145H.zip", "application/zip", key="bt_guaranteed_zip", use_container_width=True)
    if not full_universe_out.empty:
        st.download_button(
            "Download full chart universe CSV",
            full_universe_out.to_csv(index=False).encode("utf-8"),
            f"WALKFORWARD_CHART_UNIVERSE_{_tag}_v5145H.csv",
            "text/csv",
            key="bt_full_universe_csv",
            use_container_width=True,
        )
    st.caption(f"Also saved to: {_out_dir}")

    # Summary
    total = len(out)
    hits = int(out["Predicted"].sum())
    st.success(f"Evaluated {total} core-family wins; predicted {hits} ({(hits/total*100):.1f}%).")
    runtime_df = pd.DataFrame(runtime_rows)
    st.caption(f"Runtime: {total_runtime_s:.1f} seconds total; {total_runtime_s/max(1,len(dates_to_test)):.1f} seconds per tested date.")
    if not runtime_df.empty:
        with st.expander("Runtime by date and stage", expanded=False):
            _safe_st_dataframe(runtime_df, use_container_width=True, hide_index=True)

    if multi_chart_audit:
        st.markdown("#### Winner location across v51.26 charts")
        _mc_cols = [
            "Date", "Stream", "Core", "Winner", "RankPos", "BaseScoreRank", "Bucket",
            "NorthernLightsRank", "NorthernLightsPercentile", "NorthernLightsUniversalScore",
            "BaseDueGateMode", "OriginalBaseDueEligible", "ProductionEligible", "GlobalEligibleStreamCoreRank",
            "CandidateUniverseRows", "EligibleUniverseRows", "FinalBoxedUniverseRows",
            "FinalBoxedWinningMemberRank", "FinalBoxedScoreTieRank", "FinalBoxedTop40Included", "FinalBoxedTop50Included",
            "WinningMemberRetained", "FinalPlaylistIncluded", "FinalPlaylistBoxedRank",
            "FinalPlaylistBundleRank", "FinalPlaylistSelectionReason", "FinalPlaylistBudget",
            "FinalPlaylistSelectedBoxedPlays", "FailureStage",
        ]
        _mc_cols = [c for c in _mc_cols if c in out.columns]
        _safe_st_dataframe(out[_mc_cols], use_container_width=True, hide_index=True)
        st.caption("Final Box repaired in v51.33: Global Final Box rank is computed after the selected gate; FinalPlaylistIncluded now means the winner's complete member bundle was actually selected inside the 50-box production budget. Presence anywhere in the Final Box universe no longer counts as playlist inclusion.")

    # Optional: member-pick accuracy (Top1/Top2) for family members (AABC/ABBC/ABCC)
    if member_track and (not out.empty):
        st.markdown("#### Member pick accuracy (Top1/Top2)")
        st.caption("These stats answer: when a core hit, was the *predicted* family member the *actual* family member? (Top1 = exact pick; Top2 = in top-2 picks).")
        need_cols = ["ActualFamilyMember","PredMemberTop1","PredMemberTop2","MemberHitTop1","MemberHitTop2","MemberTrainN"]
        missing = [c for c in need_cols if c not in out.columns]
        if missing:
            st.warning(f"Member columns missing from output: {missing}. (This should not happen; please re-run.)")
        else:
            member_df = out.dropna(subset=["ActualFamilyMember"]).copy()
            if member_df.empty:
                st.info("No family-member hits in this test window (nothing to score for member accuracy).")
            else:
                agg = member_df.groupby("Core", dropna=False).agg(
                    N=("Core","size"),
                    Top1Hit=("MemberHitTop1","sum"),
                    Top2Hit=("MemberHitTop2","sum"),
                    AvgTrainN=("MemberTrainN","mean"),
                    MedTrainN=("MemberTrainN","median"),
                ).reset_index()
                agg["Top1Rate"] = (agg["Top1Hit"] / agg["N"]).round(4)
                agg["Top2Rate"] = (agg["Top2Hit"] / agg["N"]).round(4)
                _safe_st_dataframe(agg.sort_values(["Top2Rate","Top1Rate","N"], ascending=False), use_container_width=True)
                st.caption("Tip: if Top2Rate is strong but Top1Rate is weak, treat this as a *top-2 member shortlist* (play 2 members, not 1).")

    # Trust check: walk-forward (no leakage)
    leak_ok = True
    if (not out.empty) and ("AsOfMaxDate" in out.columns):
        try:
            _max_train = pd.to_datetime(out["AsOfMaxDate"], errors="coerce")
            _test = pd.to_datetime(out["Date"], errors="coerce")
            leak_ok = bool((_max_train <= (_test - pd.Timedelta(days=1))).fillna(True).all())
        except Exception:
            leak_ok = True
    st.caption("Leakage check: " + ("✅ OK" if leak_ok else "❌ FAILED") + " — AsOfMaxDate should be <= test_date-1 for all rows.")

    # -------------------------
    # Strategy Finder (rows/lines)
    # -------------------------
    st.markdown("#### Strategy Finder (minimize plays)")
    st.caption(
        "Goal: find the *specific row lines* where winners concentrate most, so you can play fewer rows per core while keeping as many winners as possible."
    )

    if only_hit_days:
        st.info(
            "You have **Evaluate only days where a selected core member hit** enabled. "
            "Strategy metrics below are computed on those *hit-days only* (faster, but it can inflate day-hit rates). "
            "For true daily rates across the whole date range, re-run with that box unchecked."
        )

    # Controls
    sf1, sf2, sf3 = st.columns([1.2, 1.2, 1.2])
    with sf1:
        rank_choice = st.selectbox(
            "Which chart rows?",
            ["RankPos (overall stream position)", "BaseScoreRank (base score chart position)"],
            index=0,
            key="sf_rank_choice",
            help="RankPos is the overall stream position from the per-core stream ranking. BaseScoreRank is the rank on the BaseScore chart.",
        )
    rank_col = "RankPos" if rank_choice.startswith("RankPos") else "BaseScoreRank"

    with sf2:
        cost_per_play = st.number_input(
            "Cost per play ($)",
            min_value=0.0,
            max_value=10.0,
            value=0.25,
            step=0.05,
            key="sf_cost_per_play",
        )
    with sf3:
        member_mode = st.selectbox(
            "Member play mode (affects plays + scoring)",
            [
                "Play all 3 family members (AABC+ABBC+ABCC)",
                "Play Top2 member picks (requires tracking)",
                "Play Top1 member pick (requires tracking)",
            ],
            index=0,
            key="sf_member_mode",
            help="All-3 counts a win whenever the core hit in that stream and you played the stream. Top2/Top1 count wins only if the predicted member(s) match the actual member.",
        )

    # Determine member multiplier + scoring filter
    member_mult = 3
    member_filter_col = None
    if member_mode.startswith("Play Top2"):
        member_mult = 2
        member_filter_col = "MemberHitTop2"
    elif member_mode.startswith("Play Top1"):
        member_mult = 1
        member_filter_col = "MemberHitTop1"

    # If member mode selected but tracking not available, fall back safely
    if member_filter_col is not None:
        if (not member_track) or (member_filter_col not in out.columns):
            st.warning("Top1/Top2 scoring requires **Track member accuracy**. Falling back to **Play all 3** for Strategy Finder.")
            member_mult = 3
            member_filter_col = None

    # Prepare rank dataframe
    df_rank = out.copy()
    if rank_col not in df_rank.columns:
        st.warning(f"Strategy Finder needs column '{rank_col}', but it was not found in backtest output.")
        df_rank = pd.DataFrame()
    else:
        df_rank[rank_col] = pd.to_numeric(df_rank[rank_col], errors="coerce").astype("Int64")
        df_rank = df_rank.dropna(subset=[rank_col])
        df_rank[rank_col] = df_rank[rank_col].astype(int)

    if df_rank.empty:
        st.info("No ranked rows available to analyze for Strategy Finder.")
    else:
        # Apply member scoring filter if requested
        if member_filter_col is not None and member_filter_col in df_rank.columns:
            df_rank[member_filter_col] = df_rank[member_filter_col].fillna(False).astype(bool)
            df_rank_scored = df_rank[df_rank[member_filter_col]].copy()
        else:
            df_rank_scored = df_rank

        total_wins = len(df_rank_scored)
        total_days = int(df_rank_scored["Date"].nunique())
        cores_in_test = sorted(df_rank_scored["Core"].astype(str).unique().tolist())
        ncores = len(cores_in_test)

        if total_wins == 0:
            st.info("No wins are scorable under the selected member mode in this window.")
        else:
            # Row hotness table
            rc = df_rank_scored.groupby(rank_col).agg(
                Wins=("Core", "size"),
                DaysWithWin=("Date", "nunique"),
            ).reset_index().rename(columns={rank_col: "Row"})
            rc["WinPct"] = (rc["Wins"] / total_wins * 100).round(2)
            rc["DayHitPct"] = (rc["DaysWithWin"] / max(1, total_days) * 100).round(2)

            rc = rc.sort_values(["Wins", "DaysWithWin", "Row"], ascending=[False, False, True])

            hottest_row = int(rc.iloc[0]["Row"])
            hottest_wins = int(rc.iloc[0]["Wins"])
            hottest_dayhit = int(rc.iloc[0]["DaysWithWin"])
            # Plays/day assumes you play this row for every tested core every day
            plays_per_day_row1 = ncores * 1 * member_mult
            cost_per_day_row1 = plays_per_day_row1 * float(cost_per_play)
            st.markdown(
                f"**Hottest single row:** Row **{hottest_row}** on **{rank_col}** "
                f"captured **{hottest_wins}/{total_wins} wins** ({(hottest_wins/total_wins*100):.1f}%), "
                f"and hit on **{hottest_dayhit}/{total_days} days** ({(hottest_dayhit/max(1,total_days)*100):.1f}%). "
                f"Playing only that row across **{ncores} cores** costs ~**{plays_per_day_row1} plays/day** (≈ ${cost_per_day_row1:,.2f}/day at ${cost_per_play:.2f})."
            )

            with st.expander("Row hotness table (all rows)", expanded=False):
                _safe_st_dataframe(rc, use_container_width=True, hide_index=True)

            # Evaluate top-K row strategies (specific line sets, not ranges)
            max_row = int(rc["Row"].max())
            max_k_default = min(9, max(1, min(15, max_row)))
            k_max = st.slider(
                "Evaluate Top‑K hottest rows (specific row lines)",
                min_value=1,
                max_value=min(15, max_row),
                value=max_k_default,
                step=1,
                key="sf_kmax",
                help="Top‑K is built from the K hottest rows by win count (not a contiguous range).",
            )

            top_rows = rc["Row"].astype(int).tolist()

            strat_rows = []
            for k in range(1, int(k_max) + 1):
                rows_k = top_rows[:k]
                sub = df_rank_scored[df_rank_scored["Core"].astype(str).isin(cores_in_test) & df_rank_scored[rank_col].isin(rows_k)]
                cap_wins = int(len(sub))
                cap_days = int(sub["Date"].nunique())
                cap_pct = (cap_wins / total_wins * 100.0) if total_wins else 0.0
                day_pct = (cap_days / max(1, total_days) * 100.0)

                plays_per_day = ncores * k * member_mult
                cost_per_day = plays_per_day * float(cost_per_play)
                # Over the tested days, how much spend per captured win?
                spend_total = cost_per_day * total_days
                cost_per_win = (spend_total / cap_wins) if cap_wins > 0 else None
                strat_rows.append({
                    "K (rows)": k,
                    "Rows (specific lines)": ",".join(str(r) for r in rows_k),
                    "CapturedWins": cap_wins,
                    "CapturePct": round(cap_pct, 2),
                    "DaysWith≥1Win": cap_days,
                    "DayHitPct": round(day_pct, 2),
                    "Plays/Day": int(plays_per_day),
                    "Cost/Day($)": round(cost_per_day, 2),
                    "Cost/CapturedWin($)": (round(cost_per_win, 2) if cost_per_win is not None else None),
                })

            strat_df = pd.DataFrame(strat_rows)
            st.markdown("##### Top‑K row strategies (play these specific lines for every tested core)")
            _safe_st_dataframe(strat_df, use_container_width=True, hide_index=True)

            # Manual selection (exact rows)
            st.markdown("##### Try a custom set of row lines")
            default_manual = top_rows[:min(3, len(top_rows))]
            manual_rows = st.multiselect(
                "Select specific rows to play (exact lines, not ranges)",
                options=sorted(top_rows),
                default=default_manual,
                key="sf_manual_rows",
            )
            if manual_rows:
                subm = df_rank_scored[df_rank_scored[rank_col].isin([int(x) for x in manual_rows])].copy()
                cap_wins_m = int(len(subm))
                cap_days_m = int(subm["Date"].nunique())
                plays_per_day_m = ncores * len(manual_rows) * member_mult
                cost_per_day_m = plays_per_day_m * float(cost_per_play)
                st.write(
                    f"Custom rows captured **{cap_wins_m}/{total_wins} wins** ({(cap_wins_m/total_wins*100):.1f}%) "
                    f"across **{cap_days_m}/{total_days} days** ({(cap_days_m/max(1,total_days)*100):.1f}%). "
                    f"Plays/day = **{plays_per_day_m}** (≈ ${cost_per_day_m:,.2f}/day)."
                )

                # Per-core breakdown for the chosen rows
                pc = subm.groupby("Core").size().reset_index(name="CapturedWins")
                total_by_core = df_rank_scored.groupby("Core").size().reset_index(name="TotalWins")
                pc = pc.merge(total_by_core, on="Core", how="right").fillna({"CapturedWins": 0})
                pc["CapturePct"] = (pc["CapturedWins"] / pc["TotalWins"] * 100).round(1)
                pc = pc.sort_values(["CapturePct", "TotalWins"], ascending=[False, False])
                with st.expander("Per-core capture for these rows", expanded=False):
                    _safe_st_dataframe(pc, use_container_width=True, hide_index=True)
            else:
                st.info("Select at least one row to see custom strategy metrics.")

            # Core-by-core Top2 member recommendation quick table (for the current backtest window)
            if member_track and ("ActualFamilyMember" in out.columns) and (not out.dropna(subset=["ActualFamilyMember"]).empty):
                st.markdown("##### Core-by-core Top2 member recommendation (from training window)")
                st.caption("This summarizes which member label (AABC/ABBC/ABCC) actually hit most often in this backtest window.")
                md = out.dropna(subset=["ActualFamilyMember"]).copy()
                dist = md.pivot_table(index="Core", columns="ActualFamilyMember", values="Date", aggfunc="size", fill_value=0)
                for col in ["AABC","ABBC","ABCC"]:
                    if col not in dist.columns:
                        dist[col] = 0
                dist = dist[["AABC","ABBC","ABCC"]]
                dist["Total"] = dist.sum(axis=1)
                # Top2 members
                def _top2(row):
                    pairs = [(k, int(row[k])) for k in ["AABC","ABBC","ABCC"]]
                    pairs.sort(key=lambda x: (-x[1], x[0]))
                    return pairs[0][0], pairs[1][0]
                top2 = dist.apply(_top2, axis=1, result_type="expand")
                dist["Top1"] = top2[0]
                dist["Top2"] = top2[1]
                dist["Top1Pct"] = dist.apply(lambda r: round((int(r[r["Top1"]]) / (int(r["Total"]) or 1)) * 100, 1), axis=1)
                dist = dist.reset_index().sort_values(["Top1Pct","Total"], ascending=[False, False])
                _safe_st_dataframe(dist[["Core","AABC","ABBC","ABCC","Total","Top1","Top2","Top1Pct"]], use_container_width=True, hide_index=True)



# -------------------------
# Member strategy comparisons (walk-forward, no cheat)
# -------------------------
if member_track and (not out.empty):
    st.markdown("#### Member Strategy Finder (MODE vs LAST vs overrides)")
    st.caption(
        "These comparisons are **walk-forward safe**: for each test_date, member predictions are generated using only rows with Date < test_date. "
        "This helps decide whether you should play 1 member, 2 members, or all 3 for a given core."
    )

    # Compute only on rows where the winner was one of the family members (AABC/ABBC/ABCC)
    mc = out.dropna(subset=["ActualFamilyMember"]).copy()
    mc = mc[mc["ActualFamilyMember"].astype(str).isin(["AABC","ABBC","ABCC"])].copy()

    if mc.empty:
        st.info("No family-member rows in this backtest window to compare member strategies.")
    else:
        # Build predictions per row under multiple strategies
        preds = []
        for i, r in mc.iterrows():
            try:
                core = str(r.get("Core","")).zfill(3)
                stream = str(r.get("Stream",""))
                td = pd.to_datetime(r.get("Date"))
                variants = _member_prediction_variants(
                    df_all=df_all,
                    traits_pos_df=seed_traits_pos_df,
                    core_key=core,
                    test_date=td,
                    window_days=int(cfg.window_days),
                    stream=stream,
                    basis=str(member_basis),
                )
                preds.append(variants)
            except Exception:
                preds.append({"MODE": None, "LAST_GLOBAL": None, "LAST_HIER": None, "SEED_OVERRIDE": None, "TRAIT_OVERRIDE": None})

        var_df = pd.DataFrame(preds)
        for c in ["MODE","LAST_GLOBAL","LAST_HIER","SEED_OVERRIDE","TRAIT_OVERRIDE"]:
            mc[f"PredMember_{c}"] = var_df[c].values
            mc[f"Hit_{c}"] = (mc["ActualFamilyMember"].astype(str) == mc[f"PredMember_{c}"].astype(str))

        # Summary by core
        sum_rows = []
        for core, g in mc.groupby("Core"):
            n = int(len(g))
            row = {"Core": core, "N": n}
            for c in ["MODE","LAST_GLOBAL","LAST_HIER","SEED_OVERRIDE","TRAIT_OVERRIDE"]:
                row[f"Top1_{c}"] = int(g[f"Hit_{c}"].sum())
                row[f"Top1Rate_{c}"] = round(float(g[f"Hit_{c}"].mean()), 4) if n else 0.0
            sum_rows.append(row)
        sum_df = pd.DataFrame(sum_rows).sort_values(["N"], ascending=False)

        st.markdown("##### Top1 member accuracy by core (compare strategies)")
        _safe_st_dataframe(sum_df, use_container_width=True, hide_index=True)

        # Overall summary
        overall = {"Metric": ["Rows (family-member only)"]}
        overall["Value"] = [len(mc)]
        overall_df = pd.DataFrame(overall)
        _safe_st_dataframe(overall_df, use_container_width=True, hide_index=True)

        # Recommended Top2 members per core (latest as-of end_dt)
        st.markdown("##### Core-by-core Top2 member recommendations (for play reduction)")
        st.caption(
            "Top2 is built from the **training window right before the most recent test_date** in this run. "
            "Use this when Top1 is weak but Top2 is strong (play 2 members instead of all 3)."
        )

        try:
            last_test = pd.to_datetime(out["Date"], errors="coerce").max()
        except Exception:
            last_test = pd.to_datetime(end_dt)

        recs = []
        for core in sorted(set(mc["Core"].astype(str).tolist())):
            # MODE distribution from the existing predictor (returns top1/top2 by counts)
            mp = predict_core_member(df_all, core, last_test, int(cfg.window_days), basis=("core_stream" if str(member_basis)=="core_stream" else "core"), stream=None, include_rare=False)
            t1, t2 = mp.get("top1"), mp.get("top2")
            ntrain = int(mp.get("n") or 0)
            recs.append({"Core": core, "Top1(MODE)": t1, "Top2(MODE)": t2, "TrainN": ntrain})
        rec_df = pd.DataFrame(recs).sort_values(["TrainN"], ascending=False)
        _safe_st_dataframe(rec_df, use_container_width=True, hide_index=True)

        with st.expander("Download member strategy comparison rows (copy/paste ready)", expanded=False):
            dl_cols = ["Date","Stream","Core","Winner","ActualFamilyMember"] +                           [f"PredMember_{c}" for c in ["MODE","LAST_GLOBAL","LAST_HIER","SEED_OVERRIDE","TRAIT_OVERRIDE"]] +                           [f"Hit_{c}" for c in ["MODE","LAST_GLOBAL","LAST_HIER","SEED_OVERRIDE","TRAIT_OVERRIDE"]]
            dl_cols = [c for c in dl_cols if c in mc.columns]
            _safe_st_dataframe(mc[dl_cols].sort_values(["Date","Stream","Core"]), use_container_width=True)
            st.download_button(
                "Download member strategy rows CSV",
                data=mc[dl_cols].to_csv(index=False).encode("utf-8"),
                file_name="member_strategy_comparisons.csv",
                mime="text/csv",
            )

    st.markdown("#### Hit/Miss detail (copy/paste ready)")
    show_cols = [
        "Date", "Stream", "Core", "Winner", "Predicted", "Bucket", "RankPos", "BaseScoreRank", "HitsWindow", "DaysSinceLastHit",
        "ActualFamilyMember", "PredMemberTop1", "PredMemberTop2", "MemberHitTop1", "MemberHitTop2", "MemberTrainN", "AsOfMaxDate",
    ]
    safe_cols = [c for c in show_cols if c in out.columns]
    _safe_st_dataframe(out[safe_cols].sort_values(["Date","Stream","Core"], ascending=True), use_container_width=True)
    st.download_button(
        "Download walk-forward rows CSV",
        data=out.to_csv(index=False).encode("utf-8"),
        file_name="walkforward_rows.csv",
        mime="text/csv",
    )

    st.markdown("#### By core")
    by_core = out.groupby("Core").agg(Total=("Predicted","size"), Predicted=("Predicted","sum")).reset_index()
    by_core["PredictedPct"] = (by_core["Predicted"] / by_core["Total"] * 100).round(1)
    _safe_st_dataframe(by_core.sort_values(["PredictedPct","Total"], ascending=[False, False]), use_container_width=True, hide_index=True)

    st.markdown("#### By stream")
    by_stream = out.groupby("Stream").agg(Total=("Predicted","size"), Predicted=("Predicted","sum")).reset_index()
    by_stream["PredictedPct"] = (by_stream["Predicted"] / by_stream["Total"] * 100).round(1)
    _safe_st_dataframe(by_stream.sort_values(["Predicted","Total"], ascending=[False, False]).head(80), use_container_width=True, hide_index=True)

    _bt_files = {
        "walkforward_rows.csv": out,
        "by_core.csv": by_core,
        "by_stream.csv": by_stream,
        "BUILD_INFO.txt": (
            f"BUILD: {APP_VERSION}\n"
            f"CORE CONTRACT: {'PASS' if list(cores_sel) == list(WORKING8_CORE_SET) else 'FAIL'}\n"
            f"CORE COUNT: {len(cores_sel)}\n"
            f"EXPECTED WORKING 8: {', '.join(WORKING8_CORE_SET)}\n"
            f"SELECTED CORES: {', '.join(cores_sel)}\n"
            f"MISSING CORES: {', '.join([c for c in WORKING8_CORE_SET if c not in cores_sel]) or 'none'}\n"
            f"BASE/DUE GATE MODE: {base_due_gate_mode}\nDAILY_WF_PARITY: LOCKED_SHARED_PIPELINE\n"
        ),
    }
    if 'runtime_df' in locals() and isinstance(runtime_df, pd.DataFrame):
        _bt_files["runtime_by_date.csv"] = runtime_df
    if 'mc' in locals() and isinstance(mc, pd.DataFrame):
        _bt_files["member_strategy_rows.csv"] = mc
    if 'sum_df' in locals() and isinstance(sum_df, pd.DataFrame):
        _bt_files["member_strategy_summary.csv"] = sum_df
    if 'rec_df' in locals() and isinstance(rec_df, pd.DataFrame):
        _bt_files["member_top2_recommendations.csv"] = rec_df
    st.download_button(
        "Download all Backtest outputs",
        data=_build_outputs_zip(_bt_files),
        file_name=f"BACKTEST_ALL_SELECTED_{APP_VERSION.split()[0]}.zip",
        mime="application/zip",
        key="bt_download_all_outputs",
        use_container_width=True,
    )

    # Manual day replay
    st.markdown("#### Manual day replay (mock what you'd do daily)")
    unique_days = sorted(out["Date"].unique())
    sel_day = st.selectbox("Pick a day to inspect", options=unique_days, index=max(0, len(unique_days)-1), key="bt_replay_day")
    sel_day_ts = pd.Timestamp(sel_day)

    # Show that day's core hits and where they sat on the chart
    day_rows = out[out["Date"] == sel_day].copy()
    st.write(f"Core-family wins on {sel_day}: {len(day_rows)}")
    _safe_st_dataframe(day_rows.sort_values(["Core","Predicted"], ascending=[True, False]), use_container_width=True, hide_index=True)

    # For each core that hit: show predicted stream buckets for that day (from training)
    for core in sorted(day_rows["Core"].unique()):
        st.markdown(f"**Core {core}: predicted streams as of {sel_day}**")

        if member_track:
            try:
                mp_overall = predict_core_member(
                    df_all,
                    core,
                    pd.to_datetime(sel_day_ts).normalize(),
                    window_days=int(cfg.window_days),
                    basis="core",
                    include_rare=False,
                )
            except Exception:
                mp_overall = {}
            if mp_overall:
                st.caption(
                    f"Member pick (overall): Top1={mp_overall.get('top1')}, Top2={mp_overall.get('top2')} (train hits={mp_overall.get('n')})"
                )
        train_df = df_all[df_all["Date"] < sel_day_ts]
        stats_df = compute_stream_stats(train_df, core, window_days=int(cfg.window_days))
        buckets = bucket_recommendations(stats_df, cfg)
        base_df = buckets.get("Top12BaseScore", pd.DataFrame()).copy()
        due_df = buckets.get("Due8", pd.DataFrame()).copy()
        if not base_df.empty:
            base_df["Bucket"] = "BaseScore"
        if not due_df.empty:
            due_df["Bucket"] = "Due8"
        pred_df = _safe_pd_concat([base_df, due_df], ignore_index=True)
        if pred_df.empty:
            st.info("No bucket recommendations for this core/day.")
            continue
        # mark if this stream was an actual win that day
        win_streams = set(day_rows[day_rows["Core"] == core]["Stream"].astype(str).tolist())
        pred_df["WonThatDay"] = pred_df["Stream"].astype(str).isin(win_streams)
        cols = [c for c in ["Bucket","Stream","RankPos","BaseScoreRank","HitsWindow","DaysSinceLastHit","WonThatDay"] if c in pred_df.columns]
        _safe_st_dataframe(pred_df[cols].sort_values(["WonThatDay","Bucket"], ascending=[False, True]), use_container_width=True, hide_index=True)


def _age_days(ts: Optional[pd.Timestamp]) -> Optional[int]:
    if ts is None or pd.isna(ts):
        return None
    try:
        d = pd.to_datetime(ts).date()
        return (today - d).days
    except Exception:
        return None

st.sidebar.markdown("### Data freshness")
if last_all is not None and not pd.isna(last_all):
    st.sidebar.caption(f"All‑states history most recent date: {pd.to_datetime(last_all).date()}  (age: {_age_days(last_all)} days)")
else:
    st.sidebar.caption("All‑states history most recent date: (not found)")

if last_24 is not None and not pd.isna(last_24):
    st.sidebar.caption(f"24h map most recent date: {pd.to_datetime(last_24).date()}  (age: {_age_days(last_24)} days)")
else:
    st.sidebar.caption("24h map most recent date: (not uploaded)")

st.sidebar.caption(f"Tip: if ages are >1–2 days, your files are probably behind.")

if master_file is None:
    st.info("Upload your all‑states history file to start.")
    st.stop()

if df_all.empty:
    st.error("Could not parse your history file. Make sure it contains Date, State, Game, Results.")
    st.stop()

# One place to show dataset info
colA, colB, colC = st.columns(3)
with colA:
    st.metric("Rows (draws)", f"{len(df_all):,}")
with colB:
    st.metric("Streams", f"{df_all['Stream'].nunique():,}")
with colC:
    min_d = df_all["Date"].min().date().isoformat()
    max_d = df_all["Date"].max().date().isoformat()
    st.caption(f"Date span: {min_d} → {max_d}")

# Permanent date contract for every daily run.
_history_through_ts = pd.to_datetime(last_all).normalize()
_seedlist_date = _history_through_ts.date().isoformat()
_playlist_date = (_history_through_ts + pd.Timedelta(days=1)).date().isoformat()
st.session_state["history_through"] = _seedlist_date
st.session_state["seedlist_date"] = _seedlist_date
st.session_state["playlist_date"] = _playlist_date

st.markdown("### Daily run dates")
_date1, _date2 = st.columns(2)
_date1.metric("PLAYLIST DATE", _playlist_date)
_date2.metric("SEEDLIST DATE / HISTORY THROUGH", _seedlist_date)
st.caption("Seeds shown on the playlist are the latest stream results available through the Seedlist Date. Never play a list whose Playlist Date is not the intended play day.")

st.divider()


# Load latest walk-forward backtest output (if any) for planner panels
out = st.session_state.get("wf_backtest_out", pd.DataFrame())

# Always expose one complete ZIP for the latest walk-forward result.
# This is intentionally independent of the optional member-tracking panel.
if isinstance(out, pd.DataFrame) and (not out.empty):
    _bt_core = out.groupby("Core").agg(
        Total=("Predicted", "size"),
        Predicted=("Predicted", "sum"),
    ).reset_index()
    _bt_core["PredictedPct"] = (_bt_core["Predicted"] / _bt_core["Total"] * 100).round(1)

    _bt_stream = out.groupby("Stream").agg(
        Total=("Predicted", "size"),
        Predicted=("Predicted", "sum"),
    ).reset_index()
    _bt_stream["PredictedPct"] = (_bt_stream["Predicted"] / _bt_stream["Total"] * 100).round(1)

    _bt_zip_files = {
        "walkforward_winner_rows.csv": out,
        "summary_by_core.csv": _bt_core,
        "summary_by_stream.csv": _bt_stream,
        "BACKTEST_README.txt": (
            f"BUILD: {APP_VERSION}\n"
            "This ZIP contains the latest walk-forward result stored in this session.\n"
            "IMPORTANT: the current backtester exports winner-related rows, not the full daily nonwinner ranking universe.\n"
        ),
    }
    st.download_button(
        "Download latest Backtest ZIP",
        data=_build_outputs_zip(_bt_zip_files),
        file_name=f"BACKTEST_LATEST_{APP_VERSION.split()[0]}.zip",
        mime="application/zip",
        key="bt_latest_zip_download",
        use_container_width=True,
    )

# -------------------------
# Auto Profit Planner (Box baseline + selective Straight booster)
# -------------------------
if (not out.empty):
    st.markdown("#### Auto Profit Planner (make the strategy automatic)")
    st.caption(
        "This planner uses your **walk-forward backtest rows** (no leakage) to recommend a small set of cores "
        "that can reach your target **average box wins per day**, while minimizing plays. "
        "Straight/Box combo tickets are supported as an optional **selective straight booster**."
    )

    with st.expander("Open Auto Profit Planner", expanded=False):
        # --- Controls ---
        ap1, ap2, ap3, ap4 = st.columns([1.15, 1.0, 1.0, 1.0])
        with ap1:
            eval_days = st.number_input("Evaluate on last N days", min_value=30, max_value=365, value=90, step=10, key="ap_eval_days")
        with ap2:
            target_avg_box_wins = st.number_input("Target avg BOX wins/day", min_value=0.1, max_value=10.0, value=1.0, step=0.1, key="ap_target_box_wins")
        with ap3:
            max_box_plays_per_day = st.number_input("Max BOX plays/day (soft cap)", min_value=1, max_value=2000, value=120, step=5, key="ap_max_box_plays_day")
        with ap4:
            cost_per_play_ap = st.number_input("Cost per play ($)", min_value=0.0, max_value=10.0, value=float(st.session_state.get("sf_cost_per_play", 0.25)), step=0.05, key="ap_cost_per_play")

        ap5, ap6, ap7 = st.columns([1.2, 1.2, 1.2])
        with ap5:
            rank_basis = st.selectbox(
                "Row basis (same meaning as Strategy Finder)",
                ["RankPos", "BaseScoreRank"],
                index=0,
                key="ap_rank_basis",
                help="This controls which row-number column is used to define your 'line' selections.",
            )
        rank_col_ap = "RankPos" if rank_basis == "RankPos" else "BaseScoreRank"

        with ap6:
            # Rows/lines selection
            default_rows = list(range(1, 13))  # Top12 baseline
            auto_rows = st.checkbox(
                f"Auto-expand {rank_col_ap} rows to meet target (recommended)",
                value=True,
                key="ap_auto_rows",
                help="If enabled, the planner will choose the smallest set of rows/lines (in priority order) needed to reach your target avg BOX wins/day.",
            )
            if auto_rows:
                max_row_limit = st.slider(
                    f"Max {rank_col_ap} row to consider",
                    min_value=5,
                    max_value=60,
                    value=30,
                    step=1,
                    key="ap_max_row_limit",
                    help="Upper bound for auto row expansion. Lower = cheaper and more focused.",
                )
                allowed_rows_ap = []  # filled after we compute row priorities
            else:
                allowed_rows_ap = st.multiselect(
                    f"Allowed {rank_col_ap} rows (these are your playable 'lines')",
                    options=list(range(1, 61)),
                    default=default_rows,
                    key="ap_allowed_rows",
                )
                allowed_rows_ap = sorted([int(x) for x in allowed_rows_ap]) if allowed_rows_ap else default_rows

        with ap7:
            member_mode_ap = st.selectbox(
                "Member play mode (BOX baseline)",
                [
                    "AUTO: Global member priority (Top1 baseline, then add extra picks by ROI)",
                    "BOX: Play Top1 member pick (per-row)",
                    "BOX: Play Top2 member picks (per-row)",
                    "BOX: Play all 3 family members (per-row)",
                ],
                index=0,
                key="ap_member_mode",
                help="AUTO mode prioritizes members across ALL families by incremental win gain per extra play (so a weak family’s Member#2 can be lower priority than another family’s Member#1).",
            )


        # Straight booster controls (optional)
        sb1, sb2, sb3 = st.columns([1.2, 1.0, 1.0])
        with sb1:
            use_straight_booster = st.checkbox(
                "Enable selective STRAIGHT booster (adds straight only to the highest-confidence pick(s))",
                value=True,
                key="ap_use_straight_booster",
                help="This does NOT add straight to everything. It adds straight only to the top-1 (or top-k) best-confidence ordered pick per day.",
            )
        with sb2:
            max_straights_per_day = st.number_input("Max STRAIGHT picks/day", min_value=0, max_value=50, value=1, step=1, key="ap_max_straights_day")
        with sb3:
            straight_cost = st.number_input("STRAIGHT cost per pick ($)", min_value=0.0, max_value=10.0, value=0.25, step=0.05, key="ap_straight_cost")

        # --- Build evaluation slice ---
        _tmp = out.copy()
        _tmp["Date"] = pd.to_datetime(_tmp["Date"], errors="coerce")
        _tmp = _tmp.dropna(subset=["Date"])
        max_date = _tmp["Date"].max()
        min_date = max_date - pd.Timedelta(days=int(eval_days) - 1)
        ev = _tmp[_tmp["Date"].between(min_date, max_date)].copy()

        if ev.empty:
            st.warning("No backtest rows in the selected evaluation window.")
        else:
                        # ----------------------------
            # Row (line) priority + auto row expansion (optional)
            # ----------------------------
            fam_mask = ev["ActualFamilyMember"].astype(str).isin(["AABC", "ABBC", "ABCC"])
            pred_mask = ev["Predicted"].astype(bool)

            # If RankPos and BaseScoreRank are identical in this dataset, tell the user (prevents confusion).
            try:
                if ("RankPos" in ev.columns) and ("BaseScoreRank" in ev.columns) and ev["RankPos"].astype(int).equals(ev["BaseScoreRank"].astype(int)):
                    st.info("Heads up: In this backtest export, **RankPos == BaseScoreRank** for all rows, so either basis behaves the same here.")
            except Exception:
                pass

            # Establish candidate rows universe for priority scoring
            _max_row_for_scoring = int(max_row_limit) if 'max_row_limit' in locals() else 60
            _max_row_for_scoring = max(1, min(60, _max_row_for_scoring))
            cand_rows = list(range(1, _max_row_for_scoring + 1))

            # Score each row by "wins per play" using Top1 as the baseline signal (stable + cheapest)
            row_scores = []
            for r in cand_rows:
                rm = ev[rank_col_ap].astype(int).eq(int(r))
                plays = int((fam_mask & pred_mask & rm).sum())
                wins = int((fam_mask & pred_mask & rm & ev.get("MemberHitTop1", False).astype(bool)).sum()) if "MemberHitTop1" in ev.columns else 0
                eff = (wins / plays) if plays > 0 else 0.0
                row_scores.append({"Row": r, "Plays": plays, "WinsTop1": wins, "WinsPerPlayTop1": eff})
            row_scores_df = pd.DataFrame(row_scores).sort_values(["WinsPerPlayTop1", "WinsTop1", "Plays", "Row"], ascending=[False, False, False, True])

            # Choose rows
            if 'auto_rows' in locals() and auto_rows:
                # Add rows in priority order until we meet the target avg BOX wins/day (using the chosen member mode if not AUTO; else Top1 baseline)
                priority_rows = row_scores_df["Row"].astype(int).tolist()
                chosen = []
                # helper to compute avg wins/day under a set of rows and a given member mode
                def _avg_box_wins_per_day(rows_set, mode_name: str) -> tuple[float, float]:
                    if not rows_set:
                        return 0.0, 0.0
                    rm = ev[rank_col_ap].astype(int).isin([int(x) for x in rows_set])
                    base = fam_mask & pred_mask & rm
                    # Box win for a row depends on member picks
                    if mode_name.startswith("BOX: Play Top1"):
                        w = base & ev["MemberHitTop1"].astype(bool)
                        plays_per_row = 1
                    elif mode_name.startswith("BOX: Play Top2"):
                        w = base & ev["MemberHitTop2"].astype(bool)
                        plays_per_row = 2
                    elif mode_name.startswith("BOX: Play all 3"):
                        w = base  # all 3 members guarantees a box hit for any predicted family row
                        plays_per_row = 3
                    else:
                        # AUTO: evaluate baseline (Top1) for row expansion; member expansion handled later
                        w = base & ev["MemberHitTop1"].astype(bool)
                        plays_per_row = 1
                    # day-level wins
                    day_wins = w.groupby(ev["Date"].dt.date).any().astype(int)
                    avg_wins = float(day_wins.mean()) if len(day_wins) else 0.0
                    # plays/day
                    plays_day = (base.groupby(ev["Date"].dt.date).sum() * plays_per_row)
                    avg_plays = float(plays_day.mean()) if len(plays_day) else 0.0
                    return avg_wins, avg_plays

                for r in priority_rows:
                    if r in chosen:
                        continue
                    chosen.append(int(r))
                    avg_w, avg_p = _avg_box_wins_per_day(chosen, member_mode_ap)
                    if avg_w >= float(target_avg_box_wins):
                        break

                allowed_rows_ap = sorted(chosen) if chosen else default_rows
                st.caption(f"Auto rows chosen ({rank_col_ap}): **{', '.join(map(str, allowed_rows_ap))}**")
                with st.expander("Row priority details (why these rows?)", expanded=False):
                    _safe_st_dataframe(row_scores_df, use_container_width=True, hide_index=True)
            else:
                # Manual rows already set above
                with st.expander("Row priority details (informational)", expanded=False):
                    _safe_st_dataframe(row_scores_df, use_container_width=True, hide_index=True)

            # ----------------------------
            # Build BOX win mask under the selected member mode
            # ----------------------------
            row_mask = ev[rank_col_ap].astype(int).isin([int(x) for x in allowed_rows_ap])

            # Member AUTO mode: treat each additional member pick as its own ROI-ranked action across ALL families
            if member_mode_ap.startswith("AUTO"):
                base_rows = fam_mask & pred_mask & row_mask
                # Baseline = Top1 for all predicted family rows
                base_win = base_rows & ev["MemberHitTop1"].astype(bool)
                # day-level wins under baseline
                day_any = base_win.groupby(ev["Date"].dt.date).any()
                win_days = set(day_any[day_any].index.tolist())
                all_days = sorted(ev["Date"].dt.date.unique().tolist())
                target_wins_total = float(target_avg_box_wins) * max(1, len(all_days))

                # Build candidate extra picks:
                # - Add Top2 when Top1 missed but Top2 would hit.
                # - Add 3rd member only when both Top1 and Top2 missed (meaning actual is the 3rd).
                cand = []
                if "MemberHitTop2" in ev.columns:
                    # Top2 incremental win rows
                    top2_inc = base_rows & (~ev["MemberHitTop1"].astype(bool)) & (ev["MemberHitTop2"].astype(bool))
                    for d, sub in ev[top2_inc].groupby(ev["Date"].dt.date):
                        # one extra pick can win that specific row; but we care if it converts the day
                        cand.append({"kind":"TOP2", "date": d, "gain_day": (d not in win_days)})
                # 3rd member incremental (only if we allow it; we will allow if still short after Top2)
                third_inc = base_rows & (~ev["MemberHitTop2"].astype(bool)) & (~ev["MemberHitTop1"].astype(bool))
                for d, sub in ev[third_inc].groupby(ev["Date"].dt.date):
                    cand.append({"kind":"THIRD", "date": d, "gain_day": (d not in win_days)})

                # Greedy add extra picks that convert currently losing days first
                extra_picks = []
                # We approximate 1 candidate per date for simplicity (best possible), because we only need daily coverage and extra pick converts the day.
                # Prioritize TOP2 over THIRD (cheaper confidence-wise) when both could convert the day.
                for kind in ["TOP2", "THIRD"]:
                    for item in [c for c in cand if c["kind"]==kind]:
                        if len(win_days) >= target_wins_total:
                            break
                        if item["date"] in win_days:
                            continue
                        win_days.add(item["date"])
                        extra_picks.append(item)
                    if len(win_days) >= target_wins_total:
                        break

                # Construct final win mask:
                # baseline wins + any wins due to extra picks (we approximate as converting the day when possible)
                ev["_BoxWinBaselineTop1"] = base_win
                # For reporting: effective plays per row = 1 baseline, plus extra picks counted at day-level
                ev["BoxWin"] = base_win  # row-level; day-level conversion handled below
                members_per_row = 1  # baseline
                auto_extra_picks = len(extra_picks)
            else:
                auto_extra_picks = 0
                if member_mode_ap.startswith("BOX: Play Top1"):
                    win_mask = fam_mask & pred_mask & row_mask & ev["MemberHitTop1"].astype(bool)
                    members_per_row = 1
                elif member_mode_ap.startswith("BOX: Play Top2"):
                    win_mask = fam_mask & pred_mask & row_mask & ev["MemberHitTop2"].astype(bool)
                    members_per_row = 2
                else:
                    win_mask = fam_mask & pred_mask & row_mask
                    members_per_row = 3
                ev["BoxWin"] = win_mask

            # If AUTO member mode, compute a day-level win series directly and keep it for downstream stats.
            if member_mode_ap.startswith("AUTO"):
                # baseline day wins
                day_wins_series = ev["_BoxWinBaselineTop1"].groupby(ev["Date"].dt.date).any().astype(int)
                # add extra picks as day-level conversions
                for it in extra_picks:
                    if it["date"] in day_wins_series.index:
                        day_wins_series.loc[it["date"]] = 1
                ev["_BoxWinDay"] = ev["Date"].dt.date.map(lambda d: bool(day_wins_series.get(d, 0)))
            else:
                ev["_BoxWinDay"] = ev["Date"].dt.date.map(lambda d: bool((ev.loc[ev["Date"].dt.date==d, "BoxWin"]).any()))


# Plays/day and wins/day per core
            cores = sorted(ev["Core"].astype(str).unique().tolist())
            days = sorted(ev["Date"].dt.date.unique().tolist())

            # Per-core daily metrics
            per_core = []
            for c in cores:
                g = ev[ev["Core"].astype(str) == str(c)].copy()
                # BOX plays = predicted rows within allowed lines * members_per_row
                plays = int((g[g["Predicted"].astype(bool) & g[rank_col_ap].astype(int).isin([int(x) for x in allowed_rows_ap])].shape[0]) * members_per_row) if (not g.empty) else 0
                # But plays should be per day average, so compute properly:
                plays_by_day = (
                    g[g["Predicted"].astype(bool) & g[rank_col_ap].astype(int).isin([int(x) for x in allowed_rows_ap])]
                    .groupby(g["Date"].dt.date)
                    .size()
                    .reindex(days, fill_value=0)
                    .astype(int)
                    * int(members_per_row)
                )
                wins_by_day = (
                    g[g["BoxWin"]]
                    .groupby(g["Date"].dt.date)
                    .size()
                    .reindex(days, fill_value=0)
                    .astype(int)
                )
                # Predictability signals (member accuracy), using the same evaluation slice
                top1_rate = float(g["MemberHitTop1"].astype(bool).mean()) if "MemberHitTop1" in g.columns else np.nan
                top2_rate = float(g["MemberHitTop2"].astype(bool).mean()) if "MemberHitTop2" in g.columns else np.nan

                # Simple "likely winner" signal (data-driven but conservative):
                # - prioritize small row numbers, more recent activity, and strong Top1 member accuracy
                avg_rank = float(g[rank_col_ap].astype(int).mean()) if (rank_col_ap in g.columns and len(g)) else np.nan
                avg_hitsw = float(g["HitsWindow"].astype(float).mean()) if ("HitsWindow" in g.columns and len(g)) else np.nan
                avg_dslh = float(g["DaysSinceLastHit"].astype(float).mean()) if ("DaysSinceLastHit" in g.columns and len(g)) else np.nan
                if (not np.isnan(top1_rate)) and (not np.isnan(avg_rank)) and (not np.isnan(avg_dslh)):
                    if (avg_rank <= 5) and (top1_rate >= 0.45) and (avg_dslh <= 30):
                        signal = "🟢 High"
                    elif (avg_rank <= 12) and (top1_rate >= 0.35) and (avg_dslh <= 60):
                        signal = "🟡 Medium"
                    else:
                        signal = "🔴 Low"
                else:
                    signal = "—"

                per_core.append(
                    {
                        "Core": str(c).zfill(3),
                        "EvalDays": len(days),
                        "BoxWinDays": int((wins_by_day > 0).sum()),
                        "BoxWinsTotal": int(wins_by_day.sum()),
                        "AvgBoxWinsPerDay": float(wins_by_day.sum() / max(1, len(days))),
                        "AvgBoxPlaysPerDay": float(plays_by_day.mean()),
                        "AvgRow": round(avg_rank, 2) if not np.isnan(avg_rank) else np.nan,
                        "Signal": signal,
                        "Top1Rate": round(top1_rate, 4) if not np.isnan(top1_rate) else np.nan,
                        "Top2Rate": round(top2_rate, 4) if not np.isnan(top2_rate) else np.nan,
                    }
                )

            per_core_df = pd.DataFrame(per_core)
            per_core_df["EffWinsPerPlay"] = (
                per_core_df["AvgBoxWinsPerDay"] / per_core_df["AvgBoxPlaysPerDay"].replace({0: np.nan})
            )
            per_core_df["EffWinsPerPlay"] = per_core_df["EffWinsPerPlay"].replace([np.inf, -np.inf], np.nan).fillna(0.0)

            st.markdown("**Core cadence + predictability snapshot (evaluation window)**")
            _safe_st_dataframe(
                per_core_df.sort_values(["AvgBoxWinsPerDay", "EffWinsPerPlay"], ascending=False),
                use_container_width=True,
                hide_index=True,
            )

            # --- Greedy selection: minimum cores to reach target avg wins/day under play cap ---
            st.markdown("**Auto-select minimum cores to reach target wins/day**")
            st.caption(
                "This uses a greedy set-building approach based on the *marginal increase* in wins/day. "
                "It is real-data-driven from the backtest rows you already generated."
            )

            # Precompute per-core wins_by_day and plays_by_day for fast greedy union
            core_wins = {}
            core_plays = {}
            for c in cores:
                g = ev[ev["Core"].astype(str) == str(c)].copy()
                core_wins[str(c).zfill(3)] = (
                    g[g["BoxWin"]].groupby(g["Date"].dt.date).size().reindex(days, fill_value=0).astype(int).values
                )
                core_plays[str(c).zfill(3)] = (
                    g[g["Predicted"].astype(bool) & g[rank_col_ap].astype(int).isin([int(x) for x in allowed_rows_ap])].groupby(g["Date"].dt.date).size().reindex(days, fill_value=0).astype(int).values
                    * int(members_per_row)
                )

            selected = []
            total_wins_vec = np.zeros(len(days), dtype=int)
            total_plays_vec = np.zeros(len(days), dtype=int)

            remaining = [str(c).zfill(3) for c in cores]
            # Sort candidates by efficiency first to make greedy stable
            remaining = sorted(
                remaining,
                key=lambda x: (
                    float(per_core_df.loc[per_core_df["Core"] == x, "EffWinsPerPlay"].values[0])
                    if (per_core_df["Core"] == x).any() else 0.0
                ),
                reverse=True,
            )

            def _avg(v):
                return float(np.sum(v) / max(1, len(days)))

            target_reached = False
            for _ in range(0, 60):  # safety cap
                cur_avg_wins = _avg(total_wins_vec)
                cur_avg_plays = float(np.mean(total_plays_vec))
                if (cur_avg_wins >= float(target_avg_box_wins)) and (cur_avg_plays <= float(max_box_plays_per_day)):
                    target_reached = True
                    break

                best = None
                best_gain = -1.0
                best_new_plays = None

                for c in remaining:
                    new_wins = total_wins_vec + core_wins[c]
                    new_plays = total_plays_vec + core_plays[c]
                    gain = _avg(new_wins) - _avg(total_wins_vec)
                    # Soft enforce play cap by penalizing cores that push plays too high
                    plays_penalty = max(0.0, (float(np.mean(new_plays)) - float(max_box_plays_per_day)) / max(1.0, float(max_box_plays_per_day)))
                    score = gain - (0.5 * plays_penalty)
                    if score > best_gain:
                        best_gain = score
                        best = c
                        best_new_plays = new_plays

                if best is None:
                    break

                selected.append(best)
                total_wins_vec = total_wins_vec + core_wins[best]
                total_plays_vec = total_plays_vec + core_plays[best]
                remaining = [c for c in remaining if c != best]

                if not remaining:
                    break

            # Report result
            avg_wins = _avg(total_wins_vec)
            avg_plays = float(np.mean(total_plays_vec))
            avg_cost = avg_plays * float(cost_per_play_ap)

            st.write(
                f"**Selected cores:** {', '.join(selected) if selected else '(none)'}"
            )
            st.write(
                f"**Expected avg BOX wins/day (eval window):** {avg_wins:.3f}  |  "
                f"**Avg BOX plays/day:** {avg_plays:.1f}  |  "
                f"**Avg BOX cost/day:** ${avg_cost:.2f}"
            )

            if avg_wins < float(target_avg_box_wins):
                st.warning(
                    "Even after selecting many cores, the evaluation slice did not reach your target avg wins/day under the current row + member settings. "
                    "Try widening allowed rows, switching to Top2/all-3 members, or increasing the play cap."
                )

            # --- Straight booster: build a daily top-1 ordered pick (real, walk-forward safe) ---
            if use_straight_booster and (max_straights_per_day > 0) and selected:
                st.markdown("**Selective STRAIGHT booster (daily top pick)**")
                st.caption(
                    "This uses a simple, **walk-forward safe** rule for ordering: for the chosen BOX member, "
                    "predict the STRAIGHT as LAST(stream exact hit for that member) → else MODE(stream exact hit) → else skip. "
                    "It is meant as a conservative booster, not a guarantee."
                )

                # Build a tiny per-day straight suggestion list based on the selected cores and predicted rows
                # Note: We only suggest straights on rows where you are already playing a BOX pick.
                ev_sel = ev[ev["Core"].astype(str).isin(selected)].copy()
                ev_sel = ev_sel[pred_mask & row_mask].copy()
                ev_sel["DateOnly"] = ev_sel["Date"].dt.date

                # Helper: infer the chosen member label for the row under current member mode
                def _row_chosen_member_label(r):
                    if member_mode_ap.startswith("BOX: Play Top1"):
                        return str(r.get("PredMemberTop1", "")) if pd.notna(r.get("PredMemberTop1")) else ""
                    if member_mode_ap.startswith("BOX: Play Top2"):
                        # We'll still choose the top1 label for straight ordering (cheapest & most consistent)
                        return str(r.get("PredMemberTop1", "")) if pd.notna(r.get("PredMemberTop1")) else ""
                    # all-3: choose the most likely member by Top1 prediction if present, else blank
                    return str(r.get("PredMemberTop1", "")) if pd.notna(r.get("PredMemberTop1")) else ""

                ev_sel["ChosenMemberLabel"] = ev_sel.apply(_row_chosen_member_label, axis=1)

                # Map (core, member_label) -> 4-digit BOX number (canonical member string)
                # We reuse members_from_core which returns box-keys (sorted digits), then pick the matching member label.
                def _member_box_for_label(core_key: str, lab: str) -> str | None:
                    core_key = canonical_core_key(core_key)
                    fam_boxes = members_from_core(core_key, "AABC")
                    if lab == "AABC":
                        return str(fam_boxes[0])
                    if lab == "ABBC":
                        return str(fam_boxes[1])
                    if lab == "ABCC":
                        return str(fam_boxes[2])
                    return None

                # Build straight suggestion per day: pick the row with best member predictability (Top1Rate proxy)
                # Use the per_core_df Top1Rate as a cheap, real-data confidence signal.
                top1_rate_map = {
                    str(r["Core"]).zfill(3): float(r["Top1Rate"]) if pd.notna(r["Top1Rate"]) else 0.0
                    for _, r in per_core_df.iterrows()
                }

                sugg_rows = []
                for d in days:
                    day_rows = ev_sel[ev_sel["DateOnly"] == d].copy()
                    if day_rows.empty:
                        continue
                    # choose up to k rows by confidence
                    day_rows["CoreZ"] = day_rows["Core"].astype(str).apply(lambda x: str(x).zfill(3))
                    day_rows["Conf"] = day_rows["CoreZ"].map(top1_rate_map).fillna(0.0)
                    day_rows = day_rows.sort_values(["Conf", rank_col_ap], ascending=[False, True]).head(int(max_straights_per_day))
                    for _, r in day_rows.iterrows():
                        corez = str(r["CoreZ"])
                        lab = str(r.get("ChosenMemberLabel",""))
                        boxnum = _member_box_for_label(corez, lab)
                        if not boxnum:
                            continue
                        # Determine the straight guess from train history (Date < d) within this stream
                        td = pd.to_datetime(d)
                        stream = str(r.get("Stream",""))
                        train = df_all.copy()
                        train["Date"] = pd.to_datetime(train["Date"], errors="coerce")
                        train = train.dropna(subset=["Date"])
                        train = train[train["Date"] < td].copy()
                        if train.empty:
                            continue
                        # same stream exact hits for this member
                        train_s = train[train["Stream"].astype(str) == stream].copy() if "Stream" in train.columns else train.copy()
                        train_s["Result4"] = train_s["Result"].apply(lambda x: extract_4digit(x) or "")
                        train_s = train_s[train_s["Result4"].str.len() == 4].copy()
                        # filter to this member by box_key match
                        train_s["BoxKey"] = train_s["Result4"].apply(box_key)
                        mk = box_key(boxnum)
                        train_m = train_s[train_s["BoxKey"] == mk].copy()
                        straight_pick = None
                        if not train_m.empty:
                            # LAST(stream)
                            train_m = train_m.sort_values("Date")
                            straight_pick = str(train_m["Result4"].iloc[-1])
                        else:
                            # MODE(stream) over the member
                            # (If none in stream, fall back to global)
                            train_g = train.copy()
                            train_g["Result4"] = train_g["Result"].apply(lambda x: extract_4digit(x) or "")
                            train_g = train_g[train_g["Result4"].str.len() == 4].copy()
                            train_g["BoxKey"] = train_g["Result4"].apply(box_key)
                            train_mg = train_g[train_g["BoxKey"] == mk].copy()
                            if not train_mg.empty:
                                straight_pick = str(train_mg["Result4"].value_counts().idxmax())

                        if straight_pick:
                            sugg_rows.append(
                                {
                                    "Date": str(d),
                                    "Stream": stream,
                                    "Core": corez,
                                    "MemberLabel": lab,
                                    "BoxNumber": boxnum,
                                    "StraightPick": straight_pick,
                                    "StraightCost": float(straight_cost),
                                    "BoxCost": float(cost_per_play_ap),
                                }
                            )

                sugg_df = pd.DataFrame(sugg_rows)
                if sugg_df.empty:
                    st.info("No straight booster picks could be generated for the evaluation window under the current settings.")
                else:
                    _safe_st_dataframe(sugg_df.head(200), use_container_width=True, hide_index=True)
                    st.download_button(
                        "Download straight booster picks (CSV)",
                        data=sugg_df.to_csv(index=False).encode("utf-8"),
                        file_name=f"straight_booster_picks_last{int(eval_days)}d.csv",
                        mime="text/csv",
                        key="ap_dl_straight_booster",
                    )

        st.caption(
            "Note: This planner is **evaluation-window based**. Once you like the settings, we will wire the same core+row+member rules into the daily 'Play Today' outputs."
        )

# ----------------------------
# Baseline disk cache (optional)
# ----------------------------
def _baseline_paths(core: str, window_days: int):
    core = str(core).zfill(3)
    base = DISK_CACHE_DIR / f"baseline_{window_days}d_{core}"
    return {
        "stream": base.with_suffix(".stream.parquet"),
        "pos": base.with_suffix(".pos.parquet"),
        "meta": base.with_suffix(".meta.json"),
    }

def _load_baseline_from_disk(core: str, window_days: int, expected_last_date: str | None):
    p = _baseline_paths(core, window_days)
    meta = _read_meta(p["meta"])
    if not meta:
        return None, None, None
    if expected_last_date and meta.get("last_date") != expected_last_date:
        return None, None, meta
    stream_df = _safe_read_table(p["stream"])
    pos_df = _safe_read_table(p["pos"])
    if stream_df is None or pos_df is None:
        return None, None, meta
    return stream_df, pos_df, meta

def _save_baseline_to_disk(core: str, window_days: int, stream_df, pos_df, last_date: str | None):
    p = _baseline_paths(core, window_days)
    _safe_write_table(stream_df, p["stream"])
    _safe_write_table(pos_df, p["pos"])
    _write_meta(p["meta"], {
        "core": str(core).zfill(3),
        "window_days": int(window_days),
        "last_date": last_date,
        "built_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })

def get_stream_stats_cached(core: str, window_days: int, df_all, last_date: str | None):
    key = f"stream_stats::{window_days}::{str(core).zfill(3)}"
    if key in st.session_state:
        return st.session_state[key]
    # Try disk cache
    stream_df, pos_df, meta = _load_baseline_from_disk(core, window_days, expected_last_date=last_date)
    if stream_df is not None:
        st.session_state[key] = stream_df
        # also stash pos if present
        if pos_df is not None:
            st.session_state[f"pos_map::{window_days}::{str(core).zfill(3)}"] = pos_df
        return stream_df
    # Compute fresh
    hits = compute_core_hits(df_all, core, structures=("AABC",))
    stream_df = stream_summary(df_all, hits, window_days=window_days)
    st.session_state[key] = stream_df
    return stream_df


def compute_stream_stats(df_all: pd.DataFrame, core: str, window_days: int | None = None, exclude_md: bool = False) -> pd.DataFrame:
    """Back-compat wrapper used by the Northern Lights block."""
    if window_days is None:
        window_days = 180
    # exclude_md is already applied upstream; kept only for compatibility
    last_date = most_recent_date(df_all)
    last_s = None
    if last_date is not None and not pd.isna(last_date):
        try:
            last_s = str(pd.to_datetime(last_date).date())
        except Exception:
            last_s = None
    return get_stream_stats_cached(core=str(core), window_days=int(window_days), df_all=df_all, last_date=last_s)


def build_northern_star_buckets(
    stats_df: pd.DataFrame,
    stream: str,
    top_n: int = 12,
    due_ranks: Tuple[int, int] = (13, 60),
    seed_core_key: str = "core",
    include_24h: bool = True,
    df_24: Optional[pd.DataFrame] = None,
    core: str = "000",
    **kwargs,
) -> Dict[str, object]:
    """
    Back-compat bucket logic for the Northern Lights table:
    - Base bucket: Top N streams by HitsPerWeek
    - Due bucket: from base ranks [due_from..due_to], take Top cfg.top_due by DaysSinceLastHit
    Returns fields expected by the Northern Lights renderer.
    """
    if stats_df is None or stats_df.empty:
        return {}

    s = stats_df.copy()
    s = s.sort_values(["HitsPerWeek", "HitsWindow"], ascending=[False, False]).reset_index(drop=True)
    s["BaseRank"] = s.index + 1

    # Base top
    base_top_streams = set(s.head(int(top_n))["Stream"].astype(str).tolist())

    # Due candidates are chosen from the *base-ranked* band
    d1, d2 = int(due_ranks[0]), int(due_ranks[1])
    band = s[(s["BaseRank"] >= d1) & (s["BaseRank"] <= d2)].copy()
    if band.empty:
        due_top_streams = set()
    else:
        band = band.sort_values(["DaysSinceLastHit", "HitsPerWeek"], ascending=[False, False])
        due_top_streams = set(band.head(int(getattr(st.session_state.get("_cfg", RankConfig()), "top_due", 8)))["Stream"].astype(str).tolist())

    in_base = str(stream) in base_top_streams
    in_due = str(stream) in due_top_streams

    # Pull row for this stream
    row = s[s["Stream"].astype(str) == str(stream)]
    if row.empty:
        return {}
    r = row.iloc[0]
    hits = _safe_int(r.get("HitsWindow", 0)) or 0
    hpw = float(r.get("HitsPerWeek", 0.0) or 0.0)
    dslh = (_safe_int(r.get("DaysSinceLastHit", 0)) or 0)

    # Due pressure as soft signal
    due_pressure = 0.0
    if in_due:
        due_pressure = float(dslh)

    # Optional 24h soft signal: add a small nudge if this core hit this stream in df_24
    if include_24h and df_24 is not None and not df_24.empty:
        try:
            cache_key = f"_nl_24h_corehits_{core}"
            if cache_key not in st.session_state:
                st.session_state[cache_key] = compute_core_hits(df_24, str(core), structures=["AABC"])
            df_24h_core_hits = st.session_state.get(cache_key, pd.DataFrame())
            if not df_24h_core_hits.empty and (df_24h_core_hits["Stream"].astype(str) == str(stream)).any():
                due_pressure += 1.0
        except Exception:
            pass

    seed_key = canonical_core_key(str(core))

    bucket_label = "Top12" if in_base else ("Due" if in_due else "")
    bucket_pick = "BASE" if in_base else ("DUE" if in_due else "")

    return {
        "Top12": bucket_label,
        "BucketPick": bucket_pick,
        "SeedKey": seed_key,
        "DuePressure": due_pressure,
        "HitsPerWeek": hpw,
        "Hits": hits,
        "DaysSinceLastHit": dslh,
        # Preserve the actual 1..78 position inside this core.
        "RankPos": int(r.get("RankPos", r.get("BaseRank", 9999))) if pd.notna(r.get("RankPos", r.get("BaseRank", 9999))) else 9999,
        "BaseScoreRank": int(r.get("BaseScoreRank", r.get("BaseRank", 9999))) if pd.notna(r.get("BaseScoreRank", r.get("BaseRank", 9999))) else 9999,
    }




def get_pos_map_cached(core: str, window_days: int, stream_stats_df, last_date: str | None):
    k = f"pos_map::{window_days}::{str(core).zfill(3)}"
    if k in st.session_state:
        return st.session_state[k]
    # If stream_stats was loaded from disk, pos may already be in session_state
    pos_df, _ = position_percentile_map(stream_stats_df)
    st.session_state[k] = pos_df
    # Save to disk alongside stream stats (best effort)
    try:
        _save_baseline_to_disk(core, window_days, stream_stats_df, pos_df, last_date)
    except Exception:
        pass
    return pos_df

# Baseline cache builder UI (runs only after history is loaded)
st.subheader("Baseline cache builder")
st.caption("This aggregates the Northern Star bucket picks across your selected cores and ranks them using a universal score (recent strength + due pressure + position-percentile strength).")

build_both = st.checkbox("Build cache for both windows (180 & 365)", value=False)
if st.button("Build baseline cache now"):
    if df_all is None or df_all.empty:
        st.warning("Upload a history file first.")
    else:
        build_windows = [180, 365] if build_both else [window_days]
        built = 0
        for w in build_windows:
            for c in cores_for_cache:
                stream_df = get_stream_stats_cached(c, w, df_all=df_all, last_date=last_all)
                pos_df = get_pos_map_cached(c, w, stream_df, last_date=last_all)
                try:
                    _save_baseline_to_disk(c, w, stream_df, pos_df, last_all)
                    built += 1
                except Exception:
                    pass
        st.success(f"Cache built for {built} core-window combinations. Latest history date: {last_all}")




def _rank_chart_png(stats_df: pd.DataFrame, core_id: str) -> bytes:
    """Portable evaluation chart for one selected core."""
    import io as _io
    try:
        import matplotlib.pyplot as _plt
    except Exception:
        return b""
    _d = stats_df.copy().sort_values("RankPos").head(78)
    _fig = _plt.figure(figsize=(12, 6))
    _ax = _fig.add_subplot(111)
    _ax.plot(_d["RankPos"], pd.to_numeric(_d.get("HitsWindow", 0), errors="coerce").fillna(0))
    _ax.set_title(f"Core {str(core_id).zfill(3)} — hits by stream rank")
    _ax.set_xlabel("Rank position")
    _ax.set_ylabel("Hits in window")
    _ax.grid(True, alpha=0.25)
    _fig.tight_layout()
    _buf = _io.BytesIO()
    _fig.savefig(_buf, format="png", dpi=150)
    _plt.close(_fig)
    return _buf.getvalue()


def _active_member_rules() -> pd.DataFrame:
    """Load the root-level member rule library. Failure is visible, never silent."""
    _path = Path(__file__).resolve().parent / "member_pair_rules_v1.csv"
    if not _path.exists():
        return pd.DataFrame()
    return load_member_pair_rules(_path)


def _apply_member_layer(rows_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Apply member rules to stream/core rows and preserve full firing diagnostics."""
    if rows_df is None or rows_df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    _rules = _active_member_rules()
    if _rules.empty:
        _out = rows_df.copy()
        _out["MemberRecommendedPair"] = "ALL THREE / RULE FILE MISSING"
        _out["MemberRecommendedBoxedMembers"] = ""
        _out["MemberDecisionReason"] = "MEMBER_RULE_FILE_MISSING"
        _out["MemberRulesFired"] = ""
        _out["MemberNoRuleFired"] = True
        return _out, pd.DataFrame(), pd.DataFrame()
    return apply_member_rules(rows_df, _rules)


def _apply_settled_member35_view(rows_df: pd.DataFrame, *, mode: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, object]:
    """Run the same settled core + Member35 pipeline used by Daily/WF for UI previews.

    This deliberately does not call the legacy member-pair engine.  Each surviving
    core position expands to exactly three boxed members, then the locked Member35
    sequential deletion registry decides whether 0, 1, 2, or 3 survive.
    """
    if rows_df is None or rows_df.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), None
    x = rows_df.copy()
    if "UniversalScore" not in x.columns:
        score_source = "NSScore" if "NSScore" in x.columns else ("HitsPerWeek" if "HitsPerWeek" in x.columns else None)
        x["UniversalScore"] = pd.to_numeric(x[score_source], errors="coerce").fillna(0.0) if score_source else 0.0
    if "RankPos" not in x.columns:
        x["RankPos"] = range(1, len(x) + 1)
    if "PlaylistDate" not in x.columns:
        x["PlaylistDate"] = str(st.session_state.get("playlist_date", ""))
    if "SeedlistDate" not in x.columns:
        x["SeedlistDate"] = str(st.session_state.get("seedlist_date", ""))
    if "BucketPick" not in x.columns:
        x["BucketPick"] = ""
    result = run_northern_star_open_gate(x, date_col="PlaylistDate", mode=mode)
    failed = result.handshake[result.handshake["Status"].astype(str).ne("PASS")] if not result.handshake.empty else pd.DataFrame()
    if not failed.empty:
        raise ValueError("Settled Member35 contract failure: " + failed.to_dict("records").__str__())
    return result.member_all.copy(), result.member_fire_audit.copy(), result.stage_summary.copy(), result


def _playable_bucket_mask(df: pd.DataFrame) -> pd.Series:
    """True only for rows selected into an actual playable Northern Star bucket."""
    if df is None or df.empty or "BucketPick" not in df.columns:
        return pd.Series(False, index=getattr(df, "index", []), dtype=bool)
    return df["BucketPick"].astype(str).str.upper().isin(["BASE", "DUE", "COMBINED"])


def _visible_member_columns(df: pd.DataFrame, include_rank: bool = True) -> list[str]:
    cols = []
    if include_rank:
        cols.extend(["PlayRank", "Rank"])
    cols.extend([
        "GlobalBoxedPlayRank", "BoxedScoreTieRank", "First40BoxPlays", "BoxedMember", "BoxedPlayScore",
        "PlaylistDate", "Date", "SeedlistDate", "Stream", "Seed", "Core", "BoxedMember", "DoubleRole",
        "BucketPick", "BaseDueGateMode", "OriginalBaseDueEligible", "BaseScore", "BaseScoreRank",
        "DueIndex", "DueIndexRank", "DaysSinceLastHit", "HitsPerWeek",
        "ProductionEligible", "CoreSettledSurvivor", "MemberSettledSurvivor",
        "MemberFirstDeletingRule", "MemberAllMatchingRules", "MembersBefore", "MembersAfter", "AllMembersDeleted",
        "StreamEliminationRuleID", "StreamEliminationReason", "GlobalEligibleStreamCoreRank", "AuditRowNumber",
        "MemberRecommendedPair", "MemberRecommendedBoxedMembers", "MemberDecisionReason",
        "MemberSelectedRuleID", "MemberRulesFired", "MemberNoRuleFired", "MemberRuleConflict",
        "CoreStreamRank", "GlobalStreamCoreRank", "GlobalCandidateRank", "BundleCost", "GlobalSelected", "GlobalSelectionReason", "CumulativeSelectedBoxedPlays", "PositiveRuleCount", "NegativeRuleCount", "SeedTraitsScore",
        "UniversalScore", "HitsPerWeek", "DaysSinceLastHit", "RankPos",
    ])
    # Preserve the requested display order while preventing duplicate labels.
    # Pandas permits duplicate selections, but Streamlit/PyArrow rejects them.
    visible = []
    seen = set()
    for c in cols:
        if c in df.columns and c not in seen:
            visible.append(c)
            seen.add(c)
    return visible


def _seed_trait_trace_for_row(core: str, stream: str, seed: str) -> tuple[dict, pd.DataFrame]:
    """Return row summary and exploded rule trace for one stream/core/seed."""
    core3=str(core).zfill(3); seed4=re.sub(r"\D","",str(seed or "")).zfill(4)[-4:]
    feats=_feature_values_for_seed(seed4,core3,_last5_union_by_stream.get(str(stream),set()))
    rows=[]
    for pol, lib in [("POSITIVE",seed_traits_pos_df),("NEGATIVE",seed_traits_neg_df)]:
        if lib is None or lib.empty: continue
        sub=lib[pd.to_numeric(lib["core_family"],errors="coerce").fillna(-1).astype(int)==int(core3)]
        for r in sub.itertuples(index=False):
            trait=str(getattr(r,"trait","")); expected=str(getattr(r,"value","")); actual_vals=[str(x) for x in feats.get(trait,[])]
            fired=expected in actual_vals
            lift=float(getattr(r,"lift",1.0) or 1.0)
            contribution = max(0.0, lift - 1.0) if pol == "POSITIVE" else -max(0.0, 1.0 - lift)
            rows.append({"Core":core3,"Stream":stream,"Seed":seed4,"RuleID":getattr(r,"RuleID",""),"RuleSource":getattr(r,"RuleSource",""),"Polarity":pol,"Trait":trait,"ExpectedValue":expected,"ActualValues":"|".join(actual_vals),"Implemented":trait in _IMPLEMENTED_TRAITS,"Fired":bool(fired),"Lift":lift,"RawContribution":contribution if fired else 0.0})
    tr=pd.DataFrame(rows)
    fired=tr[tr["Fired"]] if not tr.empty else tr
    pos=fired[fired["Polarity"]=="POSITIVE"] if not fired.empty else fired
    neg=fired[fired["Polarity"]=="NEGATIVE"] if not fired.empty else fired
    summary={"PositiveRuleCount":len(pos),"NegativeRuleCount":len(neg),"PositiveRuleIDs":"; ".join(pos.get("RuleID",pd.Series(dtype=str)).astype(str)),"NegativeRuleIDs":"; ".join(neg.get("RuleID",pd.Series(dtype=str)).astype(str)),"PositiveRawLift":float(pos.get("RawContribution",pd.Series(dtype=float)).sum()) if not pos.empty else 0.0,"NegativeRawLift":float(-neg.get("RawContribution",pd.Series(dtype=float)).sum()) if not neg.empty else 0.0}
    return summary,tr

def _expand_boxed_plays(df: pd.DataFrame) -> pd.DataFrame:
    """Expand recommended members to one globally rankable boxed play per row.

    No core, stream, bundle, or spending quota is applied. Members from the same
    stream/core remain separate boxed plays and may tie when the evidence does not
    distinguish between them.
    """
    if df is None or df.empty:
        return pd.DataFrame()
    # Already expanded: preserve it rather than expanding a second time.
    if "BoxedMember" in df.columns:
        z=df.copy()
    else:
        out=[]
        for r in df.to_dict("records"):
            vals=[x for x in re.findall(r"\d{4}",str(r.get("MemberRecommendedBoxedMembers", "")))]
            if not vals:
                try: vals=members_from_core(str(r.get("Core","")),"AABC")
                except Exception: vals=[]
            for cm,bx in enumerate(vals):
                q=dict(r)
                q["CanonicalMemberSlotInRecommendation"]=cm
                q["BoxedMember"]=bx
                out.append(q)
        z=pd.DataFrame(out)
    if z.empty:
        return z
    z["BoxedPlayScore"]=pd.to_numeric(z.get("UniversalScore",0.0),errors="coerce").fillna(0.0)
    sort_cols=[c for c in ["BoxedPlayScore","HitsPerWeek","DuePressure","RankPos","Stream","Core","BoxedMember"] if c in z.columns]
    asc=[]
    for c in sort_cols:
        asc.append(False if c in {"BoxedPlayScore","HitsPerWeek","DuePressure"} else True)
    z=z.sort_values(sort_cols,ascending=asc,kind="mergesort").reset_index(drop=True)
    # Idempotent ranking: this function may receive rows already expanded/ranked.
    # Assign instead of insert so repeated calls cannot crash with "already exists".
    z["GlobalBoxedPlayRank"] = range(1, len(z) + 1)
    rank_col = z.pop("GlobalBoxedPlayRank")
    z.insert(0, "GlobalBoxedPlayRank", rank_col)
    z["BoxedScoreTieRank"]=z["BoxedPlayScore"].rank(method="min",ascending=False).astype(int)
    z["First40BoxPlays"]=z["GlobalBoxedPlayRank"]<=40
    # Backward-compatible alias used by old straight/audit code.
    z["BoxedPlayNumber"]=z["GlobalBoxedPlayRank"]
    z["ActualBoxedPlayNumber"]=z["GlobalBoxedPlayRank"]
    return z


def _rank_global_boxed_plays(rows_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return all stream/core rows for audit and all individual boxed plays globally ranked."""
    if rows_df is None or rows_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    audit=rows_df.copy()
    audit=audit.sort_values(["UniversalScore","HitsPerWeek","DuePressure","RankPos"],ascending=[False,False,False,True],kind="mergesort").reset_index(drop=True)
    audit["GlobalStreamCoreRank"]=audit.index+1
    audit["CoreStreamRank"]=pd.to_numeric(audit.get("RankPos",9999),errors="coerce").fillna(9999).astype(int)
    boxed=_expand_boxed_plays(audit)
    return boxed,audit


def _select_global_play_bundles(rows_df: pd.DataFrame, max_boxed_plays: int = 50) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Select the strongest complete member bundles globally under an actual boxed-play budget.

    No core receives a quota or guaranteed representation. Rows compete globally by score.
    A row's full recommended 2- or 3-member bundle is kept intact; it is never split merely
    to fill the final remaining budget slots.
    """
    if rows_df is None or rows_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    work = rows_df.copy()
    work["BundleMembers"] = work["MemberRecommendedBoxedMembers"].astype(str).map(
        lambda x: re.findall(r"\d{4}", x)
    )
    work["BundleCost"] = work["BundleMembers"].map(len)
    # Defensive fallback: canonical AABC core always has three boxed members.
    def _fallback_cost(r):
        if int(r.get("BundleCost", 0) or 0) > 0:
            return int(r.get("BundleCost"))
        try:
            return len(members_from_core(str(r.get("Core", "")), "AABC"))
        except Exception:
            return 3
    work["BundleCost"] = work.apply(_fallback_cost, axis=1).astype(int)
    work = work.sort_values(
        ["UniversalScore", "HitsPerWeek", "DuePressure", "RankPos"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)
    work["GlobalCandidateRank"] = work.index + 1
    used = 0
    selected_flags = []
    reasons = []
    cumulative = []
    cap = max(1, int(max_boxed_plays))
    for r in work.to_dict("records"):
        cost = max(1, int(r.get("BundleCost", 3) or 3))
        if used + cost <= cap:
            used += cost
            selected_flags.append(True)
            reasons.append("SELECTED_GLOBAL_SCORE_WITHIN_BOXED_BUDGET")
        else:
            selected_flags.append(False)
            reasons.append("NOT_SELECTED_BOXED_BUDGET_EXHAUSTED")
        cumulative.append(used)
    work["GlobalSelected"] = selected_flags
    work["GlobalSelectionReason"] = reasons
    work["CumulativeSelectedBoxedPlays"] = cumulative
    selected = work[work["GlobalSelected"]].copy().reset_index(drop=True)
    selected.insert(0, "PlayRank", selected.index + 1)
    selected["GlobalStreamCoreRank"] = selected["GlobalCandidateRank"]
    selected["CoreStreamRank"] = pd.to_numeric(selected.get("RankPos", 9999), errors="coerce").fillna(9999).astype(int)
    return selected, work

def render_straights_shortlist(playable_df: pd.DataFrame, history_df: Optional[pd.DataFrame]=None, per_box: int=2) -> tuple[pd.DataFrame,pd.DataFrame]:
    """Create a history-ranked straight shortlist without rescanning all history per candidate.

    v51.25 runtime repair: historical global and stream/result counts are computed once,
    then each straight candidate uses O(1) dictionary lookups. The prior implementation
    repeatedly scanned the entire history table for every boxed play and every permutation.
    """
    boxed = _expand_boxed_plays(playable_df)
    audit = []
    hist = history_df if history_df is not None else pd.DataFrame()

    global_counts = {}
    stream_result_counts = {}
    if hist is not None and not hist.empty and "Result" in hist.columns:
        h = hist[[c for c in ["Stream", "Result"] if c in hist.columns]].copy()
        h["_Result4"] = h["Result"].astype(str).str.replace(r"\D", "", regex=True).str[-4:].str.zfill(4)
        global_counts = h["_Result4"].value_counts(dropna=False).to_dict()
        if "Stream" in h.columns:
            h["_StreamKey"] = h["Stream"].astype(str)
            stream_result_counts = h.groupby(["_StreamKey", "_Result4"], dropna=False).size().to_dict()

    for r in boxed.to_dict("records"):
        bx = str(r.get("BoxedMember", ""))
        stream = str(r.get("Stream", ""))
        for st4 in unique_straights_for_box(bx):
            audit.append({
                **r,
                "Straight": st4,
                "StreamPriorHits": int(stream_result_counts.get((stream, st4), 0)),
                "GlobalPriorHits": int(global_counts.get(st4, 0)),
            })
    ad=pd.DataFrame(audit)
    if not ad.empty:
        _box_key = "GlobalBoxedPlayRank" if "GlobalBoxedPlayRank" in ad.columns else ("BoxedPlayNumber" if "BoxedPlayNumber" in ad.columns else None)
        if _box_key is None:
            raise ValueError("Straight shortlist requires GlobalBoxedPlayRank or BoxedPlayNumber. Available columns: " + ", ".join(map(str, ad.columns)))
        ad=ad.sort_values([_box_key,"StreamPriorHits","GlobalPriorHits","Straight"],ascending=[True,False,False,True])
        ad["StraightRankWithinBox"]=ad.groupby(_box_key).cumcount()+1
        ad["SelectedStraight"]=ad["StraightRankWithinBox"]<=int(per_box)
        sel=ad[ad["SelectedStraight"]].copy().reset_index(drop=True); sel.insert(0,"StraightPlayNumber",range(1,len(sel)+1))
    else: sel=pd.DataFrame()
    return sel,ad

# ===============================
# Main tabs
# ===============================

tab_labels = ["Northern Star (v51)", "Northern Lights (Master playlist)", "Core view", "Backtest (optional)", "Core Set Lab"]
tabs = st.tabs(tab_labels)
_t_ns = tabs[0]
_t_nl = tabs[1]
_t_core = tabs[2]
_t_bt = tabs[3]
_t_lab = tabs[4]

# --- Northern Lights master playlist (best -> worst across streams/cores) ---
if _t_nl is None:
    _t_nl = st.container()
with _t_nl:
    st.subheader("Northern Lights master playlist")
    st.caption("All eligible stream/core candidates compete globally. Per-core buckets are research evidence only; no core receives a quota.")
    # Production tabs are locked to the active selected cores. The Lab alone may access the broader catalog.
    nl_use_all_cores = False
    st.caption("Output lock: Northern Lights uses only the currently selected cores.")

    # Ensure cores_for_cache is always defined (selected cores for cache building / views)
    cores_for_cache = _wf_first_nonempty_selection(st.session_state.get('cores_for_cache'), st.session_state.get('cores_for_cache_ms'))
    cores_for_cache = [str(c).zfill(3) for c in cores_for_cache if str(c).zfill(3) in cores]
    if not cores_for_cache:
        st.info("Select one or more cores above to populate the playlist.")
    else:
        cfg = st.session_state.get("_cfg", RankConfig())
        include_24h = bool(st.session_state.get("include_24h", True))
        st.caption("Production output has no automatic cutoff: every individual boxed play is globally ranked. First40BoxPlays is a viewing marker only.")

        # Build a master list: (core, stream) -> universal score
        # v51: In ALL-CORES mode, enforce strict cache-only for safety/performance.
        stats_by_core: Dict[str, pd.DataFrame] = {}
        if nl_use_all_cores:
            expected_last = last_all if isinstance(last_all, str) else None
            missing = []
            for _c in cores_for_cache:
                ss, _pos_df, _meta = _load_baseline_from_disk(_c, cfg.window_days, expected_last_date=expected_last)
                if ss is None or ss.empty:
                    missing.append(_c)
                else:
                    stats_by_core[_c] = ss
            if missing:
                st.error("ALL-CORES mode is cache-only. Missing baseline caches for: " + ", ".join(missing))
                st.caption("Build caches in the Cache Builder section, then rerun.")
                st.stop()

        rows = []
        for core in cores_for_cache:
            try:
                stats_df = stats_by_core.get(core) if nl_use_all_cores else compute_stream_stats(df_all, core, window_days=window_days, exclude_md=False)
            except Exception:
                stats_df = pd.DataFrame()

            if stats_df is None or stats_df.empty:
                continue

            for stream in stats_df["Stream"].astype(str).tolist():
                meta = build_northern_star_buckets(
                    stats_df=stats_df,
                    stream=stream,
                    top_n=cfg.top_base,
                    due_ranks=(cfg.due_from_rank, cfg.due_to_rank),
                    seed_core_key=canonical_core_key(str(core)),
                    include_24h=include_24h,
                    df_24=df_24h,
                    core=str(core),
                )
                if not meta:
                    continue

                # UniversalScore is what we rank by in the playlist
                # (recent strength + due pressure + position-percentile strength)
                # Position-percentile strength comes from the cached pos map
                try:
                    # Per-stream RankPos percentile map (cache-backed)
                    last_s = None
                    try:
                        last_s = most_recent_date_for_stream(df_all, stream)
                    except Exception:
                        last_s = None

                    pos_df = get_pos_map_cached(str(core), int(window_days), stats_df, last_date=last_s)

                    # Position strength (by RankPos)
                    try:
                        rankpos = int(meta.get("RankPos", 9999) or 9999)
                    except Exception:
                        rankpos = 9999
                    p = 0.0
                    if pos_df is not None and not pos_df.empty and "RankPos" in pos_df.columns:
                        try:
                            _strength_col = "PctStrength" if "PctStrength" in pos_df.columns else ("HitCountPctile" if "HitCountPctile" in pos_df.columns else None)
                            if _strength_col:
                                _m = pos_df[pos_df["RankPos"].astype(int) == int(rankpos)]
                                if not _m.empty:
                                    p = float(_m.iloc[0][_strength_col])
                        except Exception:
                            p = 0.0

                    # Base signals from bucket meta
                    hits_pw = float(meta.get("HitsPerWeek", 0.0) or 0.0)
                    days_since = float(meta.get("DaysSinceLastHit", 0.0) or 0.0)
                    due_bucket_pressure = float(meta.get("DuePressure", 0.0) or 0.0)

                    # Seed Traits score (soft)
                    seed = _prev_seed_by_stream.get(str(stream))
                    seed_score = 0.0
                    _trace_summary, _trace_df = _seed_trait_trace_for_row(str(core), str(stream), seed)
                    if st.session_state.get("enable_seed_traits", True) and seed_traits_pos_lookup:
                        seed_score, _seed_matches = compute_seed_traits_score(
                            str(core),
                            seed,
                            str(stream),
                            pos_lookup=seed_traits_pos_lookup,
                            neg_lookup=seed_traits_neg_lookup,
                            last5_union_digits_by_stream=_last5_union_by_stream,
                        )

                    # Cadence score (soft) — mean gap baseline from window hits
                    try:
                        _total_hits = float(stats_df["HitsWindow"].sum()) if "HitsWindow" in stats_df.columns else 0.0
                    except Exception:
                        _total_hits = 0.0
                    mean_gap_days = (window_days / _total_hits) if _total_hits > 0 else 0.0
                    cadence_score = (
                        compute_cadence_score(days_since, mean_gap_days)
                        if (st.session_state.get("enable_cadence", True) and mean_gap_days > 0)
                        else 0.0
                    )

                    # Universal score (soft additive; no eliminations)
                    due_w = float(st.session_state.get("due_weight", 0.20))
                    pos_w = float(st.session_state.get("pos_weight", 0.25))
                    st_w = float(st.session_state.get("seed_traits_weight", 0.35))
                    cad_w = float(st.session_state.get("cadence_weight", 0.25))

                    universal = (
                        hits_pw
                        + (min(days_since, 50.0) * 0.01 * due_w)
                        + (p * 0.01 * pos_w)
                        + (seed_score * st_w if st.session_state.get("enable_seed_traits", True) else 0.0)
                        + (cadence_score * cad_w if st.session_state.get("enable_cadence", True) else 0.0)
                    )

                    rows.append({
                        "Core": str(core),
                        "Stream": str(stream),
                        "BucketPick": str(meta.get("BucketPick", "")),
                        "UniversalScore": float(universal),
                        "HitsPerWeek": float(hits_pw),
                        "DaysSinceLastHit": float(days_since),
                        "DueBucketPressure": float(due_bucket_pressure),
                        "DuePressure": float(due_bucket_pressure),
                        "PctStrength": float(p),
                        "Seed": seed,
                        "SeedTraitsScore": float(seed_score),
                        "ScoreBeforeTraits": float(universal - (seed_score * st_w if st.session_state.get("enable_seed_traits", True) else 0.0)),
                        "ScoreAfterTraits": float(universal),
                        **_trace_summary,
                        "CadenceScore": float(cadence_score),
                        "TriggerBoost": float(meta.get("TriggerBoost", 0.0) or 0.0),
                        "Hits": float(meta.get("Hits", 0.0) or 0.0),
                        "RankPos": int(rankpos) if isinstance(rankpos, int) else int(meta.get("RankPos", 9999) or 9999),
                        "BaseScore": float(meta.get("BaseScore", 0.0) or 0.0),
                        "DueIndex": float(meta.get("DueIndex", 0.0) or 0.0),
                    })
                except Exception:
                    # Fallback: still emit a row without the position/traits features
                    try:
                        hits_pw = float(meta.get("HitsPerWeek", 0.0) or 0.0)
                    except Exception:
                        hits_pw = 0.0
                    try:
                        days_since = float(meta.get("DaysSinceLastHit", 0.0) or 0.0)
                    except Exception:
                        days_since = 0.0
                    rows.append({
                        "Core": str(core),
                        "Stream": str(stream),
                        "BucketPick": str(meta.get("BucketPick", "")),
                        "UniversalScore": float(hits_pw),
                        "HitsPerWeek": float(hits_pw),
                        "DaysSinceLastHit": float(days_since),
                        "DueBucketPressure": float(meta.get("DuePressure", meta.get("DueBucketPressure", 0.0)) or 0.0),
                        "DuePressure": float(meta.get("DuePressure", meta.get("DueBucketPressure", 0.0)) or 0.0),
                        "PctStrength": 0.0,
                        "Seed": _prev_seed_by_stream.get(str(stream)),
                        "SeedTraitsScore": 0.0,
                        "CadenceScore": 0.0,
                        "TriggerBoost": float(meta.get("TriggerBoost", 0.0) or 0.0),
                        "Hits": float(meta.get("Hits", 0.0) or 0.0),
                        "RankPos": int(meta.get("RankPos", 9999) or 9999),
                        "BaseScore": float(meta.get("BaseScore", 0.0) or 0.0),
                        "DueIndex": float(meta.get("DueIndex", 0.0) or 0.0),
                    })

        if not rows:
            st.warning("No playlist rows were produced. Double-check that your history file contains your selected cores in AABC structure.")
        else:
            nl_df = pd.DataFrame(rows)
            _trace_frames=[]
            for _rr in nl_df[["Core","Stream","Seed"]].to_dict("records"):
                _,_td=_seed_trait_trace_for_row(_rr["Core"],_rr["Stream"],_rr["Seed"])
                if not _td.empty: _trace_frames.append(_td)
            stream_core_rule_trace_df=_safe_pd_concat(_trace_frames,ignore_index=True) if _trace_frames else pd.DataFrame()
            # Optional: Trigger Map boost (soft weighting) for the fixed 39-play list
            apply_trigger_map = st.session_state.get("_apply_trigger_map", False)
            trigger_boost_points = float(st.session_state.get("_trigger_boost_points", 2.0) or 2.0)
            if apply_trigger_map and df_24 is not None and not df_24.empty and "BucketPick" in nl_df.columns:
                try:
                    df_prev = df_24.copy()
                    # Use the last row per Stream as "previous winner" for that stream
                    if "Date" in df_prev.columns:
                        df_prev["_DateSort"] = pd.to_datetime(df_prev["Date"], errors="coerce")
                        df_prev = df_prev.sort_values(["Stream", "_DateSort"])
                    else:
                        df_prev = df_prev.sort_values(["Stream"])
                    prev_map = df_prev.groupby("Stream")["Result"].last().to_dict() if "Result" in df_prev.columns else {}
                    nl_df["PrevResult"] = nl_df["Stream"].map(prev_map).fillna("")
                    nl_df["TriggerBoost"] = nl_df.apply(
                        lambda r: trigger_map_boost(str(r.get("BucketPick","")), str(r.get("PrevResult","")), boost_points=trigger_boost_points),
                        axis=1,
                    )
                    nl_df["UniversalScore"] = nl_df["UniversalScore"].astype(float) + nl_df["TriggerBoost"].astype(float)
                except Exception:
                    # Never break the playlist if trigger map cannot apply
                    pass

            # Ensure DuePressure exists (legacy compatibility)
            if "DuePressure" not in nl_df.columns:
                if "DueBucketPressure" in nl_df.columns:
                    nl_df["DuePressure"] = nl_df["DueBucketPressure"].astype(float)
                else:
                    nl_df["DuePressure"] = 0.0

            nl_df = nl_df.sort_values(["UniversalScore", "HitsPerWeek", "DuePressure"], ascending=[False, False, False]).reset_index(drop=True)
            nl_df.insert(0, "Rank", nl_df.index + 1)
            # Required daily contract: dates and exact stream seed travel with every playlist row.
            nl_df.insert(1, "PlaylistDate", str(st.session_state.get("playlist_date", "")))
            nl_df.insert(2, "SeedlistDate", str(st.session_state.get("seedlist_date", "")))
            if "Seed" not in nl_df.columns:
                nl_df["Seed"] = nl_df["Stream"].astype(str).map(_prev_seed_by_stream)
            nl_df["Seed"] = nl_df["Seed"].map(lambda _x: (re.sub(r"\D", "", str(_x or ""))[-4:]).zfill(4) if re.sub(r"\D", "", str(_x or "")) else "")
            # Shared Daily/WF settled pipeline. BASE/DUE is retained as a feature/audit label only.
            # It must not eliminate stream/core rows before settled core and member evaluation.
            nl_df["OriginalBaseDueEligible"] = _playable_bucket_mask(nl_df).astype(bool)
            nl_df["BaseDueGateMode"] = BASE_DUE_GATE_MODE
            nl_df["ProductionEligible"] = True
            settled_daily = run_northern_star_open_gate(
                nl_df, date_col="PlaylistDate", mode="DAILY_PRODUCTION_OPEN_GATE_MEMBER35_FINAL"
            )
            _failed_contracts = settled_daily.handshake[settled_daily.handshake["Status"].astype(str).ne("PASS")] if not settled_daily.handshake.empty else pd.DataFrame()
            if not _failed_contracts.empty:
                st.error("SETTLED PIPELINE CONTRACT FAILURE — final playlist blocked.")
                _safe_st_dataframe(_failed_contracts, width="stretch")
                st.stop()
            nl_member_all = settled_daily.member_all.copy()
            nl_member_fire_audit = settled_daily.member_fire_audit.copy()
            nl_member_fire_summary = settled_daily.stage_summary.copy()
            nl_final, _eligible_audit = _rank_global_boxed_plays(settled_daily.member_survivors.copy())
            nl_full_audit = settled_daily.core_all.sort_values(
                ["CoreSettledSurvivor", "UniversalScore", "HitsPerWeek", "DuePressure", "RankPos"],
                ascending=[False, False, False, False, True],
                kind="mergesort",
            ).reset_index(drop=True)
            nl_full_audit["ProductionEligible"] = nl_full_audit["CoreSettledSurvivor"].astype(bool)
            nl_full_audit["StreamEliminationRuleID"] = nl_full_audit["CoreFirstDeletingRule"]
            nl_full_audit["StreamEliminationReason"] = np.where(
                nl_full_audit["CoreSettledSurvivor"],
                "SURVIVED_SHARED_SETTLED_95_45",
                "REMOVED_BY_SHARED_SETTLED_CORE_PIPELINE",
            )
            nl_full_audit["AuditRowNumber"] = nl_full_audit.index + 1
            nl_full_audit["CoreStreamRank"] = pd.to_numeric(
                nl_full_audit.get("RankPos", 9999), errors="coerce"
            ).fillna(9999).astype(int)
            nl_full_audit["GlobalEligibleStreamCoreRank"] = pd.NA
            if not _eligible_audit.empty:
                _rank_map = {
                    (str(r.Core), str(r.Stream)): int(r.GlobalStreamCoreRank)
                    for r in _eligible_audit.itertuples(index=False)
                }
                nl_full_audit["GlobalEligibleStreamCoreRank"] = [
                    _rank_map.get((str(c), str(st)), pd.NA)
                    for c, st in zip(nl_full_audit["Core"], nl_full_audit["Stream"])
                ]
            nl_boxed_plays = nl_final.copy()

            st.session_state["nl_df_current"] = nl_final.copy()
            st.session_state["nl_full_ranking_audit"] = nl_full_audit.copy()
            st.session_state["nl_member_fire_audit"] = nl_member_fire_audit.copy()
            st.session_state["nl_member_fire_summary"] = nl_member_fire_summary.copy()
            st.session_state["settled_daily_result"] = settled_daily

            st.markdown(
                f"**PLAYLIST DATE:** `{st.session_state.get('playlist_date', '')}`  "
                f"| **SEEDLIST DATE:** `{st.session_state.get('seedlist_date', '')}`"
            )
            st.success(
                "BASE/DUE is OPEN and FEATURE-ONLY in this production run. Every qualified stream/core row advances "
                "to Pick3-95 and Core45; surviving cores expand to members and Member35 performs member reduction. "
                "First40BoxPlays is only a budget-view marker; no surviving play is removed."
            )
            st.subheader("Global single boxed-play ranking — most likely to least likely")
            _ranked_visible = nl_final[_visible_member_columns(nl_final)].copy()
            st.caption(f"Complete ranked list: {len(_ranked_visible):,} boxed plays. Use the table toolbar/full-screen icon or the filters below; every row is also included in the CSV/TXT downloads.")
            _rank_height = 900
            _safe_st_dataframe(
                _ranked_visible,
                width="stretch",
                height=_rank_height,
            )

            with st.expander("Search the complete global ranked list", expanded=True):
                _fc1, _fc2, _fc3 = st.columns(3)
                with _fc1:
                    _rank_stream_filter = st.text_input("Stream contains", value="", key="global_rank_stream_filter")
                with _fc2:
                    _rank_core_filter = st.text_input("Core", value="", key="global_rank_core_filter")
                with _fc3:
                    _rank_member_filter = st.text_input("Boxed member", value="", key="global_rank_member_filter")
                _rank_filtered = _ranked_visible.copy()
                if _rank_stream_filter.strip() and "Stream" in _rank_filtered.columns:
                    _rank_filtered = _rank_filtered[_rank_filtered["Stream"].astype(str).str.contains(_rank_stream_filter.strip(), case=False, na=False)]
                if _rank_core_filter.strip() and "Core" in _rank_filtered.columns:
                    _rank_filtered = _rank_filtered[_rank_filtered["Core"].astype(str).str.zfill(3).eq(re.sub(r"\D", "", _rank_core_filter).zfill(3))]
                if _rank_member_filter.strip() and "BoxedMember" in _rank_filtered.columns:
                    _rank_filtered = _rank_filtered[_rank_filtered["BoxedMember"].astype(str).str.zfill(4).eq(re.sub(r"\D", "", _rank_member_filter).zfill(4))]
                st.write(f"Matching ranked plays: **{len(_rank_filtered):,}**")
                _safe_st_dataframe(_rank_filtered, width="stretch", height=700)

            with st.expander("Exact stream/core/member firing and no-fire audit", expanded=True):
                _ac1, _ac2, _ac3 = st.columns(3)
                with _ac1:
                    _audit_stream = st.text_input("Audit stream contains", value="", key="exact_audit_stream")
                with _ac2:
                    _audit_core = st.text_input("Audit core", value="", key="exact_audit_core")
                with _ac3:
                    _audit_member = st.text_input("Audit boxed member", value="", key="exact_audit_member")
                _audit_core_digits = re.sub(r"\D", "", _audit_core)
                _audit_member_digits = re.sub(r"\D", "", _audit_member)
                _audit_core3 = _audit_core_digits.zfill(3)[-3:] if _audit_core_digits else ""
                _audit_member4 = _audit_member_digits.zfill(4)[-4:] if _audit_member_digits else ""
                _audit_has_filter = bool(_audit_stream.strip() or _audit_core3 or _audit_member4)
                _core_trace = settled_daily.core_all.copy()
                if not _audit_has_filter:
                    st.info("Enter a stream, core, or boxed member to inspect its exact settled-rule path. No stream or core is monitored by default.")
                    _core_trace = pd.DataFrame()
                elif not _core_trace.empty:
                    if _audit_core3:
                        _core_trace = _core_trace[_core_trace["Core"].astype(str).str.zfill(3).eq(_audit_core3)]
                    if _audit_stream.strip():
                        _core_trace = _core_trace[_core_trace["Stream"].astype(str).str.contains(_audit_stream.strip(), case=False, na=False)]
                if _audit_has_filter and _core_trace.empty:
                    st.warning("No stream/core row matches this audit search in the Northern Star candidate universe.")
                elif _audit_has_filter:
                    _core_cols = [c for c in ["Date","Stream","Core","Seed","BucketPick","ProductionEligibleBeforeSettled","DeletedByEarlyCore","DeletedBeforeCore45","CoreSettledSurvivor","CoreFirstDeletingRule","CoreAllMatchingRules","UniversalScore","RankPos"] if c in _core_trace.columns]
                    st.markdown("**Stream/core stage result**")
                    _safe_st_dataframe(_core_trace[_core_cols], width="stretch", height=min(700, max(180, 38 * (len(_core_trace) + 1))))
                    for _cr in _core_trace.itertuples(index=False):
                        _pk = str(getattr(_cr, "PositionKey", ""))
                        _eligible = bool(getattr(_cr, "ProductionEligibleBeforeSettled", False))
                        _survived = bool(getattr(_cr, "CoreSettledSurvivor", False))
                        if not _eligible:
                            st.error(f"{getattr(_cr, 'Stream', '')} / core {_audit_core3} was unexpectedly ineligible under OPEN_GATE_FEATURE_ONLY. This is a contract failure.")
                        elif not _survived:
                            st.warning(f"The stream/core entered the settled pipeline but was deleted before member expansion. First deleting rule: {getattr(_cr, 'CoreFirstDeletingRule', '') or 'not recorded'}")
                        else:
                            st.success("The stream/core survived to three-member expansion.")
                        _core_events = settled_daily.core_fire_audit.copy()
                        if not _core_events.empty and _pk:
                            _core_events = _core_events[_core_events["PositionKey"].astype(str).eq(_pk)]
                        st.markdown("**Core rules that fired**")
                        if not _core_events.empty:
                            _safe_st_dataframe(_core_events, width="stretch", height=min(700, max(180, 38 * (len(_core_events) + 1))))
                        else:
                            st.info("No settled core rule fired for this row.")
                        _all_core_matches = set(filter(None, str(getattr(_cr, "CoreAllMatchingRules", "")).split("|")))
                        _core_registry = settled_daily.registry[settled_daily.registry["Stage"].astype(str).isin(["PICK3_95","CORE45"])].copy()
                        if not _core_registry.empty:
                            _core_registry["FiredForThisRow"] = _core_registry["RuleID"].astype(str).isin(_all_core_matches)
                            st.markdown("**Core rule registry — fired and did not fire**")
                            _safe_st_dataframe(_core_registry[[c for c in ["Stage","Step","RuleID","RuleName","ApplicableIf","Expression","FiredForThisRow"] if c in _core_registry.columns]], width="stretch", height=700)

                    _member_trace = settled_daily.member_all.copy()
                    if not _member_trace.empty:
                        if _audit_core3:
                            _member_trace = _member_trace[_member_trace["Core"].astype(str).str.zfill(3).eq(_audit_core3)]
                        if _audit_member4:
                            _member_trace = _member_trace[_member_trace["BoxedMember"].astype(str).str.zfill(4).eq(_audit_member4)]
                        if _audit_stream.strip():
                            _member_trace = _member_trace[_member_trace["Stream"].astype(str).str.contains(_audit_stream.strip(), case=False, na=False)]
                    st.markdown("**Member stage result**")
                    if _member_trace.empty:
                        if _audit_member4:
                            st.info(f"Member {_audit_member4} was not created or did not match this search. Check the core-stage result above for pre-expansion deletion.")
                        else:
                            st.info("No member rows match this audit search. Check whether the parent core survived to member expansion.")
                    else:
                        _member_cols = [c for c in ["Date","Stream","Core","BoxedMember","MemberSettledSurvivor","MemberFirstDeletingRule","MemberAllMatchingRules","DoubledDigit","DoubledDigitInSeed","MemberSeedSumDelta","CoreLoad","StreamLoad"] if c in _member_trace.columns]
                        _safe_st_dataframe(_member_trace[_member_cols], width="stretch", height=min(700, max(180, 38 * (len(_member_trace) + 1))))
                        _member_keys = set(_member_trace.get("MemberKey", pd.Series(dtype=str)).astype(str))
                        _member_events = settled_daily.member_fire_audit.copy()
                        if not _member_events.empty and _member_keys:
                            _member_events = _member_events[_member_events["MemberKey"].astype(str).isin(_member_keys)]
                        st.markdown("**Member35 rules that fired**")
                        if not _member_events.empty:
                            _safe_st_dataframe(_member_events, width="stretch", height=min(700, max(180, 38 * (len(_member_events) + 1))))
                        else:
                            st.info("No Member35 rule fired for this member.")
                        _all_member_matches = set()
                        for _v in _member_trace.get("MemberAllMatchingRules", pd.Series(dtype=str)).astype(str):
                            _all_member_matches.update(filter(None, _v.split("|")))
                        _member_registry = settled_daily.registry[settled_daily.registry["Stage"].astype(str).eq("MEMBER35")].copy()
                        if not _member_registry.empty:
                            _member_registry["FiredForThisMember"] = _member_registry["RuleID"].astype(str).isin(_all_member_matches)
                            st.markdown("**Member35 registry — fired and did not fire**")
                            _safe_st_dataframe(_member_registry[[c for c in ["Stage","Step","RuleID","RuleName","ApplicableIf","Expression","FiredForThisMember"] if c in _member_registry.columns]], width="stretch", height=700)

            with st.expander("Full selected-core audit — includes eliminated rows", expanded=False):
                st.caption("Audit only. OriginalBaseDueEligible preserves the old BASE/DUE result; BaseDueGateMode is OPEN_GATE_FEATURE_ONLY. ProductionEligible stays true until a settled core rule removes the row.")
                _safe_st_dataframe(
                    nl_full_audit[_visible_member_columns(nl_full_audit, include_rank=True)],
                    width="stretch",
                    height=520,
                )

            # Machine CSV + printable text contain playable rows only.
            _playlist_cols = _visible_member_columns(nl_final)
            _playlist_export = nl_final[_playlist_cols].copy()
            _print_lines = [
                "PICK 4 NORTHERN LIGHTS — FINAL PLAYABLE PLAYLIST",
                f"BUILD: {APP_VERSION}",
                f"PLAYLIST DATE: {st.session_state.get('playlist_date', '')}",
                f"SEEDLIST DATE / HISTORY THROUGH: {st.session_state.get('seedlist_date', '')}",
                "ALL INDIVIDUAL BOXED PLAYS ARE RANKED BELOW; NO AUTOMATIC CUTOFF.",
                "",
                "RANK | STREAM | SEED | CORE | BOXED MEMBER | SCORE | FIRST 40 | RULE",
                "-" * 150,
            ]
            for _r in _playlist_export.itertuples(index=False):
                _d = _r._asdict()
                _print_lines.append(
                    f"{int(_d.get('GlobalBoxedPlayRank', 0)):03d} | {_d.get('Stream', '')} | "
                    f"{str(_d.get('Seed', '')).zfill(4)} | {str(_d.get('Core', '')).zfill(3)} | "
                    f"{_d.get('BoxedMember', '')} | {float(_d.get('BoxedPlayScore', 0.0)):.6f} | "
                    f"{bool(_d.get('First40BoxPlays', False))} | {_d.get('MemberDecisionReason', '')}"
                )
            _printable_txt = "\n".join(_print_lines) + "\n"

            # BAT/local runs self-store every completed Daily production result. Browser
            # downloads remain available, but they are not the only copy.
            _out_dir = Path.cwd() / "OUTPUTS"
            _out_dir.mkdir(parents=True, exist_ok=True)
            _safe_play_date = re.sub(r"[^0-9A-Za-z_-]+", "-", str(st.session_state.get("playlist_date", "UNKNOWN")))
            _run_tag = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            _base_name = f"DAILY_{_safe_play_date}_V5145_{_run_tag}"
            try:
                _playlist_export.to_csv(_out_dir / f"{_base_name}_FINAL_PLAYLIST.csv", index=False)
                (_out_dir / f"{_base_name}_FINAL_PLAYLIST.txt").write_text(_printable_txt, encoding="utf-8")
                _daily_zip_payload = {
                    "00_FINAL_PLAYLIST.csv": _playlist_export,
                    "01_FINAL_PLAYLIST_PRINTABLE.txt": _printable_txt,
                    "02_CORE_ALL.csv": settled_daily.core_all,
                    "03_CORE_SURVIVORS.csv": settled_daily.core_survivors,
                    "04_MEMBER_ALL.csv": settled_daily.member_all,
                    "05_MEMBER_SURVIVORS.csv": settled_daily.member_survivors,
                    "06_CORE_RULE_FIRE.csv": settled_daily.core_fire_audit,
                    "07_MEMBER35_RULE_FIRE.csv": settled_daily.member_fire_audit,
                    "08_STAGE_SUMMARY.csv": settled_daily.stage_summary,
                    "09_HANDSHAKE.csv": settled_daily.handshake,
                    "10_RULE_REGISTRY.csv": settled_daily.registry,
                }
                (_out_dir / f"{_base_name}_FULL_AUDIT.zip").write_bytes(_build_outputs_zip(_daily_zip_payload))
                st.caption(f"Self-stored in: {_out_dir}")
            except Exception as _save_exc:
                st.warning(f"Automatic OUTPUTS-folder saving failed: {_save_exc}")

            _dl1, _dl2 = st.columns(2)
            with _dl1:
                st.download_button(
                    "Download FINAL PLAYLIST CSV",
                    data=_playlist_export.to_csv(index=False).encode("utf-8"),
                    file_name=f"FINAL_PLAYLIST_{st.session_state.get('playlist_date', 'UNKNOWN')}_{APP_VERSION.split()[0]}.csv",
                    mime="text/csv",
                    key="nl_dated_playlist_csv",
                    use_container_width=True,
                )
            with _dl2:
                st.download_button(
                    "Download FINAL PRINTABLE PLAYLIST TXT",
                    data=_printable_txt.encode("utf-8"),
                    file_name=f"FINAL_PLAYLIST_PRINTABLE_{st.session_state.get('playlist_date', 'UNKNOWN')}_{APP_VERSION.split()[0]}.txt",
                    mime="text/plain",
                    key="nl_printable_playlist_txt",
                    use_container_width=True,
                )

            _nl_zip_files = {
                "00_FINAL_PLAYLIST.csv": _playlist_export,
                "01_FINAL_PLAYLIST_PRINTABLE.txt": _printable_txt,
                "02_FULL_SELECTED_CORE_RANKING_AUDIT.csv": nl_full_audit,
                "03_MEMBER_RULE_FIRE_AUDIT.csv": nl_member_fire_audit,
                "04_MEMBER_RULE_FIRE_SUMMARY.csv": nl_member_fire_summary,
                "05_ACTIVE_MEMBER_RULES.csv": _active_member_rules(),
                "06_STREAM_CORE_RULE_TRACE.csv": stream_core_rule_trace_df,
                "07_TRAIT_MAPPING_AUDIT.csv": trait_mapping_audit_df,
                "08_TRAIT_DICTIONARY_COLLISION_AUDIT.csv": trait_dictionary_audit_df,
                "09_FINAL_BOXED_PLAYS.csv": nl_boxed_plays,
                "BUILD_INFO.txt": (
                    f"BUILD: {APP_VERSION}\n"
                    f"PLAYLIST DATE: {st.session_state.get('playlist_date','')}\n"
                    f"SEEDLIST DATE: {st.session_state.get('seedlist_date','')}\n"
                    f"SELECTED CORES: {', '.join(cores_for_cache)}\n"
                    "RELIABLE PLAY FILE: 00_FINAL_PLAYLIST.csv\n"
                    "GLOBAL BOXED PLAY CAP: NONE\n"
                    "SELECTION POLICY: BASE/DUE OPEN FEATURE-ONLY; SETTLED CORE RULES THEN MEMBER35 DETERMINE SURVIVORS\n"
                    "ELIMINATED STREAMS: ONLY ROWS REMOVED BY SETTLED CORE OR MEMBER RULES ARE EXCLUDED\n"
                    "AUDIT-ONLY FILE: 02_FULL_SELECTED_CORE_RANKING_AUDIT.csv\n"
                ),
            }
            _nl_zip_files.update(build_audit_zip_payload(settled_daily))
            if st.session_state.get("do_straights", False):
                _straight_final, _straight_audit = render_straights_shortlist(nl_boxed_plays, df_all, per_box=2)
                _nl_zip_files["10_FINAL_STRAIGHT_SHORTLIST.csv"] = _straight_final
                _nl_zip_files["11_STRAIGHT_SHORTLIST_TRACE.csv"] = _straight_audit
                if not _straight_final.empty:
                    _straight_txt = "STRAIGHT PLAY # | STREAM | CORE | SEED | BOX | STRAIGHT\n" + "\n".join(f"{int(r.StraightPlayNumber):03d} | {r.Stream} | {str(r.Core).zfill(3)} | {str(r.Seed).zfill(4)} | {r.BoxedMember} | {r.Straight}" for r in _straight_final.itertuples()) + "\n"
                    _nl_zip_files["12_FINAL_STRAIGHT_SHORTLIST_PRINTABLE.txt"] = _straight_txt

            # Export every displayed per-core bucket table and diagnostic, regardless of expander state.
            for _core in cores_for_cache:
                try:
                    _stats = compute_stream_stats(df_all, _core, window_days, exclude_md)
                    _b = bucket_recommendations(_stats, cfg)
                    _stats_for_member = _stats.copy()
                    _stats_for_member["Core"] = str(_core).zfill(3)
                    _stats_for_member["Seed"] = _stats_for_member["Stream"].astype(str).map(_prev_seed_by_stream)
                    _stats_for_member["PlaylistDate"] = str(st.session_state.get("playlist_date", ""))
                    _stats_for_member["SeedlistDate"] = str(st.session_state.get("seedlist_date", ""))
                    _stats_member, _fa, _fs, _settled_preview = _apply_settled_member35_view(
                        _stats_for_member, mode=f"EXPORT_CORE_{str(_core).zfill(3)}_MEMBER35"
                    )
                    _nl_zip_files[f"core_{_core}/stream_stats_with_member35.csv"] = _stats_member
                    _nl_zip_files[f"core_{_core}/member35_fire_audit.csv"] = _fa
                    _nl_zip_files[f"core_{_core}/member35_stage_summary.csv"] = _fs
                    _nl_zip_files[f"core_{_core}/percentile_map.csv"] = get_position_percentiles_cached(_core, window_days, _stats)
                    _nl_zip_files[f"core_{_core}/buckets_top12_base.csv"] = _b.get("Top12", pd.DataFrame())
                    _nl_zip_files[f"core_{_core}/buckets_due.csv"] = _b.get("Due8", pd.DataFrame())
                    _nl_zip_files[f"core_{_core}/buckets_combined.csv"] = _b.get("Combined", pd.DataFrame())
                    _png = _rank_chart_png(_stats, _core)
                    if _png:
                        _nl_zip_files[f"core_{_core}/rank_chart.png"] = _png
                except Exception as _e:
                    _nl_zip_files[f"core_{_core}/ERROR.txt"] = str(_e)
            # Download-all is intentionally after the complete on-screen diagnostics section in v51.19.

            # Northern Star percentile map (playlist)
            # This summarizes how much "hit weight" concentrates by rank position in the *final* per-stream playlist.
            with st.expander("Northern Star percentile map (playlist positions)", expanded=False):
                try:
                    # Ensure DuePressure exists (legacy compatibility)
                    if "DuePressure" not in nl_df.columns:
                        if "DueBucketPressure" in nl_df.columns:
                            nl_df["DuePressure"] = nl_df["DueBucketPressure"].astype(float)
                        else:
                            nl_df["DuePressure"] = 0.0
                    # Keep only the best row per Stream (highest UniversalScore) -> one entry per stream
                    _best = nl_df.sort_values(
                        ["UniversalScore", "HitsPerWeek", "DuePressure"],
                        ascending=[False, False, False]
                    ).groupby("Stream", as_index=False).head(1).reset_index(drop=True)

                    # Assign playlist rank positions 1..N (typically 78 streams)
                    _best.insert(0, "RankPos", _best.index + 1)
                    _best["HitsWindow"] = _best.get("Hits", 0).astype(int)

                    _pos, _ = position_percentile_map(_best[["RankPos", "HitsWindow"]].copy())
                    st.caption("RankPos = position in the final per-stream playlist. HitCount = historical hits (in the selected window) of the #1 pick for that stream.")
                    _safe_st_dataframe(_pos, width="stretch", height=320)
                except Exception as _e:
                    st.warning(f"Could not build the playlist percentile map: {_e}")

            # Northern Star buckets (per core)
            cfg = st.session_state.get("_cfg", RankConfig())
            with st.expander("Northern Star buckets (per core)", expanded=True):
                if cores_for_cache:
                    _b_tabs = st.tabs([f"Core {c}" for c in cores_for_cache]) if len(cores_for_cache) > 1 else [st.container()]
                    for _tab, _c in zip(_b_tabs, cores_for_cache):
                        with _tab:
                            _core_str = str(_c).zfill(3)
                            _stats_df = compute_stream_stats(df_all, _core_str, window_days, exclude_md)
                            if _stats_df is None or _stats_df.empty:
                                st.info(f"No AABC stream stats for core {_core_str}.")
                                continue
                            _b = bucket_recommendations(_stats_df, cfg)
                            c1, c2, c3 = st.columns(3)
                            with c1:
                                st.caption("Top 12 (BaseScore)")
                                st.write(_b.get("Top12", []))
                            with c2:
                                st.caption(f"Due {getattr(cfg, 'top_due', 8)} (DueIndex)")
                                st.write(_b.get("Due8", []))
                            with c3:
                                st.caption("Combined (Top+Due)")
                                st.write(_b.get("Combined", []))
                else:
                    st.info("Select one or more cores above to view buckets.")

            
            # Percentile map(s) for selected core(s) (tie-breaker visibility in Northern Lights view)
            with st.expander("Core ranking percentile map (tie-breaker)"):
                if cores_for_cache:
                    _tabs = st.tabs([f"Core {c}" for c in cores_for_cache]) if len(cores_for_cache) > 1 else [st.container()]
                    for _tab, _c in zip(_tabs, cores_for_cache):
                        with _tab:
                            _core_str = str(_c).zfill(3)
                            # get_position_percentiles_cached() expects the active window + per-core stream stats.
                            # In this view we build / reuse the same stream-stats used by the core ranking.
                            _stream_stats = compute_stream_stats(df_all, _core_str, window_days, exclude_md)
                            _pm = get_position_percentiles_cached(_core_str, window_days, _stream_stats)
                            _safe_st_dataframe(_pm, width="stretch", height=240)
                else:
                    st.info("Select one or more cores above to view percentile maps.")

            st.download_button(
                "Download ALL Northern Lights outputs",
                data=_build_outputs_zip(_nl_zip_files),
                file_name=f"NORTHERN_LIGHTS_ALL_{st.session_state.get('playlist_date','UNKNOWN')}_{APP_VERSION.split()[0]}.zip",
                mime="application/zip",
                key="nl_download_all_outputs",
                use_container_width=True,
            )

            # Optional: straights shortlist (keep existing feature)
            if st.session_state.get("do_straights", False):
                st.divider()
                st.subheader("Generate straights shortlist (optional last)")
                st.caption("This feature is unchanged; it runs only after the master playlist is built.")
                try:
                    _sf, _sa = render_straights_shortlist(nl_boxed_plays, df_all, per_box=2)
                    _safe_st_dataframe(_sf, use_container_width=True, height=420)
                    st.caption(f"Straight shortlist plays: {len(_sf)}; full candidate audit rows: {len(_sa)}")
                except Exception as e:
                    st.error(f"Straights shortlist failed: {e}")


# --- Core view (single core or tabbed multi-core) ---

if _t_ns is None:
    _t_ns = st.container()
with _t_ns:
    st.header("Northern Star (v51)")
    st.caption("This tab restores the Northern Star scoring view and engines (Rare / Ultra-Rare) while keeping Core View unchanged. Percentile maps by position are shown here as GLOBAL (all selected cores) and PER-CORE maps.")

    # Stream-level global ranking across selected cores. The percentile-only summary remains below as an audit.
    st.subheader("Global stream/core ranking (selected cores only, cache-only)")
    ns_cores = [str(c).zfill(3) for c in (st.session_state.get("cores_for_cache_ms", []) or [])]
    st.caption("Every row identifies its Stream and Core. Drag the horizontal scrollbar at the bottom of the table to reveal all audit columns.")
    if not ns_cores:
        st.info("Select one or more cores in the multi-core section above.")
    else:
        expected_last = last_all if isinstance(last_all, str) else None
        _global_parts = []
        _global_missing = []
        for _gc in ns_cores:
            _gstats, _gpos, _gmeta = _load_baseline_from_disk(_gc, cfg.window_days, expected_last_date=expected_last)
            if _gstats is None or _gstats.empty:
                _global_missing.append(_gc)
                continue
            _g = _gstats.copy()
            _g["Core"] = str(_gc).zfill(3)
            if "Stream" not in _g.columns:
                _global_missing.append(_gc)
                continue
            if _gpos is None or _gpos.empty:
                _gpos, _ = position_percentile_map(_g)
            _pct_col = "PctStrength" if "PctStrength" in _gpos.columns else ("HitCountPctile" if "HitCountPctile" in _gpos.columns else None)
            _pct_map = dict(zip(pd.to_numeric(_gpos.get("RankPos", pd.Series(dtype=float)), errors="coerce"), pd.to_numeric(_gpos.get(_pct_col, 0.0), errors="coerce"))) if _pct_col else {}
            _g["PosPctStrength"] = pd.to_numeric(_g.get("RankPos", 9999), errors="coerce").map(_pct_map).fillna(0.0)
            _g["HitsPerWeek"] = pd.to_numeric(_g.get("HitsPerWeek", 0.0), errors="coerce").fillna(0.0)
            _g["DaysSinceLastHit"] = pd.to_numeric(_g.get("DaysSinceLastHit", 0.0), errors="coerce").fillna(0.0)
            _g["GlobalNSScore"] = (
                _g["HitsPerWeek"]
                + _g["DaysSinceLastHit"].clip(upper=50.0) * 0.01 * float(st.session_state.get("due_weight", 0.20))
                + _g["PosPctStrength"] * 0.01 * float(st.session_state.get("pos_weight", 0.25))
            )
            _g["Seed"] = _g["Stream"].astype(str).map(_prev_seed_by_stream).fillna("")
            _global_parts.append(_g)

        if _global_missing:
            st.error("Missing or invalid baseline caches for: " + ", ".join(sorted(set(_global_missing))))
            st.caption("Build caches in the Cache Builder section, then rerun.")
        if _global_parts:
            _global_stream_rank = _safe_pd_concat(_global_parts, ignore_index=True, sort=False)
            _global_stream_rank = _global_stream_rank.sort_values(
                ["GlobalNSScore", "HitsPerWeek", "DaysSinceLastHit", "Core", "Stream"],
                ascending=[False, False, False, True, True],
                kind="mergesort",
            ).reset_index(drop=True)
            _global_stream_rank.insert(0, "GlobalRank", np.arange(1, len(_global_stream_rank) + 1))
            _global_cols = [c for c in [
                "GlobalRank", "Stream", "Core", "Seed", "GlobalNSScore", "RankPos", "BaseScoreRank",
                "HitsWindow", "DrawsWindow", "HitsPerWeek", "DaysSinceLastHit", "PosPctStrength",
                "LastHitDate", "WindowStart", "WindowEnd"
            ] if c in _global_stream_rank.columns]
            _global_view = _global_stream_rank[_global_cols].copy()
            _global_cfg = {
                "GlobalRank": st.column_config.NumberColumn("Global Rank", width="small"),
                "Stream": st.column_config.TextColumn("Stream", width="large"),
                "Core": st.column_config.TextColumn("Core", width="small"),
                "Seed": st.column_config.TextColumn("Seed", width="small"),
                "GlobalNSScore": st.column_config.NumberColumn("Global Score", format="%.6f", width="medium"),
                "RankPos": st.column_config.NumberColumn("Core RankPos", width="small"),
                "BaseScoreRank": st.column_config.NumberColumn("Base Rank", width="small"),
                "HitsWindow": st.column_config.NumberColumn("Hits Window", width="small"),
                "DrawsWindow": st.column_config.NumberColumn("Draws Window", width="small"),
                "HitsPerWeek": st.column_config.NumberColumn("Hits/Week", format="%.6f", width="medium"),
                "DaysSinceLastHit": st.column_config.NumberColumn("Days Since Hit", width="small"),
                "PosPctStrength": st.column_config.NumberColumn("Position Strength", format="%.4f", width="medium"),
            }
            _safe_st_dataframe(
                _global_view,
                width="stretch",
                height=650,
                hide_index=True,
                column_config={k: v for k, v in _global_cfg.items() if k in _global_view.columns},
            )
            st.download_button(
                "Download global stream/core ranking CSV",
                data=_global_stream_rank.to_csv(index=False).encode("utf-8"),
                file_name=f"GLOBAL_STREAM_CORE_RANK_{st.session_state.get('playlist_date','UNKNOWN')}_{APP_VERSION.split()[0]}.csv",
                mime="text/csv",
                key="ns_global_stream_rank_download",
            )

        with st.expander("Global RankPos percentile summary (position-only audit)", expanded=False):
            global_map, missing = build_allcores_rankpos_pctmap(ns_cores, window_days=cfg.window_days, expected_last_date=expected_last, cache_only=True)
            if missing:
                st.warning("Percentile summary unavailable; missing caches: " + ", ".join(missing))
            else:
                st.caption("This secondary audit summarizes rank positions only; it is not the stream-level global ranking above.")
                _safe_st_dataframe(global_map, width="stretch", hide_index=True)

    st.divider()
    st.subheader("Per-core Northern Star scoring (SeedTraits + Cadence, soft)")
    _ns_current = str(st.session_state.get("view_core_ns", ns_cores[0])).zfill(3)
    if _ns_current not in ns_cores:
        _ns_current = ns_cores[0]
    view_core_ns = st.selectbox("Core (Northern Star view)", options=ns_cores, index=ns_cores.index(_ns_current), key="view_core_ns")
    core_key_ns = canonical_core_key(view_core_ns)

    try:
        stats_ns = compute_stream_stats(df_all, core_key_ns, window_days=cfg.window_days, exclude_md=False)
    except Exception as e:
        st.error(f"Could not compute stats for core {core_key_ns}: {e}")
        stats_ns = pd.DataFrame()

    if stats_ns is not None and not stats_ns.empty:
        # Position map per-core (distinct from global map)
        pos_map_ns = get_position_percentiles_cached(core_key_ns, cfg.window_days, stats_ns)
        pos_strength_by_rank = dict(zip(pos_map_ns["RankPos"], pos_map_ns["PctStrength"]))

        # Cadence base: average gap for this core across streams (in days)
        total_hits = float(stats_ns["HitsWindow"].sum()) if "HitsWindow" in stats_ns.columns else 0.0
        mean_gap_days = (cfg.window_days / total_hits) if total_hits > 0 else 0.0

        # Seed Traits + Cadence per stream
        ns_rows = []
        for _, r in stats_ns.iterrows():
            stream = str(r.get("Stream", ""))
            rankpos = int(r.get("RankPos", 9999))
            pos_strength = float(pos_strength_by_rank.get(rankpos, 0.0))
            seed = _prev_seed_by_stream.get(stream)
            seed_score, seed_matches = compute_seed_traits_score(
                core_key_ns, seed, stream,
                pos_lookup=seed_traits_pos_lookup,
                neg_lookup=seed_traits_neg_lookup,
                last5_union_digits_by_stream=_last5_union_by_stream,
            )
            cadence = compute_cadence_score(float(r.get("DaysSinceLastHit", 0.0)), mean_gap_days) if mean_gap_days > 0 else 0.0

            # Soft combined score
            hits_pw = float(r.get("HitsPerWeek", 0.0))
            due_pressure = float(r.get("DaysSinceLastHit", 0.0))
            ns_score = (
                hits_pw
                + (min(due_pressure, 50.0) * 0.01 * float(st.session_state.get("due_weight", 0.20)))
                + (pos_strength * 0.01 * float(st.session_state.get("pos_weight", 0.25)))
                + (seed_score * float(st.session_state.get("seed_traits_weight", 0.35)) if st.session_state.get("enable_seed_traits", True) else 0.0)
                + (cadence * float(st.session_state.get("cadence_weight", 0.25)) if st.session_state.get("enable_cadence", True) else 0.0)
            )
            ns_rows.append({
                "Stream": stream,
                "RankPos": rankpos,
                "HitsPerWeek": hits_pw,
                "DaysSinceLastHit": due_pressure,
                "PosPctStrength": pos_strength,
                "SeedTraitsScore": seed_score,
                "CadenceScore": cadence,
                "NSScore": ns_score,
                "Seed": seed,
            })
        ns_df = pd.DataFrame(ns_rows).sort_values(["NSScore","HitsPerWeek"], ascending=False)
        ns_df["Core"] = core_key_ns
        ns_df["PlaylistDate"] = str(st.session_state.get("playlist_date", ""))
        ns_df["SeedlistDate"] = str(st.session_state.get("seedlist_date", ""))
        # IMPORTANT: never rerun settled core/load rules on a single core. Core45 and
        # related load/percentile traits require the complete selected-core universe.
        # Reuse the full Daily settled result produced from the unchanged legacy
        # Northern Lights matrix, then filter only for this per-core display.
        ns_settled_result = st.session_state.get("settled_daily_result")
        if ns_settled_result is None:
            ns_member_all = pd.DataFrame()
            ns_member_fire_audit = pd.DataFrame()
            ns_member_fire_summary = pd.DataFrame()
            st.warning(
                "Full-universe Member35 result is not available yet. Open/run Northern Lights "
                "once so the unchanged legacy selected-core matrix is built, then this view will "
                "filter that completed result. The app will not rerun settled rules on one core."
            )
        else:
            ns_member_all = ns_settled_result.member_all.copy()
            ns_member_all = ns_member_all[
                ns_member_all.get("Core", pd.Series("", index=ns_member_all.index)).astype(str).str.zfill(3).eq(str(core_key_ns).zfill(3))
            ].copy()
            ns_member_fire_audit = ns_settled_result.member_fire_audit.copy()
            if not ns_member_fire_audit.empty and "PositionKey" in ns_member_fire_audit.columns:
                _core_token = "|" + str(core_key_ns).zfill(3)
                ns_member_fire_audit = ns_member_fire_audit[
                    ns_member_fire_audit["PositionKey"].astype(str).str.endswith(_core_token)
                ].copy()
            ns_member_fire_summary = ns_settled_result.stage_summary.copy()
            if not ns_member_fire_summary.empty and "Stage" in ns_member_fire_summary.columns:
                ns_member_fire_summary = ns_member_fire_summary[
                    ns_member_fire_summary["Stage"].astype(str).eq("MEMBER35")
                ].copy()
        st.caption(
            "Member35 source: full selected-core legacy matrix → appended settled working copy → "
            f"filtered to core {str(core_key_ns).zfill(3)}. Legacy matrix formulas and outputs remain unchanged."
        )
        _ns_visible = [c for c in [
            "Stream", "Seed", "Core", "BoxedMember", "DoubleRole", "MemberSettledSurvivor",
            "MemberFirstDeletingRule", "MemberAllMatchingRules", "MembersBefore", "MembersAfter",
            "AllMembersDeleted", "RankPos", "NSScore", "HitsPerWeek", "DaysSinceLastHit",
            "SeedTraitsScore", "CadenceScore"
        ] if c in ns_member_all.columns]
        if ns_member_all.empty and ns_settled_result is not None:
            _core_diag = ns_settled_result.core_all.copy()
            if not _core_diag.empty and "Core" in _core_diag.columns:
                _core_diag = _core_diag[_core_diag["Core"].astype(str).str.zfill(3).eq(str(core_key_ns).zfill(3))].copy()
            st.warning(
                f"Core {str(core_key_ns).zfill(3)} produced no member rows because no positions survived the settled core stages. "
                "The core-stage audit is shown below; Member35 was not skipped."
            )
            _diag_cols = [c for c in ["Date","Stream","Core","Seed","OriginalBaseDueEligible","ProductionEligibleBeforeSettled","DeletedByEarlyCore","DeletedByPick3_95","DeletedByCore45","CoreSettledSurvivor","CoreFirstDeletingRule","CoreAllMatchingRules"] if c in _core_diag.columns]
            _safe_st_dataframe(_core_diag[_diag_cols], width="stretch", height=420)
        else:
            _safe_st_dataframe(ns_member_all[_ns_visible].head(150), width="stretch", height=520)
        _member_deleted = int((~ns_member_all.get("MemberSettledSurvivor", pd.Series(dtype=bool)).fillna(False).astype(bool)).sum()) if not ns_member_all.empty else 0
        _member_kept = int(ns_member_all.get("MemberSettledSurvivor", pd.Series(dtype=bool)).fillna(False).astype(bool).sum()) if not ns_member_all.empty else 0
        st.caption(f"Full-universe settled Member35 view — kept {_member_kept:,}, deleted {_member_deleted:,} for this core. NSScore remains a soft ranking trait; Member35 performs the final boxed-member deletion.")
        with st.expander("Member35 firing audit for this core", expanded=False):
            _safe_st_dataframe(ns_member_fire_audit, width="stretch", height=420)
            _safe_st_dataframe(ns_member_fire_summary, width="stretch", height=260)

        with st.expander("Per-core RankPos percentile map (position-based)"):
            _safe_st_dataframe(pos_map_ns, width="stretch")

        # Engines (restored UI)
        st.divider()
        st.subheader("Rare Engine (AABC-family; historical lift)")
        if st.session_state.get("r1", True) or st.session_state.get("r2", True) or st.session_state.get("r3", True) or st.session_state.get("r4", True):
            # evaluate_rare_engine signature expects:
            #   (df_all, core, df_24h, enable_r1, enable_r2, enable_r3, enable_r4, window_days_recent)
            # Keep the UI-driven switches and pass the optional 24h map (may be empty).
            try:
                try:
                    rare_df, _rare_summary = evaluate_rare_engine(
                    df_all,
                    core_key_ns,
                    df_24h,
                    enable_r1=r1,
                    enable_r2=r2,
                    enable_r3=r3,
                    enable_r4=r4,
                    window_days_recent=cfg.window_days,
                    )
                except TypeError as _te:
                    if 'window_days_recent' in str(_te) and 'unexpected keyword argument' in str(_te):
                        rare_df, _rare_summary = evaluate_rare_engine(
                        df_all,
                        core_key_ns,
                        df_24h,
                        enable_r1=r1,
                        enable_r2=r2,
                        enable_r3=r3,
                        enable_r4=r4,
                        )
                    else:
                        raise
                _safe_st_dataframe(_to_dataframe(rare_df), width="stretch")
            except Exception as e:
                st.error(f"Rare Engine error: {e}")
                _safe_st_dataframe(pd.DataFrame(), width="stretch")
        else:
            st.info("Enable at least one Rare Engine checkbox above to view results.")

        st.subheader("Ultra-Rare Engine (AABB/AAAB/etc; historical lift)")
        if st.session_state.get("q1", True) or st.session_state.get("q2", True) or st.session_state.get("q3", True) or st.session_state.get("q4", True):
            try:
                try:
                    ultra_df, _ultra_summary = evaluate_ultra_rare_engine(
                    df_all,
                    core_key_ns,
                    df_24h,
                    enable_q1=q1,
                    enable_q2=q2,
                    enable_q3=q3,
                    enable_q4=q4,
                    window_days_recent=cfg.window_days,
                    )
                except TypeError as _te:
                    if 'window_days_recent' in str(_te) and 'unexpected keyword argument' in str(_te):
                        ultra_df, _ultra_summary = evaluate_ultra_rare_engine(
                        df_all,
                        core_key_ns,
                        df_24h,
                        enable_q1=q1,
                        enable_q2=q2,
                        enable_q3=q3,
                        enable_q4=q4,
                        )
                    else:
                        raise
                _safe_st_dataframe(_to_dataframe(ultra_df), width="stretch")
            except Exception as e:
                st.error(f"Ultra-Rare Engine error: {e}")
                _safe_st_dataframe(pd.DataFrame(), width="stretch")
        else:
            st.info("Enable at least one Ultra-Rare checkbox above to view results.")

        # Download all Page 1 outputs for every selected core, not just the displayed core.
        _ns_all_files = {
            "BUILD_INFO.txt": f"BUILD: {APP_VERSION}\nPLAYLIST DATE: {st.session_state.get('playlist_date','')}\nSEEDLIST DATE: {st.session_state.get('seedlist_date','')}\nSELECTED CORES: {', '.join(ns_cores)}\n"
        }
        for _core in ns_cores:
            try:
                _sdf = compute_stream_stats(df_all, _core, window_days=cfg.window_days, exclude_md=False)
                if _sdf is None or _sdf.empty:
                    continue
                _pdf = get_position_percentiles_cached(_core, cfg.window_days, _sdf)
                _sdf_member = _sdf.copy()
                _sdf_member["Core"] = str(_core).zfill(3)
                _sdf_member["Seed"] = _sdf_member["Stream"].astype(str).map(_prev_seed_by_stream)
                _sdf_member["PlaylistDate"] = str(st.session_state.get("playlist_date", ""))
                _sdf_member["SeedlistDate"] = str(st.session_state.get("seedlist_date", ""))
                _sdf_member, _mfa, _mfs, _settled_core_view = _apply_settled_member35_view(
                    _sdf_member, mode="CORE_VIEW_MEMBER35_PREVIEW"
                )
                _b = bucket_recommendations(_sdf, cfg)
                _ns_all_files[f"core_{_core}/stream_scores_with_members.csv"] = _sdf_member
                _ns_all_files[f"core_{_core}/rank_percentiles.csv"] = _pdf
                _ns_all_files[f"core_{_core}/buckets_top12_base.csv"] = _b.get("Top12", pd.DataFrame())
                _ns_all_files[f"core_{_core}/buckets_due.csv"] = _b.get("Due8", pd.DataFrame())
                _ns_all_files[f"core_{_core}/buckets_combined.csv"] = _b.get("Combined", pd.DataFrame())
                _ns_all_files[f"core_{_core}/member_rule_fire_audit.csv"] = _mfa
                _ns_all_files[f"core_{_core}/member_rule_fire_summary.csv"] = _mfs
                _png = _rank_chart_png(_sdf, _core)
                if _png:
                    _ns_all_files[f"core_{_core}/rank_chart.png"] = _png
            except Exception as _e:
                _ns_all_files[f"core_{_core}/ERROR.txt"] = str(_e)
        st.download_button(
            "Download all Northern Star outputs (selected cores)",
            data=_build_outputs_zip(_ns_all_files),
            file_name=f"NORTHERN_STAR_ALL_SELECTED_{st.session_state.get('playlist_date','UNKNOWN')}_{APP_VERSION.split()[0]}.zip",
            mime="application/zip",
            key="ns_download_all_outputs",
            use_container_width=True,
        )

        # Seed Traits match details (debug / transparency)
        with st.expander("Seed Traits matches (debug / transparency)"):
            pick_stream = st.selectbox("Stream to inspect", options=list(ns_df["Stream"].head(25)), key="ns_inspect_stream")
            seed = _prev_seed_by_stream.get(str(pick_stream))
            score, matches = compute_seed_traits_score(
                core_key_ns, seed, str(pick_stream),
                pos_lookup=seed_traits_pos_lookup,
                neg_lookup=seed_traits_neg_lookup,
                last5_union_digits_by_stream=_last5_union_by_stream,
            )
            st.write({"core": core_key_ns, "stream": str(pick_stream), "seed": seed, "score": score})
            if matches:
                _safe_st_dataframe(pd.DataFrame(matches, columns=["trait","value","lift","sign"]), width="stretch")
            else:
                st.caption("No matching traits found (or trait files not loaded).")
    else:
        st.info("No stats available for this core. Build or load data/caches and rerun.")


if _t_core is None:
    _t_core = st.container()
with _t_core:
    st.subheader("Core view")

    if df_all is None or df_all.empty:
        st.info("Upload your history file first.")
        st.stop()

    if not cores_for_cache:
        st.info("Select one or more cores above to view core stats.")
        st.stop()

    show_tabs = st.checkbox(
        "Show tabs for all selected cores (optional)",
        value=False,
        key="show_tabs_for_all_selected_cores",
        help="If ON, you'll get a separate Core tab for each selected core. If OFF, you only see the core chosen in 'View core'.",
    )

    cfg = st.session_state.get("_cfg", RankConfig())

    def _render_one_core(core_id: str):
        core_id = str(core_id).zfill(3)
        st.markdown(f"### Core {core_id}")

        # Compute the AABC stream stats
        stats_df = compute_stream_stats(df_all, core_id, window_days=window_days, exclude_md=False)
        if stats_df is None or stats_df.empty:
            st.warning(f"No AABC stream stats found for core {core_id}.")
            return

        stats_df = stats_df.copy()
        st.subheader("Stream ranking (AABC doubles)")
        _safe_st_dataframe(stats_df, width="stretch", height=420)

        # Buckets (Top 12 BaseScore + Due 8 from ranks 13–60)
        # NOTE: build_northern_star_buckets() is a *per-stream* helper used by the master playlist.
        # For the per-core view we want the actual bucket lists, which are produced by bucket_recommendations().
        buckets = bucket_recommendations(stats_df, cfg)
        top_bucket = buckets.get("Top12", [])
        due_bucket = buckets.get("Due8", [])
        combined_bucket = buckets.get("Combined", [])

        st.subheader("Northern Star buckets")
        c1, c2, c3 = st.columns(3)
        with c1:
            st.caption("Top 12 (BaseScore)")
            st.write(top_bucket)
        with c2:
            st.caption(f"Due {getattr(cfg, 'top_due', 8)} (DueIndex)")
            st.write(due_bucket)
        with c3:
            st.caption("Combined (Top+Due)")
            st.write(combined_bucket)

        # Core percentile map expander (tie-breaker)
        with st.expander("Core ranking percentile map (tie-breaker)", expanded=False):
            try:
                pos_map = get_position_percentiles_cached(core_id, window_days, stats_df)
                if pos_map is None or pos_map.empty:
                    st.info("No percentile map available for this core.")
                else:
                    _safe_st_dataframe(pos_map, width="stretch", height=420)
                    st.caption("Tip: use PctStrength as a soft tie-breaker when streams are close.")
            except Exception as e:
                st.error(f"Could not compute percentile map: {e}")

    if show_tabs:
        # Always render *all* selected cores in their own tabs
        core_tabs = st.tabs([str(c).zfill(3) for c in cores_for_cache])
        for c, t in zip(cores_for_cache, core_tabs):
            with t:
                _render_one_core(str(c))
    else:
        # Render only the currently selected view core
        _render_one_core(str(core_for_view))


# --- Backtest (optional) ---
    _core_view_files = {
        "BUILD_INFO.txt": f"BUILD: {APP_VERSION}\nPLAYLIST DATE: {st.session_state.get('playlist_date','')}\nSEEDLIST DATE: {st.session_state.get('seedlist_date','')}\nSELECTED CORES: {', '.join(cores_for_cache)}\n"
    }
    for _core in cores_for_cache:
        try:
            _stats = compute_stream_stats(df_all, _core, window_days=window_days, exclude_md=False)
            _core_view_files[f"core_{_core}/stream_ranking.csv"] = _stats
            _core_view_files[f"core_{_core}/percentile_map.csv"] = get_position_percentiles_cached(_core, window_days, _stats)
            _png = _rank_chart_png(_stats, _core)
            if _png:
                _core_view_files[f"core_{_core}/rank_chart.png"] = _png
        except Exception as _e:
            _core_view_files[f"core_{_core}/ERROR.txt"] = str(_e)
    st.download_button(
        "Download all Core View outputs",
        data=_build_outputs_zip(_core_view_files),
        file_name=f"CORE_VIEW_ALL_SELECTED_{st.session_state.get('playlist_date','UNKNOWN')}_{APP_VERSION.split()[0]}.zip",
        mime="application/zip",
        key="core_view_download_all_outputs",
        use_container_width=True,
    )

if _t_bt is None:
    _t_bt = st.container()
with _t_bt:
    st.subheader("Backtest (optional)")
    st.caption("Optional diagnostics. This does not change your core ranking output.")

    if df_all is None or df_all.empty:
        st.info("Upload your history file first.")
        st.stop()

    if not cores_for_cache:
        st.info("Select one or more cores above to backtest.")
        st.stop()

    try:
        render_backtest(df_all=df_all, cfg=cfg, cores_for_cache=cores_for_cache, df_24h=df_24h)
    except NameError:
        st.warning("Backtest utility is not available in this build.")
    except Exception as e:
        import traceback as _traceback
        _wf_trace = _traceback.format_exc()
        _wf_err_dir = Path.cwd() / "OUTPUTS"
        _wf_err_dir.mkdir(parents=True, exist_ok=True)
        _wf_err_path = _wf_err_dir / "WF_LAST_ERROR_TRACEBACK.txt"
        try:
            _wf_err_path.write_text(_wf_trace, encoding="utf-8")
        except Exception:
            pass
        st.error(f"Backtest failed: {e}")
        with st.expander("Full WF error details", expanded=True):
            st.code(_wf_trace)
        st.caption(f"Error traceback saved to: {_wf_err_path}")

# --- Core Set Separation Lab ---
if _t_lab is None:
    _t_lab = st.container()
with _t_lab:
    if df_all is None or df_all.empty:
        st.info("Upload your history file first.")
    else:
        try:
            render_core_set_lab(
                df_all,
                cfg,
                callbacks={
                    "compute_stream_stats": compute_stream_stats,
                    "position_percentile_map": position_percentile_map,
                    "compute_seed_traits_score": compute_seed_traits_score,
                    "compute_cadence_score": compute_cadence_score,
                    "feature_values_for_seed": _feature_values_for_seed,
                    "members_from_core": members_from_core,
                    "box_key": box_key,
                    "core_presets": CORE_PRESETS,
                    "working8": WORKING8_CORE_SET,
                },
                lookups={
                    "pos": seed_traits_pos_lookup,
                    "neg": seed_traits_neg_lookup,
                },
                weights={
                    "due_weight": float(st.session_state.get("due_weight",0.20)),
                    "pos_weight": float(st.session_state.get("pos_weight",0.25)),
                    "seed_traits_weight": float(st.session_state.get("seed_traits_weight",0.35)),
                    "cadence_weight": float(st.session_state.get("cadence_weight",0.25)),
                    "enable_seed_traits": bool(st.session_state.get("enable_seed_traits",True)),
                    "enable_cadence": bool(st.session_state.get("enable_cadence",True)),
                },
                exclude_md=bool(exclude_md),
            )
        except Exception as e:
            st.exception(e)

# Northern Star (core) RankPos map is the same distribution, but we cache it separately for clarity
_core_pct_cached = _load_pctmap_from_disk(f"CORE_{view_core}", cfg.window_days, expected_last_date=last_all)
if _core_pct_cached is None:
    try:
        _save_pctmap_to_disk(f"CORE_{view_core}", cfg.window_days, pos_pct, asof_last_date=last_all)
        _core_pct_cached = _load_pctmap_from_disk(f"CORE_{view_core}", cfg.window_days, expected_last_date=last_all)
    except Exception:
        _core_pct_cached = None

if _core_pct_cached is not None and not _core_pct_cached.empty:
    st.caption("Northern Star (this core) RankPos percentiles (cached for stability).")


def _rerun():
    """Compatibility rerun helper (Streamlit versions)."""
    try:
        st.rerun()
    except Exception:
        try:
            st.experimental_rerun()
        except Exception:
            pass
