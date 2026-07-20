
from __future__ import annotations

import io
import itertools
import math
import re
import zipfile
from dataclasses import dataclass
from typing import Any, Callable

import numpy as np
import pandas as pd
import streamlit as st

from member_rule_engine import (
    WORKING8,
    apply_member_rules,
    load_rules,
    member_results_zip,
)

BUILD = "NS_CORE_SET_LAB_V3_MEMBER_V1_SELECTED_CORE_LOCK"

CURRENT12 = ["016","027","028","067","138","145","256","389","457","458","567","679"]

def all_120_cores() -> list[str]:
    return ["".join(x) for x in itertools.combinations("0123456789", 3)]

def _norm_core(x: Any) -> str:
    s = re.sub(r"\D", "", str(x)).zfill(3)[-3:]
    return "".join(sorted(s))

def _core_members(core: str, members_from_core: Callable) -> list[str]:
    return [str(x).zfill(4) for x in members_from_core(_norm_core(core), "AABC")]

def _asof_seed_maps(train_df: pd.DataFrame) -> tuple[dict[str,str], dict[str,set[str]]]:
    if train_df is None or train_df.empty:
        return {}, {}
    d = train_df.copy()
    d["Date"] = pd.to_datetime(d["Date"], errors="coerce")
    d = d[d["Date"].notna()].sort_values(["Stream","Date"])
    seed_map = d.groupby("Stream")["Result"].last().astype(str).to_dict()
    last5 = {}
    for stream, g in d.groupby("Stream", sort=False):
        vals = g.tail(5)["Result"].astype(str).tolist()
        last5[str(stream)] = set("".join(vals))
    return seed_map, last5


SEED_TRAIT_COLUMNS = [
    "seed_structure",
    "seed_even_count",
    "seed_high_count",
    "seed_spread",
    "seed_sum_mod2",
    "seed_sum_mod3",
    "seed_sum_range4_best",
    "seed_sum_range4_worst",
    "overlap_unique",
    "seed_contains_core_pair",
    "seed_first_in_core",
    "seed_last_in_core",
    "grid_last5_core_digits",
]

def _flatten_feature_values(values: Any) -> str:
    if values is None:
        return ""
    if isinstance(values, (list, tuple, set)):
        return "|".join(str(x) for x in values)
    return str(values)

def _seed_match_audit(matches: list[tuple[str,str,float,str]]) -> dict[str,Any]:
    pos = [m for m in matches if len(m) >= 4 and str(m[3]) == "+"]
    neg = [m for m in matches if len(m) >= 4 and str(m[3]) == "-"]
    pos_delta = sum(float(m[2]) - 1.0 for m in pos)
    neg_delta = sum(float(m[2]) - 1.0 for m in neg)
    return {
        "SeedPositiveMatchCount": len(pos),
        "SeedNegativeMatchCount": len(neg),
        "SeedPositiveDelta": float(pos_delta),
        "SeedNegativeDelta": float(neg_delta),
        "SeedMatchedRules": ";".join(
            f"{m[3]}:{m[0]}={m[1]}@{float(m[2]):.6g}" for m in matches
        ),
    }

def _rank_desc(df: pd.DataFrame, score_col: str, rank_col: str) -> pd.DataFrame:
    out = df.sort_values([score_col, "HitsPerWeek", "Stream"],
                         ascending=[False, False, True]).reset_index(drop=True)
    out[rank_col] = np.arange(1, len(out) + 1)
    return out

def exact_ns_table(
    train_df: pd.DataFrame,
    core: str,
    cfg: Any,
    *,
    compute_stream_stats: Callable,
    position_percentile_map: Callable,
    compute_seed_traits_score: Callable,
    compute_cadence_score: Callable,
    feature_values_for_seed: Callable,
    pos_lookup: dict,
    neg_lookup: dict,
    due_weight: float,
    pos_weight: float,
    seed_traits_weight: float,
    cadence_weight: float,
    enable_seed_traits: bool,
    enable_cadence: bool,
    exclude_md: bool,
) -> pd.DataFrame:
    """Use the original app's functions and exact Northern Star formula."""
    core = _norm_core(core)
    stats = compute_stream_stats(
        train_df, core, window_days=int(cfg.window_days), exclude_md=bool(exclude_md)
    )
    if stats is None or stats.empty:
        return pd.DataFrame()

    pos_map, _ = position_percentile_map(stats)
    pos_strength_by_rank = dict(zip(pos_map["RankPos"], pos_map["PctStrength"]))

    total_hits = float(stats["HitsWindow"].sum()) if "HitsWindow" in stats.columns else 0.0
    mean_gap_days = (float(cfg.window_days) / total_hits) if total_hits > 0 else 0.0
    seed_map, last5_map = _asof_seed_maps(train_df)

    rows = []
    for _, r in stats.iterrows():
        stream = str(r.get("Stream", ""))
        rankpos = int(r.get("RankPos", 9999))
        pos_strength = float(pos_strength_by_rank.get(rankpos, 0.0))
        seed = seed_map.get(stream)
        seed_score, _matches = compute_seed_traits_score(
            core,
            seed,
            stream,
            pos_lookup=pos_lookup,
            neg_lookup=neg_lookup,
            last5_union_digits_by_stream=last5_map,
        )
        raw_features = (
            feature_values_for_seed(str(seed), core, last5_union_digits=last5_map.get(stream))
            if seed else {}
        )
        match_audit = _seed_match_audit(_matches)
        cadence = (
            compute_cadence_score(float(r.get("DaysSinceLastHit", 0.0)), mean_gap_days)
            if mean_gap_days > 0
            else 0.0
        )
        hits_pw = float(r.get("HitsPerWeek", 0.0))
        due_pressure = float(r.get("DaysSinceLastHit", 0.0))
        base_ns_score = (
            hits_pw
            + (min(due_pressure, 50.0) * 0.01 * float(due_weight))
            + (pos_strength * 0.01 * float(pos_weight))
            + (cadence * float(cadence_weight) if enable_cadence else 0.0)
        )
        existing_ns_score = (
            base_ns_score
            + (seed_score * float(seed_traits_weight) if enable_seed_traits else 0.0)
        )
        rows.append({
            "Core": core,
            "Stream": stream,
            "RankPos": rankpos,
            "BaseScoreRank": int(r.get("BaseScoreRank", rankpos)) if pd.notna(r.get("BaseScoreRank", rankpos)) else rankpos,
            "HitsWindow": int(r.get("HitsWindow", 0) or 0),
            "DrawsWindow": int(r.get("DrawsWindow", 0) or 0),
            "HitsPerWeek": hits_pw,
            "DaysSinceLastHit": due_pressure,
            "PosPctStrength": pos_strength,
            "Seed": seed or "",
            "SeedOnlyScore": float(seed_score),
            "SeedTraitsScore": float(seed_score),
            "CadenceScore": float(cadence),
            "BaseNSScore": float(base_ns_score),
            "ExistingNSScore": float(existing_ns_score),
            "CombinedSeed010": float(base_ns_score + 0.10 * seed_score),
            "CombinedSeed020": float(base_ns_score + 0.20 * seed_score),
            "CombinedSeed035": float(base_ns_score + 0.35 * seed_score),
            "CombinedSeed050": float(base_ns_score + 0.50 * seed_score),
            **match_audit,
            **{f"Trait__{t}": _flatten_feature_values(raw_features.get(t, []))
               for t in SEED_TRAIT_COLUMNS},
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    # Preserve the original app ranking as ExistingNSRank.
    out = _rank_desc(out, "ExistingNSScore", "ExistingNSRank")
    out["NSScore"] = out["ExistingNSScore"]
    out["NSRank"] = out["ExistingNSRank"]

    # Independent research lanes.
    for score_col, rank_col in [
        ("BaseNSScore", "BaseNSRank"),
        ("SeedOnlyScore", "SeedOnlyRank"),
        ("CombinedSeed010", "Combined010Rank"),
        ("CombinedSeed020", "Combined020Rank"),
        ("CombinedSeed035", "Combined035Rank"),
        ("CombinedSeed050", "Combined050Rank"),
    ]:
        rank_map = _rank_desc(out.copy(), score_col, rank_col)[["Stream", rank_col]]
        out = out.drop(columns=[rank_col], errors="ignore").merge(rank_map, on="Stream", how="left")

    return out.sort_values("ExistingNSRank").reset_index(drop=True)

def build_exact_ledger(
    df_all: pd.DataFrame,
    cores: list[str],
    start_date,
    end_date,
    max_dates: int,
    cfg: Any,
    *,
    callbacks: dict[str,Callable],
    lookups: dict[str,dict],
    weights: dict[str,float|bool],
    exclude_md: bool,
    progress: Callable[[float,str],None] | None = None,
) -> tuple[pd.DataFrame,pd.DataFrame]:
    df = df_all.copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df[df["Date"].notna()].copy()
    start_dt = pd.Timestamp(start_date)
    end_dt = pd.Timestamp(end_date)
    # Prediction/play dates are explicit calendar dates, not only dates already present
    # in the history. This is required for current-day validation:
    # history through 2026-06-17 -> play date 2026-06-18.
    test_dates = list(pd.date_range(start_dt.normalize(), end_dt.normalize(), freq="D"))
    if len(test_dates) > int(max_dates):
        test_dates = test_dates[-int(max_dates):]

    members_from_core = callbacks["members_from_core"]
    box_key = callbacks["box_key"]
    member_to_core: dict[str,str] = {}
    for c in cores:
        for m in _core_members(c, members_from_core):
            member_to_core[box_key(m)] = _norm_core(c)

    total = max(1, len(test_dates)*len(cores))
    done = 0
    all_parts = []
    winner_parts = []

    for test_date in test_dates:
        train_df = df[df["Date"] < pd.Timestamp(test_date)].copy()
        day_df = df[df["Date"].dt.normalize() == pd.Timestamp(test_date)].copy()
        if train_df.empty:
            continue

        actual: dict[tuple[str,str], list[str]] = {}
        for _, wr in day_df.iterrows():
            stream = str(wr.get("Stream","")).strip()
            winner = str(wr.get("Result","")).strip()
            core = member_to_core.get(box_key(winner))
            if core:
                actual.setdefault((core,stream), []).append(winner)

        for core in cores:
            tab = exact_ns_table(
                train_df,
                core,
                cfg,
                compute_stream_stats=callbacks["compute_stream_stats"],
                position_percentile_map=callbacks["position_percentile_map"],
                compute_seed_traits_score=callbacks["compute_seed_traits_score"],
                compute_cadence_score=callbacks["compute_cadence_score"],
                feature_values_for_seed=callbacks["feature_values_for_seed"],
                pos_lookup=lookups.get("pos",{}),
                neg_lookup=lookups.get("neg",{}),
                due_weight=float(weights["due_weight"]),
                pos_weight=float(weights["pos_weight"]),
                seed_traits_weight=float(weights["seed_traits_weight"]),
                cadence_weight=float(weights["cadence_weight"]),
                enable_seed_traits=bool(weights["enable_seed_traits"]),
                enable_cadence=bool(weights["enable_cadence"]),
                exclude_md=exclude_md,
            )
            if tab.empty:
                done += 1
                continue
            _history_through = pd.to_datetime(train_df["Date"]).max().date()
            _play_date = pd.Timestamp(test_date).date()
            tab.insert(0,"PlayDate",_play_date)
            tab.insert(1,"HistoryThrough",_history_through)
            tab.insert(2,"SeedAsOfDate",_history_through)
            tab.rename(columns={"Seed":"SeedUsedForPlayDate"}, inplace=True)
            tab["ExactStreamCoreHit"] = [
                bool(actual.get((_norm_core(core),str(s)),[])) for s in tab["Stream"].astype(str)
            ]
            tab["WinnerResult"] = [
                "|".join(actual.get((_norm_core(core),str(s)),[])) for s in tab["Stream"].astype(str)
            ]
            all_parts.append(tab)
            winner_parts.append(tab[tab["ExactStreamCoreHit"]].copy())
            done += 1
            if progress:
                progress(done/total, f"{pd.Timestamp(test_date).date()} core {_norm_core(core)} ({done}/{total})")

    all_df = pd.concat(all_parts, ignore_index=True) if all_parts else pd.DataFrame()
    wins = pd.concat(winner_parts, ignore_index=True) if winner_parts else pd.DataFrame()
    return all_df, wins


LANE_SPECS = [
    ("Base", "BaseNSScore", "BaseNSRank"),
    ("SeedOnly", "SeedOnlyScore", "SeedOnlyRank"),
    ("Combined010", "CombinedSeed010", "Combined010Rank"),
    ("Combined020", "CombinedSeed020", "Combined020Rank"),
    ("Combined035", "CombinedSeed035", "Combined035Rank"),
    ("Combined050", "CombinedSeed050", "Combined050Rank"),
    ("Existing", "ExistingNSScore", "ExistingNSRank"),
]

def lane_separation_audit(ledger: pd.DataFrame) -> pd.DataFrame:
    rows = []
    if ledger is None or ledger.empty:
        return pd.DataFrame()
    for lane, score_col, stream_rank_col in LANE_SPECS:
        if score_col not in ledger.columns:
            continue
        ranked = ledger.copy()
        ranked["CoreRankWithinStream"] = ranked.groupby(
            ["PlayDate","Stream"]
        )[score_col].rank(method="dense", ascending=False)
        wins = ranked[ranked["ExactStreamCoreHit"].fillna(False).astype(bool)].copy()
        for _, r in wins.iterrows():
            comp = ranked[
                (ranked["PlayDate"] == r["PlayDate"]) &
                (ranked["Stream"].astype(str) == str(r["Stream"])) &
                (ranked["Core"].astype(str) != str(r["Core"]))
            ]
            best = float(comp[score_col].max()) if not comp.empty else np.nan
            rows.append({
                "Lane": lane,
                "PlayDate": r["PlayDate"],
                "Stream": r["Stream"],
                "WinningCore": r["Core"],
                "WinningCoreStreamRank": r.get(stream_rank_col, np.nan),
                "WinningCoreRankAmongGroup": r["CoreRankWithinStream"],
                "WinningCoreScore": r[score_col],
                "BestCompetingScore": best,
                "WinnerMargin": float(r[score_col]) - best if pd.notna(best) else np.nan,
            })
    return pd.DataFrame(rows)

def lane_summary(ledger: pd.DataFrame) -> pd.DataFrame:
    sep = lane_separation_audit(ledger)
    rows = []
    if sep.empty:
        return pd.DataFrame()
    for lane, g in sep.groupby("Lane"):
        rows.append({
            "Lane": lane,
            "WinnerEvents": len(g),
            "WinningCoreTop1Pct": round((g["WinningCoreRankAmongGroup"] == 1).mean() * 100, 2),
            "WinningCoreTop2Pct": round((g["WinningCoreRankAmongGroup"] <= 2).mean() * 100, 2),
            "AverageWinningCoreRank": round(float(g["WinningCoreRankAmongGroup"].mean()), 4),
            "PositiveMarginPct": round((g["WinnerMargin"] > 0).mean() * 100, 2),
            "AverageWinnerMargin": round(float(g["WinnerMargin"].mean()), 8),
            "MedianWinnerMargin": round(float(g["WinnerMargin"].median()), 8),
        })
    return pd.DataFrame(rows).sort_values(
        ["WinningCoreTop1Pct","PositiveMarginPct","AverageWinnerMargin"],
        ascending=[False,False,False],
    )

def seed_trait_profile(ledger: pd.DataFrame) -> pd.DataFrame:
    """Long winner/nonwinner profile by core + raw trait + value."""
    if ledger is None or ledger.empty:
        return pd.DataFrame()
    trait_cols = [c for c in ledger.columns if c.startswith("Trait__")]
    rows = []
    base_rate = float(ledger["ExactStreamCoreHit"].fillna(False).mean())
    for core, cg in ledger.groupby("Core"):
        core_rate = float(cg["ExactStreamCoreHit"].fillna(False).mean())
        for col in trait_cols:
            trait = col.replace("Trait__", "", 1)
            temp = cg[[col,"ExactStreamCoreHit"]].copy()
            temp[col] = temp[col].fillna("").astype(str)
            temp = temp[temp[col] != ""]
            if temp.empty:
                continue
            # Multi-valued trait cells are split so each observed value is profiled.
            exploded = temp.assign(_value=temp[col].str.split("|")).explode("_value")
            for value, vg in exploded.groupby("_value"):
                n = len(vg)
                wins = int(vg["ExactStreamCoreHit"].fillna(False).sum())
                rate = wins / n if n else 0.0
                rows.append({
                    "Core": str(core).zfill(3),
                    "Trait": trait,
                    "Value": str(value),
                    "Rows": n,
                    "WinnerRows": wins,
                    "NonWinnerRows": n - wins,
                    "WinRate": rate,
                    "CoreBaselineWinRate": core_rate,
                    "GlobalBaselineWinRate": base_rate,
                    "LiftVsCore": (rate / core_rate) if core_rate > 0 else np.nan,
                    "LiftVsGlobal": (rate / base_rate) if base_rate > 0 else np.nan,
                })
    return pd.DataFrame(rows).sort_values(
        ["WinnerRows","LiftVsCore","Rows"], ascending=[False,False,False]
    ) if rows else pd.DataFrame()

def core_profile(core: str) -> dict[str,Any]:
    c=_norm_core(core)
    digs=[int(x) for x in c]
    return {
        "Core":c,
        "DigitSum":sum(digs),
        "DigitSpread":max(digs)-min(digs),
        "EvenCount":sum(d%2==0 for d in digs),
        "HighCount":sum(d>=5 for d in digs),
        "MirrorPairs":sum(1 for a,b in [("0","5"),("1","6"),("2","7"),("3","8"),("4","9")] if a in c and b in c),
    }

def pair_overlap(a: str,b: str) -> int:
    return len(set(_norm_core(a)) & set(_norm_core(b)))

def group_metrics(ledger: pd.DataFrame, group: list[str], group_name: str) -> dict[str,Any]:
    g=[_norm_core(x) for x in group]
    sub=ledger[ledger["Core"].astype(str).isin(g)].copy()
    if sub.empty:
        return {"Group":group_name,"Cores":",".join(g),"CoreCount":len(g)}
    sub["PlayDate"]=pd.to_datetime(sub["PlayDate"]).dt.date
    winners=sub[sub["ExactStreamCoreHit"].fillna(False).astype(bool)].copy()
    all_dates=sorted(sub["PlayDate"].unique())
    opp_days=int(winners["PlayDate"].nunique())

    ranked=sub.copy()
    ranked["CoreRankWithinStream"] = ranked.groupby(["PlayDate","Stream"])["NSScore"].rank(method="dense",ascending=False)
    wr=ranked[ranked["ExactStreamCoreHit"].fillna(False).astype(bool)].copy()

    margins=[]
    for _,r in wr.iterrows():
        comp=ranked[
            (ranked["PlayDate"]==r["PlayDate"]) &
            (ranked["Stream"].astype(str)==str(r["Stream"])) &
            (ranked["Core"].astype(str)!=str(r["Core"]))
        ]
        best=float(comp["NSScore"].max()) if not comp.empty else np.nan
        margins.append(float(r["NSScore"])-best if pd.notna(best) else np.nan)
    wr["WinnerMargin"]=margins

    overlaps=[pair_overlap(a,b) for a,b in itertools.combinations(g,2)]
    return {
        "Group":group_name,
        "Cores":",".join(g),
        "CoreCount":len(g),
        "DatesCovered":len(all_dates),
        "OpportunityDays":opp_days,
        "OpportunityDayPct":round(opp_days/max(1,len(all_dates))*100,2),
        "WinnerEvents":len(wr),
        "WinningCoreTop1Pct":round((wr["CoreRankWithinStream"]==1).mean()*100,2) if len(wr) else 0.0,
        "WinningCoreTop2Pct":round((wr["CoreRankWithinStream"]<=2).mean()*100,2) if len(wr) else 0.0,
        "AverageWinningCoreRank":round(float(wr["CoreRankWithinStream"].mean()),3) if len(wr) else np.nan,
        "AverageWinnerMargin":round(float(wr["WinnerMargin"].mean()),6) if len(wr) else np.nan,
        "PositiveMarginPct":round((wr["WinnerMargin"]>0).mean()*100,2) if len(wr) else 0.0,
        "AverageSharedDigits":round(float(np.mean(overlaps)),3) if overlaps else 0.0,
        "PairsSharing2Digits":int(sum(x>=2 for x in overlaps)),
    }

def _core_stats_from_ledger(ledger: pd.DataFrame) -> pd.DataFrame:
    winners=ledger[ledger["ExactStreamCoreHit"].fillna(False).astype(bool)].copy()
    if winners.empty:
        return pd.DataFrame(columns=["Core","WinnerEvents","WinnerDays"])
    return winners.groupby("Core").agg(
        WinnerEvents=("Core","size"),
        WinnerDays=("PlayDate","nunique"),
        AvgWinnerNSRank=("NSRank","mean"),
    ).reset_index()

def _objective(metrics: dict[str,Any]) -> float:
    # Balance opportunity and separation; diversity is a small tie-breaker, not the main goal.
    opp=float(metrics.get("OpportunityDayPct",0.0))
    top1=float(metrics.get("WinningCoreTop1Pct",0.0))
    top2=float(metrics.get("WinningCoreTop2Pct",0.0))
    pm=float(metrics.get("PositiveMarginPct",0.0))
    avg_overlap=float(metrics.get("AverageSharedDigits",3.0))
    return 0.45*opp + 0.25*top1 + 0.15*top2 + 0.10*pm + 0.05*(100*(1-avg_overlap/3))

def optimize_groups(ledger: pd.DataFrame, group_size: int = 12, current_group: list[str] | None = None) -> tuple[pd.DataFrame,pd.DataFrame]:
    """Heuristic search. Returns recommended groups and core profiles.

    This is intentionally not claimed as an exhaustive global optimum.
    """
    cores=sorted(ledger["Core"].astype(str).map(_norm_core).unique().tolist())
    if len(cores)<group_size:
        group_size=len(cores)
    cstats=_core_stats_from_ledger(ledger)
    if cstats.empty:
        return pd.DataFrame(),pd.DataFrame()

    ranked=cstats.sort_values(["WinnerDays","WinnerEvents","AvgWinnerNSRank"],ascending=[False,False,True])
    opportunity=ranked.head(group_size)["Core"].tolist()

    # Maximum-diversity greedy, seeded by the strongest opportunity core.
    diverse=[opportunity[0]]
    while len(diverse)<group_size:
        remaining=[c for c in cores if c not in diverse]
        def score(c):
            min_dist=min(3-pair_overlap(c,x) for x in diverse)
            avg_dist=np.mean([3-pair_overlap(c,x) for x in diverse])
            opp=float(ranked.set_index("Core").get("WinnerDays",pd.Series()).get(c,0))
            return (min_dist,avg_dist,opp)
        diverse.append(max(remaining,key=score))

    # Balanced local search from opportunity group.
    def improve(seed,name):
        best=list(seed)
        bm=group_metrics(ledger,best,name)
        bs=_objective(bm)
        improved=True
        loops=0
        while improved and loops<8:
            loops+=1
            improved=False
            outside=[c for c in cores if c not in best]
            for old in list(best):
                for new in outside:
                    cand=[new if x==old else x for x in best]
                    m=group_metrics(ledger,cand,name)
                    s=_objective(m)
                    if s>bs+1e-9:
                        best,bm,bs=cand,m,s
                        improved=True
                        break
                if improved:
                    break
        bm["ObjectiveScore"]=round(bs,4)
        return best,bm

    balanced,bm=improve(opportunity,"Recommended Balanced")
    separation,sm=improve(diverse,"Recommended Separation")
    rows=[]
    if current_group:
        cm=group_metrics(ledger,current_group,"Current Group")
        cm["ObjectiveScore"]=round(_objective(cm),4)
        rows.append(cm)
    om=group_metrics(ledger,opportunity,"Maximum Opportunity")
    om["ObjectiveScore"]=round(_objective(om),4)
    dm=group_metrics(ledger,diverse,"Maximum Diversity")
    dm["ObjectiveScore"]=round(_objective(dm),4)
    rows += [om,dm,bm,sm]
    rec=pd.DataFrame(rows).sort_values("ObjectiveScore",ascending=False).reset_index(drop=True)

    profiles=pd.DataFrame([core_profile(c) for c in cores]).merge(cstats,on="Core",how="left")
    return rec,profiles

def separation_rows(ledger: pd.DataFrame, group: list[str]) -> pd.DataFrame:
    g=[_norm_core(x) for x in group]
    sub=ledger[ledger["Core"].astype(str).isin(g)].copy()
    if sub.empty:
        return pd.DataFrame()
    sub["CoreRankWithinStream"]=sub.groupby(["PlayDate","Stream"])["NSScore"].rank(method="dense",ascending=False)
    wins=sub[sub["ExactStreamCoreHit"].fillna(False).astype(bool)].copy()
    rows=[]
    for _,r in wins.iterrows():
        comp=sub[
            (sub["PlayDate"]==r["PlayDate"]) &
            (sub["Stream"].astype(str)==str(r["Stream"])) &
            (sub["Core"].astype(str)!=str(r["Core"]))
        ]
        best=float(comp["NSScore"].max()) if not comp.empty else np.nan
        best_core=str(comp.sort_values("NSScore",ascending=False).iloc[0]["Core"]) if not comp.empty else ""
        rows.append({
            "PlayDate":r["PlayDate"],"Stream":r["Stream"],"WinningCore":r["Core"],
            "WinningCoreNSRank":r["NSRank"],"WinningCoreScore":r["NSScore"],
            "WinningCoreRankAmongGroup":r["CoreRankWithinStream"],
            "BestCompetingCore":best_core,"BestCompetingScore":best,
            "WinnerMargin":float(r["NSScore"])-best if pd.notna(best) else np.nan,
        })
    return pd.DataFrame(rows)

def render_core_set_lab(
    df_all: pd.DataFrame,
    cfg: Any,
    *,
    callbacks: dict[str,Callable],
    lookups: dict[str,dict],
    weights: dict[str,float|bool],
    exclude_md: bool,
):
    st.header("Core Set Separation Lab")
    st.caption(
        "Uses the original app's exact Northern Star functions and formula. "
        "Generate explicit play-date batches, then combine uploaded ledgers to recommend core groups."
    )

    mode=st.radio("Lab mode",["Generate exact ledger batch","Analyze / optimize uploaded ledgers"],horizontal=True)

    if mode.startswith("Generate"):
        pool_name=st.selectbox("Candidate core pool",["Working 8","Current 12","Tracked cores","All 120","Custom"], index=0)
        if pool_name=="Working 8":
            default=list(callbacks.get("working8", WORKING8))
        elif pool_name=="Current 12":
            default=CURRENT12
        elif pool_name=="Tracked cores":
            default=[_norm_core(c) for c in callbacks["core_presets"]]
        elif pool_name=="All 120":
            default=all_120_cores()
        else:
            default=[]
        custom=st.text_area("Core list (comma-separated)",value=",".join(default),height=100)
        cores=sorted(set(_norm_core(x) for x in re.split(r"[\s,;]+",custom) if x.strip()))
        dmin=pd.to_datetime(df_all["Date"],errors="coerce").min().date()
        dmax=pd.to_datetime(df_all["Date"],errors="coerce").max().date()
        next_play = dmax + pd.Timedelta(days=1)
        st.info(
            f"Loaded history through **{dmax}**. The original live Northern Star screen "
            f"uses the latest seed from that history and therefore represents play date **{next_play}**."
        )
        validate_current = st.checkbox(
            "Validate against current original-app screen (one core, next play date)",
            value=False,
            help="Sets Start/End to history-through + 1 day. Use Custom with one core, such as 389."
        )
        c1,c2,c3=st.columns(3)
        default_start = next_play if validate_current else max(dmin,dmax-pd.Timedelta(days=20))
        default_end = next_play if validate_current else dmax
        start=c1.date_input("Play-date start",value=default_start,min_value=dmin,max_value=next_play,key="lab_start_v2")
        end=c2.date_input("Play-date end",value=default_end,min_value=dmin,max_value=next_play,key="lab_end_v2")
        max_dates=c3.number_input("Max play dates in this batch",1,60,1 if validate_current else 20,1)
        st.caption(
            "For each PlayDate, SeedUsedForPlayDate is the latest result strictly before that date. "
            "Example: history through 2026-06-17 → play date 2026-06-18 → seed from 2026-06-17."
        )
        st.info(
            f"This batch will score up to {len(cores)} cores × {max_dates} play dates. "
            "Download the ZIP before running another batch."
        )
        if st.button("Run exact ledger batch",type="primary"):
            bar=st.progress(0.0,text="Starting")
            def prog(frac,msg):
                bar.progress(min(1.0,float(frac)),text=msg)
            ledger,winners=build_exact_ledger(
                df_all,cores,start,end,int(max_dates),cfg,
                callbacks=callbacks,lookups=lookups,weights=weights,
                exclude_md=exclude_md,progress=prog
            )
            bar.progress(1.0,text="Complete")
            st.success(f"Created {len(ledger):,} candidate rows and {len(winners):,} winner rows.")
            st.session_state["_core_lab_last_ledger"] = ledger.copy()
            current_available=[c for c in callbacks.get("working8", WORKING8) if c in set(ledger["Core"].astype(str))]
            rec,profiles=optimize_groups(ledger,group_size=min(12,len(cores)),current_group=current_available or None)
            if not rec.empty:
                st.subheader("Preliminary group recommendations for this batch")
                st.dataframe(rec,use_container_width=True,hide_index=True)
            _lane_summary = lane_summary(ledger)
            if not _lane_summary.empty:
                st.subheader("Baseline vs seed scoring lanes")
                st.dataframe(_lane_summary,use_container_width=True,hide_index=True)
                if not lookups.get("pos") and not lookups.get("neg"):
                    st.warning(
                        "No seed-trait lift lookup tables are active. Raw seed traits are still exported, "
                        "but SeedOnlyScore will remain zero until the original app loads its positive/negative trait tables."
                    )
            bio=io.BytesIO()
            with zipfile.ZipFile(bio,"w",zipfile.ZIP_DEFLATED) as z:
                z.writestr("00_BUILD.txt",BUILD)
                z.writestr("01_EXACT_NS_LEDGER.csv",ledger.to_csv(index=False))
                z.writestr("02_WINNER_ROWS.csv",winners.to_csv(index=False))
                z.writestr("03_GROUP_RECOMMENDATIONS.csv",rec.to_csv(index=False))
                z.writestr("04_CORE_PROFILES.csv",profiles.to_csv(index=False))
                _lane_summary = lane_summary(ledger)
                _lane_sep = lane_separation_audit(ledger)
                _trait_profile = seed_trait_profile(ledger)
                z.writestr("05_LANE_SUMMARY.csv",_lane_summary.to_csv(index=False))
                z.writestr("06_LANE_WINNER_SEPARATION.csv",_lane_sep.to_csv(index=False))
                z.writestr("07_SEED_TRAIT_PROFILE.csv",_trait_profile.to_csv(index=False))
                if not ledger.empty:
                    _seed_audit = ledger[[
                        "PlayDate","HistoryThrough","SeedAsOfDate","Core","Stream","SeedUsedForPlayDate"
                    ]].copy()
                    _seed_audit["ExpectedContract"] = "latest stream result strictly before PlayDate"
                    z.writestr("08_SEED_DATE_ALIGNMENT_AUDIT.csv",_seed_audit.to_csv(index=False))
            st.download_button("Download this batch (ZIP)",bio.getvalue(),"NS_LAB_BATCH.zip","application/zip")
    else:
        ups=st.file_uploader("Upload one or more 01_EXACT_NS_LEDGER.csv files",type=["csv"],accept_multiple_files=True)
        if ups:
            frames=[]
            for u in ups:
                try:
                    frames.append(pd.read_csv(u,dtype={"Core":str,"Seed":str}))
                except Exception as e:
                    st.warning(f"Could not read {u.name}: {e}")
            if frames:
                ledger=pd.concat(frames,ignore_index=True).drop_duplicates(["PlayDate","Core","Stream"],keep="last")
                pool=sorted(ledger["Core"].astype(str).map(_norm_core).unique())
                group_size=st.number_input("Recommended group size",min_value=2,max_value=min(30,len(pool)),value=min(12,len(pool)),step=1)
                st.session_state["_core_lab_last_ledger"] = ledger.copy()
                current_txt=st.text_input("Current comparison group",value=",".join([c for c in callbacks.get("working8", WORKING8) if c in pool]))
                current=[_norm_core(x) for x in re.split(r"[\s,;]+",current_txt) if x.strip()]
                if st.button("Find recommended groups",type="primary"):
                    rec,profiles=optimize_groups(ledger,int(group_size),current_group=current or None)
                    st.subheader("Recommended groups")
                    st.dataframe(rec,use_container_width=True,hide_index=True)
                    best=rec.iloc[0]["Cores"].split(",") if not rec.empty else []
                    sep=separation_rows(ledger,best) if best else pd.DataFrame()
                    st.subheader("Best group's winner separation")
                    st.dataframe(sep.head(300),use_container_width=True,hide_index=True)
                    bio=io.BytesIO()
                    with zipfile.ZipFile(bio,"w",zipfile.ZIP_DEFLATED) as z:
                        z.writestr("01_COMBINED_LEDGER.csv",ledger.to_csv(index=False))
                        z.writestr("02_GROUP_RECOMMENDATIONS.csv",rec.to_csv(index=False))
                        z.writestr("03_CORE_PROFILES.csv",profiles.to_csv(index=False))
                        z.writestr("04_BEST_GROUP_SEPARATION.csv",sep.to_csv(index=False))
                        z.writestr("05_LANE_SUMMARY.csv",lane_summary(ledger).to_csv(index=False))
                        z.writestr("06_LANE_WINNER_SEPARATION.csv",lane_separation_audit(ledger).to_csv(index=False))
                        z.writestr("07_SEED_TRAIT_PROFILE.csv",seed_trait_profile(ledger).to_csv(index=False))
                    st.download_button("Download lab analysis (ZIP)",bio.getvalue(),"NS_CORE_SET_ANALYSIS.zip","application/zip")

    st.divider()
    st.subheader("Member level — consumes stream/core ledger")
    st.caption(
        "Reads the Core Set Lab result grain directly: PlayDate + HistoryThrough + Stream + Core + SeedUsedForPlayDate. "
        "Rules remain in a separate CSV and are never hard-coded into the scoring functions."
    )
    default_rule_path = Path(__file__).resolve().parent / "member_pair_rules_v1.csv"
    uploaded_rule_file = st.file_uploader(
        "Optional replacement member-rule CSV", type=["csv"], key="member_rule_csv_upload"
    )
    try:
        active_rules = load_rules(uploaded_rule_file if uploaded_rule_file is not None else default_rule_path)
        st.success(f"Loaded {len(active_rules)} active member rules from " + (uploaded_rule_file.name if uploaded_rule_file is not None else "member_pair_rules_v1.csv"))
        with st.expander("Active member rules", expanded=False):
            st.dataframe(active_rules, use_container_width=True, hide_index=True)
    except Exception as exc:
        st.error(f"Member rule file failed validation: {exc}")
        active_rules = pd.DataFrame()

    ledger_source = st.radio(
        "Member input",
        ["Use latest Core Set Lab ledger in this session", "Upload stream/core result CSV"],
        horizontal=True,
        key="member_input_mode",
    )
    member_input = None
    if ledger_source.startswith("Use latest"):
        member_input = st.session_state.get("_core_lab_last_ledger")
        if member_input is None or getattr(member_input, "empty", True):
            st.info("Run or upload a Core Set Lab ledger above first.")
    else:
        member_upload = st.file_uploader(
            "Upload core-level result CSV", type=["csv"], key="member_core_result_upload"
        )
        if member_upload is not None:
            try:
                member_input = pd.read_csv(
                    member_upload, dtype={"Core": str, "Seed": str, "SeedUsedForPlayDate": str}
                )
            except Exception as exc:
                st.error(f"Could not read member input: {exc}")

    if member_input is not None and not getattr(member_input, "empty", True) and not active_rules.empty:
        required = {"Core", "Stream"}
        if not required.issubset(member_input.columns):
            st.error("Member input must contain Core and Stream.")
        elif "SeedUsedForPlayDate" not in member_input.columns and "Seed" not in member_input.columns:
            st.error("Member input must contain SeedUsedForPlayDate or Seed.")
        elif st.button("Apply member rules to stream/core results", type="primary", key="apply_member_rules_btn"):
            try:
                decisions, fire_audit, fire_summary = apply_member_rules(member_input, active_rules)
                st.session_state["_member_decisions"] = decisions
                st.session_state["_member_fire_audit"] = fire_audit
                st.session_state["_member_fire_summary"] = fire_summary
                st.success(
                    f"Member rules applied to {len(decisions):,} stream/core rows. "
                    f"No-rule rows: {int(decisions['MemberNoRuleFired'].sum()):,}; "
                    f"conflicts: {int(decisions['MemberRuleConflict'].sum()):,}."
                )
            except Exception as exc:
                st.exception(exc)

    decisions = st.session_state.get("_member_decisions")
    fire_audit = st.session_state.get("_member_fire_audit")
    fire_summary = st.session_state.get("_member_fire_summary")
    if isinstance(decisions, pd.DataFrame) and not decisions.empty:
        st.markdown("##### Member decisions")
        show_cols = [
            c for c in [
                "PlayDate", "HistoryThrough", "Stream", "Core", "SeedUsedForPlayDate", "Seed",
                "MemberRecommendedPair", "MemberRecommendedBoxedMembers", "MemberSelectedRuleID",
                "MemberRulesFired", "MemberNoRuleFired", "MemberRuleConflict",
                "WinnerBoxedMember", "WinnerCanonicalIndex", "MemberWinnerPreserved",
                "MemberLossCausingRuleIDs", "MemberDecisionReason",
            ] if c in decisions.columns
        ]
        st.dataframe(decisions[show_cols], use_container_width=True, hide_index=True)

        c1, c2, c3 = st.columns(3)
        c1.metric("Rows", f"{len(decisions):,}")
        c2.metric("No rule fired", f"{int(decisions['MemberNoRuleFired'].sum()):,}")
        c3.metric("Conflicts", f"{int(decisions['MemberRuleConflict'].sum()):,}")

        if "WinnerCanonicalIndex" in decisions.columns and decisions["WinnerCanonicalIndex"].astype(str).str.len().gt(0).any():
            losses = decisions[~decisions["MemberWinnerPreserved"].fillna(True).astype(bool)]
            st.markdown("##### Winner-loss attribution")
            if losses.empty:
                st.success("No labelled winner was removed by the active member rules in this input.")
            else:
                st.error(f"{len(losses):,} labelled winner row(s) were removed.")
                st.dataframe(losses[show_cols], use_container_width=True, hide_index=True)

        if isinstance(fire_summary, pd.DataFrame):
            st.markdown("##### Rule firing / non-firing summary")
            st.dataframe(fire_summary, use_container_width=True, hide_index=True)
        if isinstance(fire_audit, pd.DataFrame):
            with st.expander("Full row-by-rule firing audit", expanded=False):
                st.dataframe(fire_audit, use_container_width=True, hide_index=True)

        payload = member_results_zip(
            decisions,
            fire_audit if isinstance(fire_audit, pd.DataFrame) else pd.DataFrame(),
            fire_summary if isinstance(fire_summary, pd.DataFrame) else pd.DataFrame(),
            active_rules,
        )
        st.download_button(
            "Download member results + audits (ZIP)",
            payload,
            "MEMBER_LEVEL_RESULTS.zip",
            "application/zip",
            key="download_member_results_zip",
        )

    # One-click Lab package. The Lab intentionally retains access to the broader core catalog.
    _lab_files: dict[str, pd.DataFrame | str] = {
        "BUILD_INFO.txt": f"LAB BUILD: {BUILD}\n",
    }
    _lab_ledger = st.session_state.get("_core_lab_last_ledger")
    if isinstance(_lab_ledger, pd.DataFrame) and not _lab_ledger.empty:
        _lab_files["core_lab_ledger.csv"] = _lab_ledger
    if isinstance(decisions, pd.DataFrame) and not decisions.empty:
        _lab_files["member_decisions.csv"] = decisions
    if isinstance(fire_audit, pd.DataFrame) and not fire_audit.empty:
        _lab_files["member_rule_firing_audit.csv"] = fire_audit
    if isinstance(fire_summary, pd.DataFrame) and not fire_summary.empty:
        _lab_files["member_rule_summary.csv"] = fire_summary
    if isinstance(active_rules, pd.DataFrame) and not active_rules.empty:
        _lab_files["active_member_rules.csv"] = active_rules
    if len(_lab_files) > 1:
        _bio_all = io.BytesIO()
        with zipfile.ZipFile(_bio_all, "w", zipfile.ZIP_DEFLATED) as _z:
            for _name, _obj in _lab_files.items():
                if isinstance(_obj, pd.DataFrame):
                    _z.writestr(_name, _obj.to_csv(index=False).encode("utf-8"))
                else:
                    _z.writestr(_name, str(_obj).encode("utf-8"))
        st.download_button(
            "Download all Core Set Lab outputs",
            _bio_all.getvalue(),
            "CORE_SET_LAB_ALL_OUTPUTS.zip",
            "application/zip",
            key="download_all_core_lab_outputs",
            use_container_width=True,
        )

