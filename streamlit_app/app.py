#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RaceX Streamlit App - Horse Racing Analysis
Reuses all business logic from the main codebase.
"""

import sys
import os
import sqlite3

# Add parent directory to path so we can import existing modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import pandas as pd
import numpy as np
from datetime import date
import io
import re
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use("Agg")
import plotly.express as px
import requests
from bs4 import BeautifulSoup

# ── Import existing business logic ──────────────────────────────────────────
from data_sources import data_source_manager
from meeting_cache import get_cached_meetings, cache_meetings
from race_scraper_app import (
    scrape_meeting_urls,
    compute_composite_score,
    compute_prognosis,
    compute_summary_horses,
    compute_trending_horses,
    get_pdf_header_footer,
    sanitize_horse_list,
    normalize_composite_columns,
    generate_trotting_prognosis,
    compute_trotting_summary_horses,
    analyze_trotting_fitness,
    analyze_trotting_performance,
    analyze_trotting_trend,
    analyze_trotting_shoeing,
    analyze_trotting_disqualification_risk,
    analyze_trotting_summary_prognosis,
    analyze_fitness_if,
    analyze_class_ic,
    analyze_success_coeff,
    analyze_weight_stability,
    analyze_light_weight_surprise,
    analyze_odds_divergence,
    analyze_consistency_score,
    analyze_underperforming_favorites,
    reducing_system,
    generate_trotting_bets,
    convert_date_to_french_url,
)
from favorable_cordes import compute_favorable_corde_horses
from model_functions import (
    detect_race_type_from_html,
    clean_ref_course,
    clean_distance,
    clean_gains_trot,
    time_to_seconds,
    bayesian_performance_score,
    success_coefficient,
    compute_d_perf,
    parse_performance_string,
    parse_shoeing_features,
)

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="RaceX - Horse Racing Analysis",
    page_icon="🏇",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Global styling ────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    :root {
        --bg: #f6f7fb;
        --card: #ffffff;
        --ink: #1f2937;
        --muted: #6b7280;
        --accent: #0f766e;
        --accent-2: #1d4ed8;
        --border: #e5e7eb;
    }
    .stApp {
        background: radial-gradient(1200px 600px at 20% 0%, #eef2ff 0%, #f6f7fb 40%, #f6f7fb 100%);
        color: var(--ink);
    }
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #f8fafc 0%, #eef2f7 100%);
        border-right: 1px solid var(--border);
    }
    /* Hide built-in multipage navigation to use custom ordering */
    [data-testid="stSidebarNav"] {
        display: none;
    }
    h1, h2, h3, h4, h5 {
        font-family: "Georgia", "Times New Roman", serif;
        letter-spacing: 0.3px;
    }
    .block-container {
        padding-top: 1.2rem;
    }
    .stTabs [data-baseweb="tab"] {
        background: #ffffff;
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 8px 14px;
        margin-right: 6px;
    }
    .stTabs [data-baseweb="tab"][aria-selected="true"] {
        background: #ecfeff;
        border-color: #67e8f9;
    }
    .stDataFrame, .stTable {
        border: 1px solid var(--border);
        border-radius: 12px;
        overflow: hidden;
        box-shadow: 0 6px 18px rgba(0,0,0,0.04);
    }
    .stMetric {
        background: var(--card);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 10px 12px;
        box-shadow: 0 6px 18px rgba(0,0,0,0.04);
    }
    button[kind="primary"] {
        background: linear-gradient(135deg, var(--accent) 0%, #0ea5a4 100%);
        border: none;
    }
    button {
        border-radius: 10px !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Session state defaults ───────────────────────────────────────────────────
for key, default in {
    "race_df": pd.DataFrame(),       # full scraped data (all races in the meeting)
    "filtered_df": pd.DataFrame(),   # slice for the selected REF_COURSE
    "composite_df": pd.DataFrame(),
    "race_type": "trot",
    "race_type_detected": None,
    "meetings": {},
    "selected_ref_course": None,
    "last_bets_df": None,
    "last_bets_label": "",
    "last_summary_df": None,
    "current_weights": None,
    "flat_analysis": None,
    "mc_last_key": None,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default


# ── Helpers ──────────────────────────────────────────────────────────────────
def show_df(df: pd.DataFrame, height: int = 400):
    if df is None or df.empty:
        st.info("No data available.")
    else:
        st.dataframe(df, use_container_width=True, height=height)


def _find_col(df: pd.DataFrame, *candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _detect_horse_num_col(df: pd.DataFrame):
    return _find_col(df, "NÂ°", "N°", "N", "Numero", "NUM", "Num")


def _get_race_meta(df: pd.DataFrame):
    hippo_col = _find_col(df, "HIPPODROME", "Hippodrome", "HIPPO", "Lieu", "LIEU")
    dist_col = _find_col(df, "DISTANCE", "Distance", "DIST")
    hippodrome = None
    distance = None
    if hippo_col:
        try:
            hippodrome = df[hippo_col].dropna().iloc[0]
        except Exception:
            hippodrome = None
    if dist_col:
        try:
            distance = df[dist_col].dropna().iloc[0]
        except Exception:
            distance = None
    return hippodrome, distance


def _weights_config(race_type: str):
    if race_type == "trot":
        return {
            "S_COEFF": ("S_COEFF (success)", 0.20),
            "COTE": ("COTE (odds)", 0.15),
            "DQ_Risk_Inverted": ("DQ Risk (lower better)", 0.20),
            "Fitness": ("Fitness (FA/FM)", 0.15),
            "Slope": ("Trend slope", 0.15),
            "REC": ("REC (recent races)", 0.10),
            "Shoeing_Agg": ("Shoeing aggressiveness", 0.05),
        }
    return {
        "IC": ("IC (class)", 0.20),
        "S_COEFF": ("S_COEFF (success)", 0.20),
        "IF": ("IF (fitness)", 0.15),
        "n_weight": ("n_weight (stability)", 0.10),
        "COTE": ("COTE (odds)", 0.20),
        "Corde": ("Corde (track)", 0.10),
        "VALEUR": ("VALEUR", 0.05),
    }


def _render_weights_ui(race_type: str):
    cfg = _weights_config(race_type)
    keys = []
    for metric, (_, default) in cfg.items():
        key = f"w_{race_type}_{metric}"
        keys.append((metric, key, default))
        if key not in st.session_state:
            st.session_state[key] = int(default * 100)

    st.markdown("Adjust weights as percentages. They will be normalized automatically.")
    cols = st.columns(2)
    for i, (metric, key, default) in enumerate(keys):
        with cols[i % 2]:
            label = cfg[metric][0]
            st.slider(label, 0, 100, key=key)

    total = sum(st.session_state[key] for _, key, _ in keys)
    st.caption(f"Total: {total}% (auto-normalized for scoring)")

    if st.button("Reset Weights", key=f"reset_{race_type}_weights"):
        for _, key, default in keys:
            st.session_state[key] = int(default * 100)
        total = sum(st.session_state[key] for _, key, _ in keys)

    if total <= 0:
        return {m: d for m, (_, d) in cfg.items()}

    weights = {metric: (st.session_state[key] / total) for metric, key, _ in keys}
    return weights


def _weights_signature_flat() -> str:
    weights = st.session_state.get("current_weights") or {}
    if not isinstance(weights, dict):
        return ""
    parts = [(str(k), round(float(v), 6)) for k, v in weights.items()]
    # MC params (flat)
    parts.extend([
        ("mc_comp_trials", st.session_state.get("mc_comp_trials")),
        ("mc_comp_sigma", st.session_state.get("mc_comp_sigma")),
        ("mc_comp_seed", st.session_state.get("mc_comp_seed")),
        ("mc_metric_trials", st.session_state.get("mc_metric_trials")),
        ("mc_metric_sigma", st.session_state.get("mc_metric_sigma")),
        ("mc_metric_seed", st.session_state.get("mc_metric_seed")),
        ("mc_odds_trials", st.session_state.get("mc_odds_trials")),
        ("mc_odds_alpha", st.session_state.get("mc_odds_alpha")),
        ("mc_odds_temp", st.session_state.get("mc_odds_temp")),
        ("mc_odds_seed", st.session_state.get("mc_odds_seed")),
        ("mc_bet_trials", st.session_state.get("mc_bet_trials")),
        ("mc_bet_alpha", st.session_state.get("mc_bet_alpha")),
        ("mc_bet_temp", st.session_state.get("mc_bet_temp")),
        ("mc_bet_seed", st.session_state.get("mc_bet_seed")),
        ("mc_bet_use_topk", st.session_state.get("mc_bet_use_topk")),
        ("mc_bet_maxk", st.session_state.get("mc_bet_maxk")),
        ("mc_scenarios", st.session_state.get("mc_scenarios")),
        ("mc_scen_sigma", st.session_state.get("mc_scen_sigma")),
        ("mc_scen_corde", st.session_state.get("mc_scen_corde")),
        ("mc_scen_seed", st.session_state.get("mc_scen_seed")),
        ("mc_hist_trials", st.session_state.get("mc_hist_trials")),
        ("mc_hist_dist", st.session_state.get("mc_hist_dist")),
        ("mc_hist_seed", st.session_state.get("mc_hist_seed")),
    ])
    return repr(sorted((k, v) for k, v in parts))


def _weights_signature_trot() -> str:
    # Include trot MC weight sliders + composite weights
    parts = [
        ("mc_trot_w_scoeff", st.session_state.get("mc_trot_w_scoeff")),
        ("mc_trot_w_cote", st.session_state.get("mc_trot_w_cote")),
        ("mc_trot_w_dq", st.session_state.get("mc_trot_w_dq")),
        ("mc_trot_w_fit", st.session_state.get("mc_trot_w_fit")),
        ("mc_trot_w_slope", st.session_state.get("mc_trot_w_slope")),
        ("mc_trot_w_rec", st.session_state.get("mc_trot_w_rec")),
        ("mc_trot_w_shoe", st.session_state.get("mc_trot_w_shoe")),
        # MC params (trot)
        ("mc_trot_comp_trials", st.session_state.get("mc_trot_comp_trials")),
        ("mc_trot_comp_sigma", st.session_state.get("mc_trot_comp_sigma")),
        ("mc_trot_comp_seed", st.session_state.get("mc_trot_comp_seed")),
        ("mc_trot_metric_trials", st.session_state.get("mc_trot_metric_trials")),
        ("mc_trot_metric_sigma", st.session_state.get("mc_trot_metric_sigma")),
        ("mc_trot_metric_seed", st.session_state.get("mc_trot_metric_seed")),
        ("mc_trot_odds_trials", st.session_state.get("mc_trot_odds_trials")),
        ("mc_trot_odds_alpha", st.session_state.get("mc_trot_odds_alpha")),
        ("mc_trot_odds_temp", st.session_state.get("mc_trot_odds_temp")),
        ("mc_trot_odds_seed", st.session_state.get("mc_trot_odds_seed")),
        ("mc_trot_bet_trials", st.session_state.get("mc_trot_bet_trials")),
        ("mc_trot_bet_alpha", st.session_state.get("mc_trot_bet_alpha")),
        ("mc_trot_bet_temp", st.session_state.get("mc_trot_bet_temp")),
        ("mc_trot_bet_seed", st.session_state.get("mc_trot_bet_seed")),
        ("mc_trot_bet_use_topk", st.session_state.get("mc_trot_bet_use_topk")),
        ("mc_trot_bet_maxk", st.session_state.get("mc_trot_bet_maxk")),
        ("mc_trot_scenarios", st.session_state.get("mc_trot_scenarios")),
        ("mc_trot_scen_sigma", st.session_state.get("mc_trot_scen_sigma")),
        ("mc_trot_scen_dq_bias", st.session_state.get("mc_trot_scen_dq_bias")),
        ("mc_trot_scen_seed", st.session_state.get("mc_trot_scen_seed")),
        ("mc_trot_hist_trials", st.session_state.get("mc_trot_hist_trials")),
        ("mc_trot_hist_dist", st.session_state.get("mc_trot_hist_dist")),
        ("mc_trot_hist_seed", st.session_state.get("mc_trot_hist_seed")),
    ]
    weights = st.session_state.get("current_weights") or {}
    if isinstance(weights, dict):
        parts.extend([(f"w:{k}", round(float(v), 6)) for k, v in weights.items()])
    return repr(sorted((k, v) for k, v in parts))


def _df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def _parse_numeric_series(ser: pd.Series) -> pd.Series:
    try:
        if ser is None or ser.empty:
            return pd.Series(dtype=float)
        s = ser.astype(str).str.replace(',', '.').str.extract(r'([0-9]+\.?[0-9]*)')[0]
        return pd.to_numeric(s, errors='coerce')
    except Exception:
        return pd.to_numeric(ser, errors='coerce')


def _clean_scraped_data(df: pd.DataFrame, race_type: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    try:
        if race_type == "trot":
            # Keep original column names to match desktop logic
            # Map key columns if present under alternative names
            if "RACE_TYPE" not in out.columns and "race_type" in out.columns:
                out["RACE_TYPE"] = out["race_type"]

            # Keep REF_COURSE as provided by zone_trot.py; only set if missing
            if "REF_COURSE" not in out.columns:
                alt = _find_col(out, "COURSE_ID", "RACE_ID", "race_id")
                if alt:
                    out["REF_COURSE"] = out[alt].astype(str)

            dist_col = _find_col(out, "DIST.", "DISTANCE", "DIST")
            if dist_col:
                out[dist_col] = out[dist_col].apply(clean_distance)

            if "GAINS" in out.columns:
                out["GAINS"] = out["GAINS"].apply(clean_gains_trot)
            if "REC." in out.columns:
                out["REC."] = out["REC."].apply(time_to_seconds)

            # Compute trotting-specific metrics to match desktop app
            out = _compute_trotting_metrics_df(out)

        else:
            # Normalize column names to avoid encoding mismatches (e.g., "REF_COURSE", "REF COURSE", "REF COURSE")
            out.columns = [re.sub(r"\s+", "_", str(c)).strip() for c in out.columns]
            # Harmonize key column names used in flat_zone cleaning
            rename_map = {}
            for c in out.columns:
                cl = str(c).lower()
                if cl in ("c.", "c", "corde"):
                    rename_map[c] = "CORDE"
                elif cl in ("cote", "odds"):
                    rename_map[c] = "COTE"
                elif cl in ("vh",):
                    rename_map[c] = "VALEUR"
                elif "dern" in cl and "perf" in cl:
                    rename_map[c] = "MUSIQUE"
                elif cl == "race_id":
                    rename_map[c] = "REF_COURSE"
                elif cl == "cheval":
                    rename_map[c] = "CHEVAL"
                elif cl == "poids":
                    rename_map[c] = "POIDS"
                elif cl in ("dech.", "dech"):
                    rename_map[c] = "DECH"
                elif cl in ("gains",):
                    rename_map[c] = "GAINS"
            if rename_map:
                out = out.rename(columns=rename_map)

            if "REF_COURSE" in out.columns:
                out["REF_COURSE"] = out["REF_COURSE"].apply(clean_ref_course)
                out["REF_COURSE"] = out["REF_COURSE"].astype(str).str.strip()
                out["REF_COURSE"] = out["REF_COURSE"].replace({"None": None, "nan": None, "NaN": None, "": None})
            if "DIST." in out.columns:
                out["DIST."] = out["DIST."].apply(clean_distance)
            if "DISTANCE" in out.columns:
                out["DISTANCE"] = out["DISTANCE"].apply(clean_distance)
            # Flat-specific cleaning & derived metrics (align with desktop logic)
            if "CHEVAL" in out.columns:
                out["CHEVAL"] = out["CHEVAL"].astype(str).str.strip()
            if "CORDE" in out.columns:
                out["CORDE"] = out["CORDE"].astype(str).str.replace("c:", "", regex=False).str.strip()
            if "GAINS" in out.columns:
                out["GAINS"] = out["GAINS"].astype(str).str.replace("€", "", regex=False).str.replace("\xa0", "", regex=False).str.strip()
            # Clean numeric columns VALEUR/POIDS/DECH
            def _clean_value(val):
                try:
                    if pd.isna(val):
                        return None
                    val_str = str(val).replace(",", ".").strip()
                    if val_str in ["-", ""]:
                        return None
                    num_val = float(val_str)
                    if num_val >= 100:
                        num_val = num_val / 10.0
                    return num_val
                except Exception:
                    return None
            for col in ["VALEUR", "POIDS", "DECH"]:
                if col in out.columns:
                    out[col] = out[col].apply(_clean_value)
            # n_weight
            if "PAST_POIDS" in out.columns and "POIDS" in out.columns:
                try:
                    out["n_weight"] = pd.to_numeric(out["PAST_POIDS"], errors="coerce") - pd.to_numeric(out["POIDS"], errors="coerce")
                except Exception:
                    pass
            # IC
            if "POIDS" in out.columns and "COTE" in out.columns:
                try:
                    out["IC"] = pd.to_numeric(out["POIDS"], errors="coerce") - pd.to_numeric(out["COTE"], errors="coerce")
                except Exception:
                    pass
            # IF and S_COEFF from MUSIQUE if present
            if "MUSIQUE" in out.columns:
                try:
                    out["IF"] = out["MUSIQUE"].apply(lambda x: bayesian_performance_score(str(x) if pd.notna(x) else ""))
                except Exception:
                    pass
                try:
                    out["S_COEFF"] = out["MUSIQUE"].apply(lambda x: success_coefficient(str(x), "a") if pd.notna(x) else 0.0)
                except Exception:
                    pass
    except Exception:
        return out
    return out


def _auto_detect_race_type(url: str):
    if not url:
        return None
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=10)
        if not resp.ok:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        return detect_race_type_from_html(soup)
    except Exception:
        return None


def _compute_dq_risk_score(df: pd.DataFrame) -> pd.Series:
    try:
        risk_series = pd.Series(0.0, index=df.index)
        metrics_data = pd.DataFrame(index=df.index)
        if 'disq_count' in df.columns:
            vals = pd.to_numeric(df['disq_count'], errors='coerce').fillna(0)
            metrics_data['disq_count'] = vals / vals.max() if vals.max() > 0 else 0.0
        if 'disq_harness_rate' in df.columns:
            vals = pd.to_numeric(df['disq_harness_rate'], errors='coerce').fillna(0)
            metrics_data['disq_harness_rate'] = vals
        if 'disq_mounted_rate' in df.columns:
            vals = pd.to_numeric(df['disq_mounted_rate'], errors='coerce').fillna(0)
            metrics_data['disq_mounted_rate'] = vals
        if 'recent_disq_count' in df.columns:
            vals = pd.to_numeric(df['recent_disq_count'], errors='coerce').fillna(0)
            metrics_data['recent_disq_count'] = vals / vals.max() if vals.max() > 0 else 0.0
        if 'recent_disq_rate' in df.columns:
            vals = pd.to_numeric(df['recent_disq_rate'], errors='coerce').fillna(0)
            metrics_data['recent_disq_rate'] = vals

        weights = {
            'disq_count': 0.3,
            'disq_harness_rate': 0.2,
            'disq_mounted_rate': 0.2,
            'recent_disq_count': 0.2,
            'recent_disq_rate': 0.1
        }
        total_weight = sum(weights.get(c, 0) for c in metrics_data.columns)
        if total_weight > 0:
            for col in metrics_data.columns:
                risk_series += metrics_data[col] * weights.get(col, 0)
            risk_series = risk_series / total_weight
        return (risk_series * 100).round(2)
    except Exception:
        return pd.Series(0.0, index=df.index)


def _compute_trotting_metrics_df(df: pd.DataFrame) -> pd.DataFrame:
    try:
        out = df.copy()
        # detect performance column
        perf_col = None
        for c in out.columns:
            cl = str(c).lower()
            if "dern" in cl and "perf" in cl:
                perf_col = c
                break
        if not perf_col:
            return out

        # Determine discipline from RACE_TYPE
        discipline = 'a'
        if 'RACE_TYPE' in out.columns and out['RACE_TYPE'].iloc[0]:
            if 'monté' in str(out['RACE_TYPE'].iloc[0]).lower() or 'monte' in str(out['RACE_TYPE'].iloc[0]).lower():
                discipline = 'm'

        # FA/FM
        if 'FA' not in out.columns and 'FM' not in out.columns:
            out['FA'] = out[perf_col].apply(lambda x: compute_d_perf(x).get('a') if pd.notna(x) else None)
            out['FM'] = out[perf_col].apply(lambda x: compute_d_perf(x).get('m') if pd.notna(x) else None)

        # S_COEFF
        if 'S_COEFF' not in out.columns:
            out['S_COEFF'] = out[perf_col].apply(lambda x: success_coefficient(x, discipline) if pd.notna(x) else 0.0)

        # Normalize S_COEFF
        if 'S_COEFF' in out.columns and 'S_COEFF_norm' not in out.columns:
            s_min = out['S_COEFF'].min()
            s_max = out['S_COEFF'].max()
            if s_max > s_min:
                out['S_COEFF_norm'] = ((out['S_COEFF'] - s_min) / (s_max - s_min) * 100).round(2)

        # Performance metrics
        perf_metrics_list = []
        for _, row in out.iterrows():
            perf_str = row.get(perf_col)
            if pd.notna(perf_str):
                perf_metrics = parse_performance_string(str(perf_str))
            else:
                perf_metrics = pd.Series({
                    'num_races': 0, 'avg_rank': None, 'best_rank': None, 'worst_rank': None,
                    'time_decay_avg_rank': None, 'disq_count': 0, 'disq_harness_rate': 0,
                    'disq_mounted_rate': 0, 'recent_avg_rank': None, 'recent_disq_count': 0,
                    'recent_disq_rate': 0, 'mounted_ratio': None, 'trend_rank_slope': None,
                    'trend_label': 'Unknown'
                })
            perf_metrics_list.append(perf_metrics)
        perf_df = pd.DataFrame(perf_metrics_list)
        for col in perf_df.columns:
            out[col] = perf_df[col].values

        # Shoeing features
        def_col = 'DEF.' if 'DEF.' in out.columns else ('Def' if 'Def' in out.columns else None)
        if def_col:
            shoe_feats = out[def_col].apply(lambda x: parse_shoeing_features(x))
            shoe_df = pd.DataFrame(list(shoe_feats))
            for col in shoe_df.columns:
                if col not in out.columns:
                    out[col] = shoe_df[col].values

        # DQ_Risk
        out['DQ_Risk'] = _compute_dq_risk_score(out)
        return out
    except Exception:
        return df


def _mc_rank_from_composite(composite_df: pd.DataFrame, trials: int = 2000, sigma: float = 0.05, seed=None) -> pd.DataFrame:
    if composite_df is None or composite_df.empty or "Composite" not in composite_df.columns:
        return pd.DataFrame()
    horse_col = _find_col(composite_df, "Cheval", "CHEVAL")
    if not horse_col:
        return pd.DataFrame()
    names = composite_df[horse_col].astype(str).values
    num_col = _detect_horse_num_col(composite_df)
    nums = composite_df[num_col].astype(str).values if num_col else None
    comp = pd.to_numeric(composite_df["Composite"], errors="coerce").fillna(0.5).values
    n = len(comp)
    if n == 0:
        return pd.DataFrame()
    trials = max(50, int(trials))
    sigma = max(0.001, float(sigma))
    rng = np.random.default_rng(seed)
    samples = rng.normal(comp, sigma, size=(trials, n))
    samples = np.clip(samples, 0.0, 1.0)
    order = np.argsort(-samples, axis=1)
    ranks = np.empty_like(order)
    rows = np.arange(trials)[:, None]
    ranks[rows, order] = np.arange(1, n + 1)
    p1 = (ranks == 1).mean(axis=0)
    p3 = (ranks <= 3).mean(axis=0)
    p5 = (ranks <= 5).mean(axis=0)
    avg_rank = ranks.mean(axis=0)
    out = pd.DataFrame({
        "N°": nums if nums is not None else "",
        "Cheval": names,
        "Composite": comp,
        "P(win)": p1,
        "P(top3)": p3,
        "P(top5)": p5,
        "Avg Rank": avg_rank,
    })
    return out.sort_values("P(win)", ascending=False)


def _mc_rank_from_metrics(race_df: pd.DataFrame, composite_df: pd.DataFrame, weights: dict,
                          trials: int = 2000, sigma: float = 0.05, seed=None) -> pd.DataFrame:
    if race_df is None or race_df.empty or composite_df is None or composite_df.empty:
        return pd.DataFrame()
    horse_col = _find_col(composite_df, "Cheval", "CHEVAL")
    race_horse_col = _find_col(race_df, "CHEVAL", "Cheval")
    if not horse_col or not race_horse_col:
        return pd.DataFrame()
    names = composite_df[horse_col].astype(str).values
    num_col = _detect_horse_num_col(composite_df)
    nums = composite_df[num_col].astype(str).values if num_col else None

    def _series_by_name(df: pd.DataFrame, col: str, name_col: str) -> pd.Series:
        ser = pd.to_numeric(df[col], errors="coerce")
        ser.index = df[name_col].astype(str)
        return ser

    metrics_df = pd.DataFrame(index=names)
    for metric in weights.keys():
        norm_col = f"_norm_{metric}"
        if norm_col in race_df.columns:
            ser = _series_by_name(race_df, norm_col, race_horse_col)
            metrics_df[metric] = ser.reindex(metrics_df.index).fillna(0.5).values
            continue
        if metric in composite_df.columns:
            ser = _series_by_name(composite_df, metric, horse_col)
            metrics_df[metric] = ser.reindex(metrics_df.index).fillna(0.5).values
            continue
        if metric == "COTE":
            cote_col = _find_col(race_df, "COTE", "Cote")
            if cote_col:
                ser = _series_by_name(race_df, cote_col, race_horse_col)
                inv = -ser
                if inv.dropna().empty or inv.max() == inv.min():
                    metrics_df[metric] = 0.5
                else:
                    norm = (inv - inv.min()) / (inv.max() - inv.min())
                    metrics_df[metric] = norm.reindex(metrics_df.index).fillna(0.5).values
            continue

    metrics_used = [m for m in weights.keys() if m in metrics_df.columns]
    if not metrics_used:
        return pd.DataFrame()

    base = metrics_df[metrics_used].to_numpy()
    n = base.shape[0]
    m = base.shape[1]
    trials = max(50, int(trials))
    sigma = max(0.001, float(sigma))
    rng = np.random.default_rng(seed)
    noise = rng.normal(0.0, sigma, size=(trials, n, m))
    samples = np.clip(base[None, :, :] + noise, 0.0, 1.0)
    w = np.array([weights[k] for k in metrics_used], dtype=float)
    total_w = w.sum()
    if total_w <= 0:
        return pd.DataFrame()
    scores = (samples * w[None, None, :]).sum(axis=2) / total_w

    order = np.argsort(-scores, axis=1)
    ranks = np.empty_like(order)
    rows = np.arange(trials)[:, None]
    ranks[rows, order] = np.arange(1, n + 1)
    p1 = (ranks == 1).mean(axis=0)
    p3 = (ranks <= 3).mean(axis=0)
    p5 = (ranks <= 5).mean(axis=0)
    avg_rank = ranks.mean(axis=0)
    out = pd.DataFrame({
        "N°": nums if nums is not None else "",
        "Cheval": names,
        "P(win)": p1,
        "P(top3)": p3,
        "P(top5)": p5,
        "Avg Rank": avg_rank,
    })
    return out.sort_values("P(win)", ascending=False)


def _mc_rank_from_trot_metrics(race_df: pd.DataFrame, composite_df: pd.DataFrame, weights: dict,
                               trials: int = 2000, sigma: float = 0.05, seed=None) -> pd.DataFrame:
    if race_df is None or race_df.empty or composite_df is None or composite_df.empty:
        return pd.DataFrame()
    horse_col = _find_col(composite_df, "Cheval", "CHEVAL")
    race_horse_col = _find_col(race_df, "CHEVAL", "Cheval")
    if not horse_col or not race_horse_col:
        return pd.DataFrame()
    names = composite_df[horse_col].astype(str).values
    num_col = _detect_horse_num_col(composite_df)
    nums = composite_df[num_col].astype(str).values if num_col else None

    def _series_by_name(df: pd.DataFrame, col: str, name_col: str) -> pd.Series:
        ser = pd.to_numeric(df[col], errors="coerce")
        ser.index = df[name_col].astype(str)
        return ser

    metrics_df = pd.DataFrame(index=names)
    for metric in weights.keys():
        norm_col = f"_norm_{metric}"
        if norm_col in race_df.columns:
            ser = _series_by_name(race_df, norm_col, race_horse_col)
            metrics_df[metric] = ser.reindex(metrics_df.index).fillna(0.5).values
            continue
        # normalize raw metric when needed
        raw_col = metric
        if raw_col in race_df.columns:
            ser = _series_by_name(race_df, raw_col, race_horse_col)
            if ser.dropna().empty or ser.max() == ser.min():
                metrics_df[metric] = 0.5
            else:
                vals = ser.copy()
                # invert where lower is better
                if metric in ("COTE", "REC", "REC.", "FA", "FM", "avg_rank", "recent_avg_rank", "DQ_Risk"):
                    vals = -vals
                if metric in ("trend_rank_slope",):
                    vals = -vals
                norm = (vals - vals.min()) / (vals.max() - vals.min())
                metrics_df[metric] = norm.reindex(metrics_df.index).fillna(0.5).values
            continue

    metrics_used = [m for m in weights.keys() if m in metrics_df.columns]
    if not metrics_used:
        return pd.DataFrame()

    base = metrics_df[metrics_used].to_numpy()
    n = base.shape[0]
    m = base.shape[1]
    trials = max(50, int(trials))
    sigma = max(0.001, float(sigma))
    rng = np.random.default_rng(seed)
    noise = rng.normal(0.0, sigma, size=(trials, n, m))
    samples = np.clip(base[None, :, :] + noise, 0.0, 1.0)
    w = np.array([weights[k] for k in metrics_used], dtype=float)
    total_w = w.sum()
    if total_w <= 0:
        return pd.DataFrame()
    scores = (samples * w[None, None, :]).sum(axis=2) / total_w

    order = np.argsort(-scores, axis=1)
    ranks = np.empty_like(order)
    rows = np.arange(trials)[:, None]
    ranks[rows, order] = np.arange(1, n + 1)
    p1 = (ranks == 1).mean(axis=0)
    p3 = (ranks <= 3).mean(axis=0)
    p5 = (ranks <= 5).mean(axis=0)
    avg_rank = ranks.mean(axis=0)
    out = pd.DataFrame({
        "N°": nums if nums is not None else "",
        "Cheval": names,
        "P(win)": p1,
        "P(top3)": p3,
        "P(top5)": p5,
        "Avg Rank": avg_rank,
    })
    return out.sort_values("P(win)", ascending=False)


def _softmax(x: np.ndarray, temp: float = 0.15) -> np.ndarray:
    t = max(1e-6, float(temp))
    z = x / t
    z = z - np.max(z)
    exp = np.exp(z)
    s = exp.sum()
    return exp / s if s > 0 else np.ones_like(exp) / len(exp)


def _mc_odds_vs_model(race_df: pd.DataFrame, composite_df: pd.DataFrame,
                      trials: int = 5000, alpha: float = 0.6, temp: float = 0.15, seed=None) -> pd.DataFrame:
    if race_df is None or race_df.empty or composite_df is None or composite_df.empty:
        return pd.DataFrame()
    horse_col = _find_col(composite_df, "Cheval", "CHEVAL")
    race_horse_col = _find_col(race_df, "CHEVAL", "Cheval")
    if not horse_col or not race_horse_col or "Composite" not in composite_df.columns:
        return pd.DataFrame()
    names = composite_df[horse_col].astype(str).values
    num_col = _detect_horse_num_col(composite_df)
    nums = composite_df[num_col].astype(str).values if num_col else None

    comp = pd.to_numeric(composite_df["Composite"], errors="coerce").fillna(0.5).values
    model_p = _softmax(comp, temp=temp)

    cote_col = _find_col(race_df, "COTE", "Cote")
    if not cote_col:
        return pd.DataFrame()
    cote_series = pd.to_numeric(race_df[cote_col], errors="coerce")
    cote_series.index = race_df[race_horse_col].astype(str)
    cote_series = cote_series.reindex(names)
    odds = cote_series.fillna(cote_series.median())
    odds = odds.replace(0, np.nan).fillna(odds.median() if not odds.dropna().empty else 10.0)
    odds_p = (1.0 / odds).values
    odds_p = odds_p / odds_p.sum() if odds_p.sum() > 0 else np.ones_like(odds_p) / len(odds_p)

    alpha = float(np.clip(alpha, 0.0, 1.0))
    mix_p = alpha * model_p + (1.0 - alpha) * odds_p
    mix_p = mix_p / mix_p.sum() if mix_p.sum() > 0 else np.ones_like(mix_p) / len(mix_p)

    trials = max(200, int(trials))
    rng = np.random.default_rng(seed)
    winners = rng.choice(len(names), size=trials, p=mix_p)
    win_counts = np.bincount(winners, minlength=len(names))
    win_p = win_counts / trials

    out = pd.DataFrame({
        "N°": nums if nums is not None else "",
        "Cheval": names,
        "COTE": odds.values,
        "P(model)": model_p,
        "P(odds)": odds_p,
        "P(win_sim)": win_p,
        "Delta(model-odds)": model_p - odds_p,
    })
    return out.sort_values("P(win_sim)", ascending=False)


def _mc_eval_bets(race_df: pd.DataFrame, composite_df: pd.DataFrame,
                  trials: int = 5000, alpha: float = 0.6, temp: float = 0.15,
                  bets_df=None, seed=None) -> pd.DataFrame:
    if race_df is None or race_df.empty or composite_df is None or composite_df.empty:
        return pd.DataFrame()
    horse_col = _find_col(composite_df, "Cheval", "CHEVAL")
    race_horse_col = _find_col(race_df, "CHEVAL", "Cheval")
    if not horse_col or not race_horse_col or "Composite" not in composite_df.columns:
        return pd.DataFrame()
    names = composite_df[horse_col].astype(str).values
    num_col = _detect_horse_num_col(composite_df)
    nums = composite_df[num_col].astype(str).values if num_col else None

    comp = pd.to_numeric(composite_df["Composite"], errors="coerce").fillna(0.5).values
    model_p = _softmax(comp, temp=temp)

    cote_col = _find_col(race_df, "COTE", "Cote")
    if not cote_col:
        return pd.DataFrame()
    cote_series = pd.to_numeric(race_df[cote_col], errors="coerce")
    cote_series.index = race_df[race_horse_col].astype(str)
    cote_series = cote_series.reindex(names)
    odds = cote_series.fillna(cote_series.median())
    odds = odds.replace(0, np.nan).fillna(odds.median() if not odds.dropna().empty else 10.0)
    odds_p = (1.0 / odds).values
    odds_p = odds_p / odds_p.sum() if odds_p.sum() > 0 else np.ones_like(odds_p) / len(odds_p)

    alpha = float(np.clip(alpha, 0.0, 1.0))
    mix_p = alpha * model_p + (1.0 - alpha) * odds_p
    mix_p = mix_p / mix_p.sum() if mix_p.sum() > 0 else np.ones_like(mix_p) / len(mix_p)

    trials = max(200, int(trials))
    rng = np.random.default_rng(seed)
    winners = rng.choice(len(names), size=trials, p=mix_p)

    # Build mapping for quick lookup
    name_to_idx = {str(n): i for i, n in enumerate(names)}
    num_to_idx = {str(n): i for i, n in enumerate(nums)} if nums is not None else {}

    def _parse_ticket(row) -> list:
        horses = []
        for v in row:
            if pd.isna(v):
                continue
            s = str(v).strip()
            if not s:
                continue
            if s in name_to_idx:
                horses.append(name_to_idx[s])
                continue
            if s in num_to_idx:
                horses.append(num_to_idx[s])
                continue
            # try extract numbers from strings
            m = re.findall(r"\d+", s)
            for token in m:
                if token in num_to_idx:
                    horses.append(num_to_idx[token])
        return sorted(set(horses))

    results = []
    if bets_df is not None and not bets_df.empty:
        for i, row in bets_df.iterrows():
            idxs = _parse_ticket(row.values.tolist())
            if not idxs:
                continue
            hit = np.isin(winners, idxs)
            payout = odds.values[winners]
            returns = np.where(hit, payout, 0.0)
            results.append({
                "Ticket": f"T{i+1}",
                "Size": len(idxs),
                "Hit%": hit.mean(),
                "Avg Payout": returns.mean(),
                "EV (Avg Payout - 1)": returns.mean() - 1.0,
            })

    if results:
        return pd.DataFrame(results).sort_values("EV (Avg Payout - 1)", ascending=False)
    return pd.DataFrame()


def _mc_scenario_analysis(race_df: pd.DataFrame, composite_df: pd.DataFrame, base_weights: dict,
                          scenarios: int = 1000, sigma: float = 0.1, corde_bias: float = 0.0, seed=None) -> pd.DataFrame:
    if race_df is None or race_df.empty or composite_df is None or composite_df.empty:
        return pd.DataFrame()
    horse_col = _find_col(composite_df, "Cheval", "CHEVAL")
    race_horse_col = _find_col(race_df, "CHEVAL", "Cheval")
    if not horse_col or not race_horse_col:
        return pd.DataFrame()

    flat_cfg = _weights_config("flat")
    flat_keys = list(flat_cfg.keys())
    weights = {k: float(base_weights.get(k, flat_cfg[k][1])) for k in flat_keys if k in flat_cfg}
    metrics = [k for k in flat_keys if f"_norm_{k}" in race_df.columns]
    if not metrics:
        return pd.DataFrame()

    names = composite_df[horse_col].astype(str).values
    num_col = _detect_horse_num_col(composite_df)
    nums = composite_df[num_col].astype(str).values if num_col else None

    mat = pd.DataFrame(index=names)
    for m in metrics:
        ser = pd.to_numeric(race_df[f"_norm_{m}"], errors="coerce")
        ser.index = race_df[race_horse_col].astype(str)
        mat[m] = ser.reindex(mat.index).fillna(0.5).values

    scenarios = max(200, int(scenarios))
    sigma = max(0.001, float(sigma))
    rng = np.random.default_rng(seed)
    base = np.array([weights[m] for m in metrics], dtype=float)
    base = np.clip(base, 0, None)
    if base.sum() == 0:
        base = np.ones_like(base)

    ranks_all = []
    for _ in range(scenarios):
        noise = rng.normal(0.0, sigma, size=len(metrics))
        w = np.clip(base + noise, 0, None)
        if "Corde" in metrics and corde_bias != 0:
            idx = metrics.index("Corde")
            w[idx] = max(0.0, w[idx] * (1.0 + corde_bias))
        if w.sum() == 0:
            w = np.ones_like(w)
        w = w / w.sum()
        scores = (mat.values * w).sum(axis=1)
        order = np.argsort(-scores)
        ranks = np.empty_like(order)
        ranks[order] = np.arange(1, len(order) + 1)
        ranks_all.append(ranks)

    ranks_all = np.vstack(ranks_all)
    p1 = (ranks_all == 1).mean(axis=0)
    p3 = (ranks_all <= 3).mean(axis=0)
    p5 = (ranks_all <= 5).mean(axis=0)
    avg_rank = ranks_all.mean(axis=0)
    out = pd.DataFrame({
        "N°": nums if nums is not None else "",
        "Cheval": names,
        "P(win)": p1,
        "P(top3)": p3,
        "P(top5)": p5,
        "Avg Rank": avg_rank,
    })
    return out.sort_values("P(win)", ascending=False)


def _mc_trot_scenario_analysis(race_df: pd.DataFrame, composite_df: pd.DataFrame, base_weights: dict,
                               scenarios: int = 1000, sigma: float = 0.1, dq_bias: float = 0.0, seed=None) -> pd.DataFrame:
    if race_df is None or race_df.empty or composite_df is None or composite_df.empty:
        return pd.DataFrame()
    horse_col = _find_col(composite_df, "Cheval", "CHEVAL")
    race_horse_col = _find_col(race_df, "CHEVAL", "Cheval")
    if not horse_col or not race_horse_col:
        return pd.DataFrame()

    defaults = {
        "S_COEFF": 0.20,
        "COTE": 0.15,
        "DQ_Risk": 0.20,
        "Fitness": 0.15,
        "Slope": 0.15,
        "REC": 0.10,
        "Shoeing_Agg": 0.05,
    }
    # Map DQ_Risk_Inverted -> DQ_Risk for compatibility with current_weights
    weights = {}
    for k, v in (base_weights or {}).items():
        if k == "DQ_Risk_Inverted":
            weights["DQ_Risk"] = v
        else:
            weights[k] = v
    for k, v in defaults.items():
        weights.setdefault(k, v)

    names = composite_df[horse_col].astype(str).values
    num_col = _detect_horse_num_col(composite_df)
    nums = composite_df[num_col].astype(str).values if num_col else None

    def _norm_series(series: pd.Series, invert: bool = False) -> pd.Series:
        s = pd.to_numeric(series, errors="coerce")
        if s.dropna().empty or s.max() == s.min():
            return pd.Series(0.5, index=s.index)
        vals = -s if invert else s
        return (vals - vals.min()) / (vals.max() - vals.min())

    mat = pd.DataFrame(index=names)
    # S_COEFF
    if "S_COEFF" in race_df.columns:
        ser = race_df["S_COEFF"]
        ser.index = race_df[race_horse_col].astype(str)
        mat["S_COEFF"] = _norm_series(ser).reindex(mat.index).fillna(0.5).values
    # COTE (lower is better)
    cote_col = _find_col(race_df, "COTE", "Cote")
    if cote_col:
        ser = race_df[cote_col]
        ser.index = race_df[race_horse_col].astype(str)
        mat["COTE"] = _norm_series(ser, invert=True).reindex(mat.index).fillna(0.5).values
    # DQ_Risk (lower is better)
    if "DQ_Risk" in race_df.columns:
        ser = race_df["DQ_Risk"]
        ser.index = race_df[race_horse_col].astype(str)
        mat["DQ_Risk"] = _norm_series(ser, invert=True).reindex(mat.index).fillna(0.5).values
    # Fitness from FA/FM (lower is better)
    fitness_col = "FA" if "FA" in race_df.columns else ("FM" if "FM" in race_df.columns else None)
    if fitness_col:
        ser = race_df[fitness_col]
        ser.index = race_df[race_horse_col].astype(str)
        mat["Fitness"] = _norm_series(ser, invert=True).reindex(mat.index).fillna(0.5).values
    # REC (lower is better)
    rec_col = "REC." if "REC." in race_df.columns else ("REC" if "REC" in race_df.columns else None)
    if rec_col:
        ser = race_df[rec_col]
        ser.index = race_df[race_horse_col].astype(str)
        mat["REC"] = _norm_series(ser, invert=True).reindex(mat.index).fillna(0.5).values
    # Slope (lower/negative is better)
    slope_col = "trend_rank_slope" if "trend_rank_slope" in race_df.columns else ("trend_slope" if "trend_slope" in race_df.columns else None)
    if slope_col:
        ser = race_df[slope_col]
        ser.index = race_df[race_horse_col].astype(str)
        mat["Slope"] = _norm_series(ser, invert=True).reindex(mat.index).fillna(0.5).values
    # Shoeing Aggressiveness (higher is better)
    shoe_col = "shoeing_aggressiveness" if "shoeing_aggressiveness" in race_df.columns else ("Shoeing_Agg" if "Shoeing_Agg" in race_df.columns else None)
    if shoe_col:
        ser = race_df[shoe_col]
        ser.index = race_df[race_horse_col].astype(str)
        mat["Shoeing_Agg"] = _norm_series(ser, invert=False).reindex(mat.index).fillna(0.5).values

    metrics = [m for m in weights.keys() if m in mat.columns]
    if not metrics:
        return pd.DataFrame()

    scenarios = max(200, int(scenarios))
    sigma = max(0.001, float(sigma))
    rng = np.random.default_rng(seed)
    base = np.array([weights[m] for m in metrics], dtype=float)
    base = np.clip(base, 0, None)
    if base.sum() == 0:
        base = np.ones_like(base)

    ranks_all = []
    for _ in range(scenarios):
        noise = rng.normal(0.0, sigma, size=len(metrics))
        w = np.clip(base + noise, 0, None)
        if "DQ_Risk" in metrics and dq_bias != 0:
            idx = metrics.index("DQ_Risk")
            w[idx] = max(0.0, w[idx] * (1.0 + dq_bias))
        if w.sum() == 0:
            w = np.ones_like(w)
        w = w / w.sum()
        scores = (mat[metrics].values * w).sum(axis=1)
        order = np.argsort(-scores)
        ranks = np.empty_like(order)
        ranks[order] = np.arange(1, len(order) + 1)
        ranks_all.append(ranks)

    ranks_all = np.vstack(ranks_all)
    p1 = (ranks_all == 1).mean(axis=0)
    p3 = (ranks_all <= 3).mean(axis=0)
    p5 = (ranks_all <= 5).mean(axis=0)
    avg_rank = ranks_all.mean(axis=0)
    out = pd.DataFrame({
        "NÂ°": nums if nums is not None else "",
        "Cheval": names,
        "P(win)": p1,
        "P(top3)": p3,
        "P(top5)": p5,
        "Avg Rank": avg_rank,
    })
    return out.sort_values("P(win)", ascending=False)


def _filter_history_for_trot(history_df: pd.DataFrame) -> pd.DataFrame:
    if history_df is None or history_df.empty:
        return history_df
    for col in ["RACE_TYPE", "RaceType", "TYPE", "Type", "DISCIPLINE", "Discipline"]:
        if col in history_df.columns:
            mask = history_df[col].astype(str).str.lower().str.contains(r"trot|attel|monté|monte", regex=True)
            if mask.any():
                return history_df[mask]
    return history_df


def _mc_trot_historical_similarity(history_df: pd.DataFrame, race_df: pd.DataFrame, composite_df: pd.DataFrame,
                                   trials: int = 5000, distance_window: int = 200, seed=None):
    hist = _filter_history_for_trot(history_df)
    return _mc_historical_similarity(hist, race_df, composite_df, trials=trials, distance_window=distance_window, seed=seed)


def _mc_historical_similarity(history_df: pd.DataFrame, race_df: pd.DataFrame, composite_df: pd.DataFrame,
                              trials: int = 5000, distance_window: int = 200, seed=None):
    if history_df is None or history_df.empty or race_df is None or race_df.empty:
        return pd.DataFrame(), pd.DataFrame()
    horse_col = _find_col(composite_df, "Cheval", "CHEVAL")
    race_horse_col = _find_col(race_df, "CHEVAL", "Cheval")
    if not horse_col or not race_horse_col:
        return pd.DataFrame(), pd.DataFrame()

    hippo, dist = _get_race_meta(race_df)
    if hippo is None or dist is None:
        return pd.DataFrame(), pd.DataFrame()

    hist = history_df.copy()
    hippo_col = _find_col(hist, "HIPPODROME", "Hippodrome", "HIPPO", "Lieu", "LIEU")
    dist_col = _find_col(hist, "DISTANCE", "Distance", "DIST")
    rang_col = _find_col(hist, "RANG", "Rang", "RANG_ARRIVEE", "ARRIVEE")
    cote_col = _find_col(hist, "COTE", "Cote")
    if not hippo_col or not dist_col or not rang_col or not cote_col:
        return pd.DataFrame(), pd.DataFrame()

    try:
        hippo_norm = str(hippo).strip().title()
        hist = hist[hist[hippo_col].astype(str).str.strip().str.title() == hippo_norm]
    except Exception:
        pass

    dist_vals = pd.to_numeric(hist[dist_col], errors="coerce")
    try:
        dist_val = float(dist)
    except Exception:
        dist_val = None
    if dist_val is not None:
        hist = hist[(dist_vals - dist_val).abs() <= float(distance_window)]

    hist = hist[[rang_col, cote_col]].copy()
    hist["rang"] = pd.to_numeric(hist[rang_col], errors="coerce")
    hist["cote"] = pd.to_numeric(hist[cote_col], errors="coerce")
    hist = hist.dropna(subset=["rang", "cote"])
    if hist.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Build odds bins
    try:
        hist["bin"] = pd.qcut(hist["cote"], q=5, duplicates="drop")
    except Exception:
        bins = [0, 5, 10, 20, 50, np.inf]
        hist["bin"] = pd.cut(hist["cote"], bins=bins, include_lowest=True)

    bin_stats = hist.groupby("bin").agg(
        count=("rang", "size"),
        win_rate=("rang", lambda s: (s == 1).mean()),
        top3_rate=("rang", lambda s: (s <= 3).mean()),
        avg_cote=("cote", "mean"),
    ).reset_index()

    # Assign probabilities to current horses
    names = composite_df[horse_col].astype(str).values
    num_col = _detect_horse_num_col(composite_df)
    nums = composite_df[num_col].astype(str).values if num_col else None

    cote_series = pd.to_numeric(race_df[_find_col(race_df, "COTE", "Cote")], errors="coerce")
    cote_series.index = race_df[race_horse_col].astype(str)
    cote_series = cote_series.reindex(names)
    cote_series = cote_series.fillna(cote_series.median())

    # Map to bins
    if "bin" in hist.columns and not hist["bin"].empty:
        try:
            bins = hist["bin"].cat.categories
            horse_bins = pd.cut(cote_series, bins=bins, include_lowest=True)
        except Exception:
            horse_bins = None
    else:
        horse_bins = None

    overall_win = (hist["rang"] == 1).mean()
    win_rates = {}
    if not bin_stats.empty:
        for _, row in bin_stats.iterrows():
            win_rates[str(row["bin"])] = float(row["win_rate"])

    p_win_hist = []
    for i, cote in cote_series.items():
        if horse_bins is not None:
            b = horse_bins.loc[i]
            if pd.notna(b) and str(b) in win_rates:
                p_win_hist.append(win_rates[str(b)])
                continue
        p_win_hist.append(overall_win)

    p_win_hist = np.array(p_win_hist, dtype=float)
    if p_win_hist.sum() <= 0:
        p_win_hist = np.ones_like(p_win_hist) / len(p_win_hist)
    else:
        p_win_hist = p_win_hist / p_win_hist.sum()

    trials = max(200, int(trials))
    rng = np.random.default_rng(seed)
    winners = rng.choice(len(names), size=trials, p=p_win_hist)
    win_counts = np.bincount(winners, minlength=len(names))
    win_sim = win_counts / trials

    out = pd.DataFrame({
        "N°": nums if nums is not None else "",
        "Cheval": names,
        "COTE": cote_series.values,
        "P(win_hist)": p_win_hist,
        "P(win_sim)": win_sim,
    })
    return out.sort_values("P(win_sim)", ascending=False), bin_stats


def _recompute_all_mc_flat(race_df: pd.DataFrame, composite_df: pd.DataFrame, weights: dict, history_df: pd.DataFrame):
    if race_df is None or race_df.empty or composite_df is None or composite_df.empty:
        return
    # Read current UI params if available, otherwise defaults from UI
    trials_1 = int(st.session_state.get("mc_comp_trials", 3000))
    sigma_1 = float(st.session_state.get("mc_comp_sigma", 0.06))
    seed_1 = st.session_state.get("mc_comp_seed", 0)
    trials_2 = int(st.session_state.get("mc_metric_trials", 3000))
    sigma_2 = float(st.session_state.get("mc_metric_sigma", 0.05))
    seed_2 = st.session_state.get("mc_metric_seed", 0)
    trials_3 = int(st.session_state.get("mc_odds_trials", 8000))
    alpha_3 = float(st.session_state.get("mc_odds_alpha", 0.6))
    temp_3 = float(st.session_state.get("mc_odds_temp", 0.15))
    seed_3 = st.session_state.get("mc_odds_seed", 0)
    trials_4 = int(st.session_state.get("mc_bet_trials", 8000))
    alpha_4 = float(st.session_state.get("mc_bet_alpha", 0.6))
    temp_4 = float(st.session_state.get("mc_bet_temp", 0.15))
    seed_4 = st.session_state.get("mc_bet_seed", 0)
    scenarios_5 = int(st.session_state.get("mc_scenarios", 2000))
    sigma_5 = float(st.session_state.get("mc_scen_sigma", 0.10))
    corde_bias = float(st.session_state.get("mc_scen_corde", 0.0))
    seed_5 = st.session_state.get("mc_scen_seed", 0)
    trials_6 = int(st.session_state.get("mc_hist_trials", 8000))
    dist_window = int(st.session_state.get("mc_hist_dist", 200))
    seed_6 = st.session_state.get("mc_hist_seed", 0)

    # Method 1
    st.session_state.mc_comp_res = _mc_rank_from_composite(
        composite_df, trials=trials_1, sigma=sigma_1, seed=None if seed_1 == 0 else int(seed_1)
    )
    # Method 2 (ensure flat metrics)
    flat_cfg = _weights_config("flat")
    flat_keys = set(flat_cfg.keys())
    base_weights = {k: v for k, v in (weights or {}).items() if k in flat_keys}
    required_flat = ["IF", "IC", "n_weight", "Corde", "VALEUR"]
    for k in required_flat:
        if k not in base_weights:
            base_weights[k] = flat_cfg[k][1]
    st.session_state.mc_metric_res = _mc_rank_from_metrics(
        race_df,
        composite_df,
        weights=base_weights,
        trials=trials_2,
        sigma=sigma_2,
        seed=None if seed_2 == 0 else int(seed_2),
    )
    # Method 3
    st.session_state.mc_odds_res = _mc_odds_vs_model(
        race_df,
        composite_df,
        trials=trials_3,
        alpha=alpha_3,
        temp=temp_3,
        seed=None if seed_3 == 0 else int(seed_3),
    )
    # Method 4
    bets_df = None
    use_generated = bool(st.session_state.get("mc_bet_use_topk", True))
    max_k = int(st.session_state.get("mc_bet_maxk", 5))
    if use_generated:
        base = st.session_state.mc_odds_res
        if isinstance(base, pd.DataFrame) and not base.empty and "N°" in base.columns:
            tickets = []
            nums = base["N°"].astype(str).tolist()
            for k in range(2, max_k + 1):
                tickets.append(nums[:k])
            if tickets:
                max_len = max(len(r) for r in tickets)
                cols = [f"H{i+1}" for i in range(max_len)]
                padded = [r + [""] * (max_len - len(r)) for r in tickets]
                bets_df = pd.DataFrame(padded, columns=cols)
    else:
        if isinstance(st.session_state.last_bets_df, pd.DataFrame):
            bets_df = st.session_state.last_bets_df
    st.session_state.mc_bet_res = _mc_eval_bets(
        race_df,
        composite_df,
        trials=trials_4,
        alpha=alpha_4,
        temp=temp_4,
        bets_df=bets_df,
        seed=None if seed_4 == 0 else int(seed_4),
    )
    # Method 5
    st.session_state.mc_scen_res = _mc_scenario_analysis(
        race_df,
        composite_df,
        base_weights=weights or {},
        scenarios=scenarios_5,
        sigma=sigma_5,
        corde_bias=corde_bias,
        seed=None if seed_5 == 0 else int(seed_5),
    )
    # Method 6
    res_6, bins_6 = _mc_historical_similarity(
        history_df if isinstance(history_df, pd.DataFrame) else pd.DataFrame(),
        race_df,
        composite_df,
        trials=trials_6,
        distance_window=dist_window,
        seed=None if seed_6 == 0 else int(seed_6),
    )
    st.session_state.mc_hist_res = res_6
    st.session_state.mc_hist_bins = bins_6


def _recompute_all_mc_trot(race_df: pd.DataFrame, composite_df: pd.DataFrame, history_df: pd.DataFrame):
    if race_df is None or race_df.empty or composite_df is None or composite_df.empty:
        return
    # T1
    trials_t1 = int(st.session_state.get("mc_trot_comp_trials", 3000))
    sigma_t1 = float(st.session_state.get("mc_trot_comp_sigma", 0.06))
    seed_t1 = st.session_state.get("mc_trot_comp_seed", 0)
    st.session_state.mc_trot_comp_res = _mc_rank_from_composite(
        composite_df,
        trials=trials_t1,
        sigma=sigma_t1,
        seed=None if seed_t1 == 0 else int(seed_t1),
    )

    # T2
    trials_t2 = int(st.session_state.get("mc_trot_metric_trials", 3000))
    sigma_t2 = float(st.session_state.get("mc_trot_metric_sigma", 0.05))
    seed_t2 = st.session_state.get("mc_trot_metric_seed", 0)
    w_scoeff = float(st.session_state.get("mc_trot_w_scoeff", 20))
    w_cote = float(st.session_state.get("mc_trot_w_cote", 15))
    w_dq = float(st.session_state.get("mc_trot_w_dq", 20))
    w_fit = float(st.session_state.get("mc_trot_w_fit", 15))
    w_slope = float(st.session_state.get("mc_trot_w_slope", 15))
    w_rec = float(st.session_state.get("mc_trot_w_rec", 10))
    w_shoe = float(st.session_state.get("mc_trot_w_shoe", 5))
    total_w = w_scoeff + w_cote + w_dq + w_fit + w_slope + w_rec + w_shoe
    if total_w <= 0:
        total_w = 1.0
    trot_weights = {
        "S_COEFF": w_scoeff / total_w,
        "COTE": w_cote / total_w,
        "DQ_Risk": w_dq / total_w,
        "Fitness": w_fit / total_w,
        "Slope": w_slope / total_w,
        "REC": w_rec / total_w,
        "Shoeing_Agg": w_shoe / total_w,
    }
    st.session_state.mc_trot_metric_res = _mc_rank_from_trot_metrics(
        race_df,
        composite_df,
        weights=trot_weights,
        trials=trials_t2,
        sigma=sigma_t2,
        seed=None if seed_t2 == 0 else int(seed_t2),
    )

    # T3
    trials_t3 = int(st.session_state.get("mc_trot_odds_trials", 8000))
    alpha_t3 = float(st.session_state.get("mc_trot_odds_alpha", 0.6))
    temp_t3 = float(st.session_state.get("mc_trot_odds_temp", 0.15))
    seed_t3 = st.session_state.get("mc_trot_odds_seed", 0)
    st.session_state.mc_trot_odds_res = _mc_odds_vs_model(
        race_df,
        composite_df,
        trials=trials_t3,
        alpha=alpha_t3,
        temp=temp_t3,
        seed=None if seed_t3 == 0 else int(seed_t3),
    )

    # T4
    trials_t4 = int(st.session_state.get("mc_trot_bet_trials", 8000))
    alpha_t4 = float(st.session_state.get("mc_trot_bet_alpha", 0.6))
    temp_t4 = float(st.session_state.get("mc_trot_bet_temp", 0.15))
    seed_t4 = st.session_state.get("mc_trot_bet_seed", 0)
    use_generated_t4 = bool(st.session_state.get("mc_trot_bet_use_topk", True))
    max_k_t4 = int(st.session_state.get("mc_trot_bet_maxk", 5))
    bets_df = None
    if use_generated_t4:
        base = st.session_state.get("mc_trot_odds_res")
        if isinstance(base, pd.DataFrame) and not base.empty and "NÂ°" in base.columns:
            tickets = []
            nums = base["NÂ°"].astype(str).tolist()
            for k in range(2, max_k_t4 + 1):
                tickets.append(nums[:k])
            if tickets:
                max_len = max(len(r) for r in tickets)
                cols = [f"H{i+1}" for i in range(max_len)]
                padded = [r + [""] * (max_len - len(r)) for r in tickets]
                bets_df = pd.DataFrame(padded, columns=cols)
    else:
        if isinstance(st.session_state.last_bets_df, pd.DataFrame):
            bets_df = st.session_state.last_bets_df
    st.session_state.mc_trot_bet_res = _mc_eval_bets(
        race_df,
        composite_df,
        trials=trials_t4,
        alpha=alpha_t4,
        temp=temp_t4,
        bets_df=bets_df,
        seed=None if seed_t4 == 0 else int(seed_t4),
    )

    # T5
    scenarios_t5 = int(st.session_state.get("mc_trot_scenarios", 2000))
    sigma_t5 = float(st.session_state.get("mc_trot_scen_sigma", 0.10))
    dq_bias_t5 = float(st.session_state.get("mc_trot_scen_dq_bias", 0.0))
    seed_t5 = st.session_state.get("mc_trot_scen_seed", 0)
    st.session_state.mc_trot_scen_res = _mc_trot_scenario_analysis(
        race_df,
        composite_df,
        base_weights=st.session_state.current_weights or {},
        scenarios=scenarios_t5,
        sigma=sigma_t5,
        dq_bias=dq_bias_t5,
        seed=None if seed_t5 == 0 else int(seed_t5),
    )

    # T6
    trials_t6 = int(st.session_state.get("mc_trot_hist_trials", 8000))
    dist_window_t6 = int(st.session_state.get("mc_trot_hist_dist", 200))
    seed_t6 = st.session_state.get("mc_trot_hist_seed", 0)
    res_t6, bins_t6 = _mc_trot_historical_similarity(
        history_df if isinstance(history_df, pd.DataFrame) else pd.DataFrame(),
        race_df,
        composite_df,
        trials=trials_t6,
        distance_window=dist_window_t6,
        seed=None if seed_t6 == 0 else int(seed_t6),
    )
    st.session_state.mc_trot_hist_res = res_t6
    st.session_state.mc_trot_hist_bins = bins_t6


@st.cache_data(show_spinner=False)
def _load_flat_history(db_path: str = "zone_turf_flat.db") -> pd.DataFrame:
    if not os.path.exists(db_path):
        return pd.DataFrame()
    con = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query("SELECT * FROM plat", con)
    finally:
        con.close()
    if 'HIPPODROME' in df.columns:
        df['HIPPODROME'] = df['HIPPODROME'].astype(str).str.strip().str.title()
    return df


def _extract_distance_m(df: pd.DataFrame) -> pd.Series:
    for col in ("DISTANCE", "DIST", "Distance", "distance"):
        if col in df.columns:
            return _parse_numeric_series(df[col])
    return pd.Series(dtype=float)


def _build_flat_analysis_payload(race_df: pd.DataFrame, composite_df: pd.DataFrame):
    if race_df is None or race_df.empty or composite_df is None or composite_df.empty:
        return None
    prog = compute_prognosis(race_df, max_len=8)
    # Sort prognosis by composite score when possible
    try:
        if prog and 'Composite' in composite_df.columns:
            num_col = _detect_horse_num_col(composite_df)
            if num_col:
                score_map = {}
                for _, row in composite_df.iterrows():
                    try:
                        n = str(row.get(num_col, '')).strip()
                        if n.endswith('.0'):
                            n = n[:-2]
                        if n and n != '0' and n.lower() not in ('nan', 'none'):
                            score_map[n] = float(row['Composite'])
                    except Exception:
                        pass
                prog = sorted(prog, key=lambda x: score_map.get(x, -1), reverse=True)
    except Exception:
        pass
    prog = sanitize_horse_list(prog)

    summary = compute_summary_horses(composite_df)
    summary = sanitize_horse_list(summary)
    exclusive = [h for h in summary if h not in prog]
    prognosis_only = [h for h in prog if h not in summary]
    summary_prognosis_intersection = [h for h in prog if h in summary]

    trending_list = compute_trending_horses(race_df, top_n=3)
    hippodrome, distance = _get_race_meta(race_df)
    favorable_list = []
    temp_df = race_df.copy()
    if "NÂ°" not in temp_df.columns:
        num_col = _detect_horse_num_col(temp_df)
        if num_col:
            temp_df = temp_df.rename(columns={num_col: "NÂ°"})
    if hippodrome and distance and "CORDE" in temp_df.columns:
        favorable_list = compute_favorable_corde_horses(temp_df, hippodrome, distance, top_n=3)
    return {
        "race_df": race_df,
        "composite_df": composite_df,
        "prognosis_text": "  ".join(prog),
        "summary_text": "  ".join(summary),
        "exclusive_text": "  ".join(exclusive),
        "prognosis_only_text": "  ".join(prognosis_only),
        "summary_prognosis_text": "  ".join(summary_prognosis_intersection),
        "trending_text": "  ".join(trending_list[:3]) if trending_list else "",
        "favorable_cordes_text": "  ".join(favorable_list),
    }


def _analysis_pdf_bytes_flat(race_df: pd.DataFrame, composite_df: pd.DataFrame,
                             prognosis_text: str, summary_text: str,
                             summary_prognosis_text: str, favorable_cordes_text: str,
                             prognosis_only_text: str, exclusive_text: str,
                             trending_text: str) -> bytes:
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
        from reportlab.lib import colors
        from reportlab.lib.enums import TA_CENTER
    except Exception as e:
        raise ImportError(f"reportlab not available: {e}")

    prize_name = str(race_df['PRIZE_NAME'].iloc[0]) if 'PRIZE_NAME' in race_df.columns else ''
    ref_course = str(race_df['REF_COURSE'].iloc[0]) if 'REF_COURSE' in race_df.columns else ''
    race_date = str(race_df['RACE_DATE'].iloc[0]) if 'RACE_DATE' in race_df.columns else ''
    hippodrome = str(race_df['HIPPODROME'].iloc[0]) if 'HIPPODROME' in race_df.columns else ''
    ext_conditions = str(race_df['DESCRIPTIF'].iloc[0]) if 'DESCRIPTIF' in race_df.columns else ''

    header_text_racex, footer_text_racex, header_style, footer_style, icon_path = get_pdf_header_footer()

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, rightMargin=36, leftMargin=36, topMargin=60, bottomMargin=30)
    story = []
    styles = getSampleStyleSheet()

    if icon_path:
        try:
            img = Image(icon_path, width=0.5*inch, height=0.5*inch)
            story.append(img)
        except Exception:
            pass
    story.append(Paragraph(header_text_racex, header_style))
    story.append(Spacer(1, 6))

    title_style = ParagraphStyle('CustomTitle', parent=styles['Heading1'], fontSize=14, spaceAfter=12, alignment=TA_CENTER)
    header_style_custom = ParagraphStyle('CustomHeader', parent=styles['Heading2'], fontSize=10, spaceAfter=10, textColor=colors.darkblue)
    italic_style = ParagraphStyle('CustomItalic', parent=styles['Normal'], fontSize=9, fontName='Helvetica-Oblique', spaceAfter=10, textColor=colors.grey)
    section_style = ParagraphStyle('SectionStyle', parent=styles['Normal'], fontSize=9, spaceAfter=4)

    header_text = f"{prize_name}" if prize_name else "Analyse de Course"
    story.append(Paragraph(header_text, title_style))

    metadata_text = f"<b>Reference:</b> {ref_course} | <b>Date:</b> {race_date} | <b>Hippodrome:</b> {hippodrome}"
    story.append(Paragraph(metadata_text, styles['Normal']))
    story.append(Spacer(1, 6))

    if ext_conditions:
        story.append(Paragraph(f"<i>{ext_conditions}</i>", italic_style))
        story.append(Spacer(1, 8))

    if composite_df is not None and not composite_df.empty:
        story.append(Paragraph("Classement des Chevaux (Score Composite)", header_style_custom))
        df_sorted = composite_df.sort_values('Composite', ascending=False)
        num_col = _detect_horse_num_col(df_sorted) or "N°"
        table_data = [['Pos.', 'N°', 'Cheval', 'COTE', 'Score Composite']]
        for i, (_, row) in enumerate(df_sorted.iterrows(), 1):
            cote_val = row.get('COTE', row.get('Cote', ''))
            table_data.append([
                str(i),
                str(row.get(num_col, '')),
                str(row.get('Cheval', row.get('CHEVAL', ''))),
                str(cote_val),
                f"{row.get('Composite', 0):.2f}"
            ])
        table = Table(table_data, colWidths=[0.6*inch, 0.6*inch, 2.0*inch, 0.8*inch, 0.9*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.lightgrey])
        ]))
        story.append(table)
        story.append(Spacer(1, 12))

    story.append(Paragraph("Analyse", header_style_custom))

    sections = [
        ("Notre Prono Flash", prognosis_text),
        ("Synthese (Composite)", summary_text),
        ("[TARGET] Intersection Synthese+Prono Flash", summary_prognosis_text),
        ("Cordes Favorables", favorable_cordes_text),
        ("Dans le Prono Uniquement", prognosis_only_text),
        ("Exclusif (Synthese seulement)", exclusive_text),
        ("Populaires (Baisse de Cotes)", trending_text),
    ]
    for section_title, section_text in sections:
        if section_text:
            story.append(Paragraph(f"<b>{section_title}:</b> {section_text}", section_style))
        else:
            story.append(Paragraph(f"<b>{section_title}:</b> <i>(aucun resultat)</i>", section_style))
        story.append(Spacer(1, 2))

    story.append(Spacer(1, 24))
    story.append(Paragraph(footer_text_racex, footer_style))

    doc.build(story)
    buf.seek(0)
    return buf.getvalue()


def _sorted_prognosis_display(race_df: pd.DataFrame, composite_df: pd.DataFrame):
    prog = compute_prognosis(race_df, max_len=8)
    try:
        if prog and composite_df is not None and not composite_df.empty and 'Composite' in composite_df.columns:
            num_col = _detect_horse_num_col(composite_df)
            if num_col:
                score_map = {}
                for _, row in composite_df.iterrows():
                    try:
                        n = str(row.get(num_col, '')).strip()
                        if n.endswith('.0'):
                            n = n[:-2]
                        if n and n != '0' and n.lower() not in ('nan', 'none'):
                            score_map[n] = float(row['Composite'])
                    except Exception:
                        pass
                prog = sorted(prog, key=lambda x: score_map.get(x, -1), reverse=True)
    except Exception:
        pass
    return sanitize_horse_list(prog)


def _qdate_from_pydate(d: date):
    """Wrap a Python date into a minimal QDate-like object for scrape_meeting_urls."""
    class _QDate:
        def __init__(self, d):
            self._d = d
        def isValid(self): return True
        def day(self): return self._d.day
        def month(self): return self._d.month
        def year(self): return self._d.year
        def dayOfWeek(self): return self._d.isoweekday()  # Mon=1 … Sun=7
    return _QDate(d)


# ── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🏇 RaceX")
    st.markdown("---")
    st.subheader("Pages")
    st.page_link("app.py", label="Race Analysis", icon="🏇")
    st.page_link("pages/1_History_Analysis.py", label="History & Analysis", icon="📊")
    st.page_link("pages/3_Dictionary.py", label="Dictionary", icon="📘")
    st.page_link("pages/4_Insights_PMU.py", label="Insights & PMU", icon="🧠")
    st.page_link("pages/2_About.py", label="About", icon="ℹ️")

    # Date picker
    selected_date = st.date_input("Race Date", value=date.today())
    qdate = _qdate_from_pydate(selected_date)

    # Load meetings
    if st.button("🔄 Load Meetings", use_container_width=True):
        with st.spinner("Fetching meetings…"):
            meetings = scrape_meeting_urls(qdate)
        if meetings:
            st.session_state.meetings = meetings
            st.success(f"Found {len(meetings)} meetings")
        else:
            st.warning("No meetings found for this date.")

    meetings = st.session_state.meetings
    if meetings:
        selected_label = st.selectbox("Select Meeting", list(meetings.keys()))
        selected_url = meetings[selected_label]
    else:
        selected_label = ""
        selected_url = st.text_input("Or paste URL directly", placeholder="https://zone-turf.fr/…")

    race_type = st.session_state.race_type
    detected_banner = st.session_state.get("race_type_detected")
    if detected_banner in ("trot", "flat"):
        st.info(f"Auto-detected: {detected_banner}")
    else:
        st.caption("Race type will be auto-detected at scrape time.")

    st.markdown("---")
    with st.expander("Metric Weights", expanded=False):
        st.session_state.current_weights = _render_weights_ui(race_type)

    with st.expander("Exports", expanded=False):
        if not st.session_state.race_df.empty:
            st.download_button(
                "Download Raw Data (CSV)",
                data=_df_to_csv_bytes(st.session_state.race_df),
                file_name="race_raw_data.csv",
                mime="text/csv",
            )
        if not st.session_state.filtered_df.empty:
            st.download_button(
                "Download Filtered Race (CSV)",
                data=_df_to_csv_bytes(st.session_state.filtered_df),
                file_name="race_filtered_data.csv",
                mime="text/csv",
            )
        if not st.session_state.composite_df.empty:
            st.download_button(
                "Download Composite (CSV)",
                data=_df_to_csv_bytes(st.session_state.composite_df),
                file_name="race_composite.csv",
                mime="text/csv",
            )
        if isinstance(st.session_state.last_summary_df, pd.DataFrame) and not st.session_state.last_summary_df.empty:
            st.download_button(
                "Download Summary (CSV)",
                data=_df_to_csv_bytes(st.session_state.last_summary_df),
                file_name="race_summary.csv",
                mime="text/csv",
            )
        if isinstance(st.session_state.last_bets_df, pd.DataFrame) and not st.session_state.last_bets_df.empty:
            bets_name = st.session_state.last_bets_label or "race_bets"
            st.download_button(
                "Download Bets (CSV)",
                data=_df_to_csv_bytes(st.session_state.last_bets_df),
                file_name=f"{bets_name}.csv",
                mime="text/csv",
            )
        if isinstance(st.session_state.flat_analysis, dict):
            fa = st.session_state.flat_analysis
            ref_course = str(fa["race_df"]['REF_COURSE'].iloc[0]) if 'REF_COURSE' in fa["race_df"].columns else 'race'
            race_date = str(fa["race_df"]['RACE_DATE'].iloc[0]) if 'RACE_DATE' in fa["race_df"].columns else ''
            date_clean = race_date.replace('/', '').replace('-', '') if race_date else ''
            filename = f"{date_clean}_{ref_course}_analysis.pdf" if date_clean else f"{ref_course}_analysis.pdf"

            if st.button("Prepare Flat Analysis PDF", key="prep_flat_pdf"):
                try:
                    st.session_state.prepared_flat_pdf = _analysis_pdf_bytes_flat(
                        fa["race_df"],
                        fa["composite_df"],
                        fa["prognosis_text"],
                        fa["summary_text"],
                        fa["summary_prognosis_text"],
                        fa["favorable_cordes_text"],
                        fa["prognosis_only_text"],
                        fa["exclusive_text"],
                        fa["trending_text"],
                    )
                    st.session_state.prepared_flat_pdf_name = filename
                except ImportError:
                    st.session_state.prepared_flat_pdf = None
                    st.error("ReportLab is required for PDF export. Install it with `pip install reportlab`.")
                except Exception as e:
                    st.session_state.prepared_flat_pdf = None
                    st.error(f"Flat analysis PDF export failed: {e}")

            if st.session_state.get("prepared_flat_pdf"):
                st.download_button(
                    "Download Flat Analysis PDF",
                    data=st.session_state.prepared_flat_pdf,
                    file_name=st.session_state.get("prepared_flat_pdf_name", filename),
                    mime="application/pdf",
                )

    st.markdown("---")
    if st.button("🚀 Scrape & Analyse", use_container_width=True, type="primary"):
        if not selected_url:
            st.error("Please select a meeting or enter a URL.")
        else:
            detected = _auto_detect_race_type(selected_url)
            if detected in ("trot", "flat"):
                st.session_state.race_type_detected = detected
                st.session_state.race_type = detected
                race_type = detected
            with st.spinner("Scraping race data…"):
                try:
                    df = data_source_manager.scrape_races(selected_url, race_type)
                    if df is not None and not df.empty:
                        df = _clean_scraped_data(df, race_type)
                        if race_type == "flat":
                            try:
                                df = normalize_composite_columns(df)
                            except Exception:
                                pass
                        st.session_state.race_df = df
                        st.session_state.race_type = race_type
                        # Auto-select first race for display
                        if "REF_COURSE" in df.columns and not df["REF_COURSE"].dropna().empty:
                            ref_series = df["REF_COURSE"].astype(str).str.strip()
                            ref_courses = sorted(ref_series.dropna().unique().tolist(), key=str)
                            st.session_state.selected_ref_course = ref_courses[0] if ref_courses else None
                            if st.session_state.selected_ref_course:
                                filtered = df[ref_series == str(st.session_state.selected_ref_course)].copy()
                                st.session_state.filtered_df = filtered
                                comp = compute_composite_score(filtered, weights=st.session_state.current_weights)
                                st.session_state.composite_df = comp if comp is not None else pd.DataFrame()
                        else:
                            st.session_state.selected_ref_course = None
                            st.session_state.filtered_df = df.copy()
                            comp = compute_composite_score(df, weights=st.session_state.current_weights)
                            st.session_state.composite_df = comp if comp is not None else pd.DataFrame()
                        st.success(f"Loaded {len(df)} horses across {df['REF_COURSE'].nunique() if 'REF_COURSE' in df.columns else 1} race(s)")
                    else:
                        st.error("No data returned. Check the URL or race type.")
                except Exception as e:
                    st.error(f"Scraping failed: {e}")

    # ── REF_COURSE filter (shown only when data is loaded) ───────────────────
    full_df: pd.DataFrame = st.session_state.race_df
    if not full_df.empty and "REF_COURSE" in full_df.columns:
        st.markdown("---")
        st.markdown("**Filter by Race**")
        ref_series = full_df["REF_COURSE"].astype(str).str.strip()
        ref_courses = sorted(ref_series.dropna().unique().tolist(), key=str)
        # Default to previously selected value, or first in list
        default_idx = 0
        if st.session_state.selected_ref_course in ref_courses:
            default_idx = ref_courses.index(st.session_state.selected_ref_course)
        chosen_id = st.selectbox(
            "REF_COURSE",
            ref_courses,
            index=default_idx,
            format_func=str,
        )
        if chosen_id != st.session_state.selected_ref_course:
            st.session_state.selected_ref_course = chosen_id

        filtered = full_df[ref_series == str(st.session_state.selected_ref_course)].copy()
        composite = compute_composite_score(filtered, weights=st.session_state.current_weights)
        st.session_state.filtered_df = filtered
        st.session_state.composite_df = composite if composite is not None else pd.DataFrame()
        st.caption(f"{len(filtered)} horses in {st.session_state.selected_ref_course}")
        if st.session_state.race_type == "flat":
            st.session_state.flat_analysis = _build_flat_analysis_payload(
                st.session_state.filtered_df,
                st.session_state.composite_df,
            )
            mc_key = f"flat:{st.session_state.selected_ref_course}:{_weights_signature_flat()}"
            if st.session_state.get("mc_last_key") != mc_key:
                hist = st.session_state.get("history_df")
                if not isinstance(hist, pd.DataFrame):
                    hist = _load_flat_history()
                    st.session_state.history_df = hist
                _recompute_all_mc_flat(
                    st.session_state.filtered_df,
                    st.session_state.composite_df,
                    st.session_state.current_weights or {},
                    hist,
                )
                st.session_state.mc_last_key = mc_key
        if st.session_state.race_type == "trot":
            mc_key = f"trot:{st.session_state.selected_ref_course}:{_weights_signature_trot()}"
            if st.session_state.get("mc_last_key") != mc_key:
                hist = st.session_state.get("history_df")
                if not isinstance(hist, pd.DataFrame):
                    hist = _load_flat_history()
                    st.session_state.history_df = hist
                _recompute_all_mc_trot(
                    st.session_state.filtered_df,
                    st.session_state.composite_df,
                    hist,
                )
                st.session_state.mc_last_key = mc_key
    elif not full_df.empty:
        # No REF_COURSE column — treat the whole df as one race
        if st.session_state.filtered_df.empty:
            composite = compute_composite_score(full_df, weights=st.session_state.current_weights)
            st.session_state.filtered_df = full_df.copy()
            st.session_state.composite_df = composite if composite is not None else pd.DataFrame()
            if st.session_state.race_type == "flat":
                st.session_state.flat_analysis = _build_flat_analysis_payload(
                    st.session_state.filtered_df,
                    st.session_state.composite_df,
                )
                mc_key = f"flat:all:{_weights_signature_flat()}"
                if st.session_state.get("mc_last_key") != mc_key:
                    hist = st.session_state.get("history_df")
                    if not isinstance(hist, pd.DataFrame):
                        hist = _load_flat_history()
                        st.session_state.history_df = hist
                    _recompute_all_mc_flat(
                        st.session_state.filtered_df,
                        st.session_state.composite_df,
                        st.session_state.current_weights or {},
                        hist,
                    )
                    st.session_state.mc_last_key = mc_key
            if st.session_state.race_type == "trot":
                mc_key = f"trot:all:{_weights_signature_trot()}"
                if st.session_state.get("mc_last_key") != mc_key:
                    hist = st.session_state.get("history_df")
                    if not isinstance(hist, pd.DataFrame):
                        hist = _load_flat_history()
                        st.session_state.history_df = hist
                    _recompute_all_mc_trot(
                        st.session_state.filtered_df,
                        st.session_state.composite_df,
                        hist,
                    )
                    st.session_state.mc_last_key = mc_key

    st.markdown("---")
    st.caption("RaceX v1.0 · Georges BODIONG")


# ── Main area ────────────────────────────────────────────────────────────────
race_df: pd.DataFrame = st.session_state.filtered_df   # always the filtered slice
composite_df: pd.DataFrame = st.session_state.composite_df
race_type: str = st.session_state.race_type

st.title("🏇 RaceX — Race Analysis Dashboard")

no_data_loaded = st.session_state.race_df.empty
no_filtered_data = race_df.empty

if no_data_loaded:
    st.info("👈 Select a date, load meetings, choose one and click **Scrape & Analyse** to begin.")
elif no_filtered_data:
    st.info("No data for the selected race. Choose a different REF_COURSE in the sidebar.")

# Warm-load historical data at startup for Dashboard & History
if "history_df" not in st.session_state:
    st.session_state.history_df = _load_flat_history()

# Show active race banner
if st.session_state.selected_ref_course:
    st.caption(f"📍 Analysing race: **{st.session_state.selected_ref_course}** · {len(race_df)} horses")

# ── Tabs ─────────────────────────────────────────────────────────────────────
if race_type == "trot":
    tabs = st.tabs([
        "📋 Raw Data",
        "📊 Composite",
        "🔥 Heatmap",
        "✨ Summary & Prognosis",
        "💪 Fitness (FA/FM)",
        "⚡ Performance (S_COEFF)",
        "📈 Form Trend",
        "👟 Shoeing",
        "⚠️ Disq Risk",
        "🎲 Bet Generator",
    ])
    (
        tab_raw, tab_comp, tab_heat, tab_summary,
        tab_fit, tab_perf, tab_trend, tab_shoe,
        tab_disq, tab_bets,
    ) = tabs
else:
    tabs = st.tabs([
        "📋 Raw Data",
        "📊 Composite",
        "🔥 Heatmap",
        "💪 Fitness (IF)",
        "🏆 Class (IC)",
        "⚡ Success Coeff",
        "⚖️ Weight Stability",
        "🪶 Light Weight",
        "🔮 Prognosis",
        "📈 Trending Odds",
        "🧭 Favorable Cordes",
        "🎲 Bet Generator",
    ])
    (
        tab_raw, tab_comp, tab_heat,
        tab_fit, tab_cls, tab_scoeff,
        tab_wt, tab_lw, tab_prog, tab_trending, tab_corde, tab_bets,
    ) = tabs


# ── RAW DATA ─────────────────────────────────────────────────────────────────
with tab_raw:
    st.subheader("Raw Race Data")
    if no_data_loaded or no_filtered_data:
        st.info("No race data loaded yet.")
    else:
        show_df(race_df)
        st.caption(f"{len(race_df)} horses · {len(race_df.columns)} columns")


# ── COMPOSITE ────────────────────────────────────────────────────────────────
with tab_comp:
    st.subheader("Composite Score Ranking")
    if no_data_loaded or no_filtered_data:
        st.info("No race data loaded yet.")
    else:
        show_df(composite_df)
        if race_type == "trot":
            st.markdown("---")
            st.subheader("Monte Carlo (Trot)")

            with st.expander("Method T1: Composite Uncertainty", expanded=False):
                st.caption("Simulates uncertainty around each horse’s composite score (trot).")
                col_a, col_b, col_c = st.columns(3)
                with col_a:
                    trials_t1 = st.number_input("Trials", 200, 20000, 3000, step=200, key="mc_trot_comp_trials")
                with col_b:
                    sigma_t1 = st.slider("Sigma (Composite)", 0.01, 0.30, 0.06, 0.01, key="mc_trot_comp_sigma")
                with col_c:
                    seed_t1 = st.number_input("Seed (optional)", value=0, step=1, key="mc_trot_comp_seed")
                if st.button("Run MC (Composite)", key="mc_trot_comp_run"):
                    st.session_state.mc_trot_comp_res = _mc_rank_from_composite(
                        composite_df,
                        trials=int(trials_t1),
                        sigma=float(sigma_t1),
                        seed=None if seed_t1 == 0 else int(seed_t1),
                    )

                res_t1 = st.session_state.get("mc_trot_comp_res")
                if isinstance(res_t1, pd.DataFrame) and not res_t1.empty:
                    st.markdown("**Win / Top3 / Top5 Probabilities**")
                    st.dataframe(res_t1, use_container_width=True, height=320)
                    top = res_t1.head(10).copy()
                    top["Horse"] = top["Cheval"].astype(str) + " (" + top["N°"].astype(str) + ")"
                    fig = px.bar(top, x="Horse", y="P(win)", title="Top 10 Win Probabilities (Trot)")
                    st.plotly_chart(fig, use_container_width=True)

            with st.expander("Method T2: Trot Metric Noise", expanded=False):
                st.caption("Adds noise to trot-specific metrics and recomputes ranking.")
                col_a, col_b, col_c = st.columns(3)
                with col_a:
                    trials_t2 = st.number_input("Trials", 200, 20000, 3000, step=200, key="mc_trot_metric_trials")
                with col_b:
                    sigma_t2 = st.slider("Sigma (Metrics)", 0.01, 0.30, 0.05, 0.01, key="mc_trot_metric_sigma")
                with col_c:
                    seed_t2 = st.number_input("Seed (optional)", value=0, step=1, key="mc_trot_metric_seed")

                st.markdown("**Trot weights**")
                wcol1, wcol2 = st.columns(2)
                with wcol1:
                    w_scoeff = st.slider("S_COEFF", 0, 100, 20, key="mc_trot_w_scoeff")
                    w_cote = st.slider("COTE", 0, 100, 15, key="mc_trot_w_cote")
                    w_dq = st.slider("DQ_Risk", 0, 100, 20, key="mc_trot_w_dq")
                    w_fit = st.slider("Fitness", 0, 100, 15, key="mc_trot_w_fit")
                with wcol2:
                    w_slope = st.slider("Slope", 0, 100, 15, key="mc_trot_w_slope")
                    w_rec = st.slider("REC", 0, 100, 10, key="mc_trot_w_rec")
                    w_shoe = st.slider("Shoeing_Agg", 0, 100, 5, key="mc_trot_w_shoe")

                total_w = w_scoeff + w_cote + w_dq + w_fit + w_slope + w_rec + w_shoe
                if total_w <= 0:
                    total_w = 1
                trot_weights = {
                    "S_COEFF": w_scoeff / total_w,
                    "COTE": w_cote / total_w,
                    "DQ_Risk": w_dq / total_w,
                    "Fitness": w_fit / total_w,
                    "Slope": w_slope / total_w,
                    "REC": w_rec / total_w,
                    "Shoeing_Agg": w_shoe / total_w,
                }
                st.caption(f"Total: {w_scoeff + w_cote + w_dq + w_fit + w_slope + w_rec + w_shoe}% (auto-normalized)")
                st.caption("Metrics used: " + ", ".join(trot_weights.keys()))

                if st.button("Run MC (Trot Metrics)", key="mc_trot_metric_run"):
                    st.session_state.mc_trot_metric_res = _mc_rank_from_trot_metrics(
                        race_df,
                        composite_df,
                        weights=trot_weights,
                        trials=int(trials_t2),
                        sigma=float(sigma_t2),
                        seed=None if seed_t2 == 0 else int(seed_t2),
                    )

                res_t2 = st.session_state.get("mc_trot_metric_res")
                if isinstance(res_t2, pd.DataFrame) and not res_t2.empty:
                    st.markdown("**Win / Top3 / Top5 Probabilities**")
                    st.dataframe(res_t2, use_container_width=True, height=320)
                    top = res_t2.head(10).copy()
                    top["Horse"] = top["Cheval"].astype(str) + " (" + top["N°"].astype(str) + ")"
                    fig = px.bar(top, x="Horse", y="P(win)", title="Top 10 Win Probabilities (Trot Metrics)")
                    st.plotly_chart(fig, use_container_width=True)

            with st.expander("Method T3: Odds vs Model (Monte Carlo)", expanded=False):
                st.caption("Blends composite-based probabilities with odds-implied probabilities (trot).")
                col_a, col_b, col_c, col_d = st.columns(4)
                with col_a:
                    trials_t3 = st.number_input("Trials", 500, 50000, 8000, step=500, key="mc_trot_odds_trials")
                with col_b:
                    alpha_t3 = st.slider("Blend α (model weight)", 0.0, 1.0, 0.6, 0.05, key="mc_trot_odds_alpha")
                with col_c:
                    temp_t3 = st.slider("Model temperature", 0.05, 0.5, 0.15, 0.01, key="mc_trot_odds_temp")
                with col_d:
                    seed_t3 = st.number_input("Seed (optional)", value=0, step=1, key="mc_trot_odds_seed")

                if st.button("Run MC (Trot Odds vs Model)", key="mc_trot_odds_run"):
                    st.session_state.mc_trot_odds_res = _mc_odds_vs_model(
                        race_df,
                        composite_df,
                        trials=int(trials_t3),
                        alpha=float(alpha_t3),
                        temp=float(temp_t3),
                        seed=None if seed_t3 == 0 else int(seed_t3),
                    )

                res_t3 = st.session_state.get("mc_trot_odds_res")
                if isinstance(res_t3, pd.DataFrame) and not res_t3.empty:
                    st.markdown("**Model vs Odds vs Simulated Win%**")
                    st.dataframe(res_t3, use_container_width=True, height=360)
                    top = res_t3.head(10).copy()
                    top["Horse"] = top["Cheval"].astype(str) + " (" + top["N°"].astype(str) + ")"
                    fig = px.bar(top, x="Horse", y="P(win_sim)", title="Top 10 Win Probabilities (Trot Blend)")
                    st.plotly_chart(fig, use_container_width=True)

            with st.expander("Method T4: Bet Portfolio EV (Monte Carlo)", expanded=False):
                st.caption("Simulates winners from the blended model and evaluates ticket hit rate and EV (trot).")
                col_a, col_b, col_c, col_d = st.columns(4)
                with col_a:
                    trials_t4 = st.number_input("Trials", 500, 50000, 8000, step=500, key="mc_trot_bet_trials")
                with col_b:
                    alpha_t4 = st.slider("Blend α (model weight)", 0.0, 1.0, 0.6, 0.05, key="mc_trot_bet_alpha")
                with col_c:
                    temp_t4 = st.slider("Model temperature", 0.05, 0.5, 0.15, 0.01, key="mc_trot_bet_temp")
                with col_d:
                    seed_t4 = st.number_input("Seed (optional)", value=0, step=1, key="mc_trot_bet_seed")

                use_generated_t4 = st.checkbox("Use generated Top-K tickets", value=True, key="mc_trot_bet_use_topk")
                max_k_t4 = st.slider("Max K (Top-K tickets)", 2, 8, 5, 1, key="mc_trot_bet_maxk")

                if st.button("Run MC (Trot Bet EV)", key="mc_trot_bet_run"):
                    bets_df = None
                    if use_generated_t4:
                        base = _mc_odds_vs_model(
                            race_df,
                            composite_df,
                            trials=2000,
                            alpha=float(alpha_t4),
                            temp=float(temp_t4),
                            seed=None if seed_t4 == 0 else int(seed_t4),
                        )
                        if isinstance(base, pd.DataFrame) and not base.empty and "N°" in base.columns:
                            tickets = []
                            nums = base["N°"].astype(str).tolist()
                            for k in range(2, max_k_t4 + 1):
                                tickets.append(nums[:k])
                            if tickets:
                                max_len = max(len(r) for r in tickets)
                                cols = [f"H{i+1}" for i in range(max_len)]
                                padded = [r + [""] * (max_len - len(r)) for r in tickets]
                                bets_df = pd.DataFrame(padded, columns=cols)
                    else:
                        if isinstance(st.session_state.last_bets_df, pd.DataFrame):
                            bets_df = st.session_state.last_bets_df

                    st.session_state.mc_trot_bet_res = _mc_eval_bets(
                        race_df,
                        composite_df,
                        trials=int(trials_t4),
                        alpha=float(alpha_t4),
                        temp=float(temp_t4),
                        bets_df=bets_df,
                        seed=None if seed_t4 == 0 else int(seed_t4),
                    )

                res_t4 = st.session_state.get("mc_trot_bet_res")
                if isinstance(res_t4, pd.DataFrame) and not res_t4.empty:
                    st.markdown("**Ticket EV (1 unit stake per ticket)**")
                    st.dataframe(res_t4, use_container_width=True, height=320)

            with st.expander("Method T5: Scenario Analysis (Weights)", expanded=False):
                st.caption("Randomly perturbs trot weights to see which horses stay strong across scenarios. DQ bias can emphasize low DQ risk.")
                col_a, col_b, col_c, col_d = st.columns(4)
                with col_a:
                    scenarios_t5 = st.number_input("Scenarios", 200, 20000, 2000, step=200, key="mc_trot_scenarios")
                with col_b:
                    sigma_t5 = st.slider("Weight noise (sigma)", 0.01, 0.40, 0.10, 0.01, key="mc_trot_scen_sigma")
                with col_c:
                    dq_bias = st.slider("DQ bias", -0.5, 0.5, 0.0, 0.05, key="mc_trot_scen_dq_bias")
                with col_d:
                    seed_t5 = st.number_input("Seed (optional)", value=0, step=1, key="mc_trot_scen_seed")

                if st.button("Run MC (Trot Scenarios)", key="mc_trot_scen_run"):
                    st.session_state.mc_trot_scen_res = _mc_trot_scenario_analysis(
                        race_df,
                        composite_df,
                        base_weights=st.session_state.current_weights or {},
                        scenarios=int(scenarios_t5),
                        sigma=float(sigma_t5),
                        dq_bias=float(dq_bias),
                        seed=None if seed_t5 == 0 else int(seed_t5),
                    )

                res_t5 = st.session_state.get("mc_trot_scen_res")
                if isinstance(res_t5, pd.DataFrame) and not res_t5.empty:
                    st.markdown("**Scenario Robustness**")
                    st.dataframe(res_t5, use_container_width=True, height=320)
                    top = res_t5.head(10).copy()
                    top["Horse"] = top["Cheval"].astype(str) + " (" + top["NÂ°"].astype(str) + ")"
                    fig = px.bar(top, x="Horse", y="P(win)", title="Top 10 Win Probabilities (Trot Scenario Mix)")
                    st.plotly_chart(fig, use_container_width=True)

            with st.expander("Method T6: Historical Similar Races", expanded=False):
                st.caption("Uses historical races from same track and nearby distance to estimate win rates by odds bucket, then simulates winners (trot).")
                col_a, col_b, col_c = st.columns(3)
                with col_a:
                    trials_t6 = st.number_input("Trials", 500, 50000, 8000, step=500, key="mc_trot_hist_trials")
                with col_b:
                    dist_window_t6 = st.slider("Distance window (m)", 100, 600, 200, 50, key="mc_trot_hist_dist")
                with col_c:
                    seed_t6 = st.number_input("Seed (optional)", value=0, step=1, key="mc_trot_hist_seed")

                if st.button("Run MC (Trot Historical)", key="mc_trot_hist_run"):
                    hist_df = st.session_state.get("history_df", pd.DataFrame())
                    res_t6, bins_t6 = _mc_trot_historical_similarity(
                        hist_df,
                        race_df,
                        composite_df,
                        trials=int(trials_t6),
                        distance_window=int(dist_window_t6),
                        seed=None if seed_t6 == 0 else int(seed_t6),
                    )
                    st.session_state.mc_trot_hist_res = res_t6
                    st.session_state.mc_trot_hist_bins = bins_t6

                res_t6 = st.session_state.get("mc_trot_hist_res")
                if isinstance(res_t6, pd.DataFrame) and not res_t6.empty:
                    st.markdown("**Historical Win Probabilities**")
                    st.dataframe(res_t6, use_container_width=True, height=320)
                    top = res_t6.head(10).copy()
                    top["Horse"] = top["Cheval"].astype(str) + " (" + top["NÂ°"].astype(str) + ")"
                    fig = px.bar(top, x="Horse", y="P(win_sim)", title="Top 10 Win Probabilities (Trot Historical)")
                    st.plotly_chart(fig, use_container_width=True)

                bins_t6 = st.session_state.get("mc_trot_hist_bins")
                if isinstance(bins_t6, pd.DataFrame) and not bins_t6.empty:
                    st.markdown("**Historical Odds Bin Stats**")
                    st.dataframe(bins_t6, use_container_width=True, height=220)

        if race_type == "flat":
            st.markdown("---")
            st.subheader("Monte Carlo (Flat)")

            with st.expander("Method 1: Composite Uncertainty", expanded=False):
                st.caption("Simulates uncertainty around each horse’s composite score. Increase sigma to reflect less confidence. Use more trials for stability.")
                col_a, col_b, col_c = st.columns(3)
                with col_a:
                    trials_1 = st.number_input("Trials", 200, 20000, 3000, step=200, key="mc_comp_trials")
                with col_b:
                    sigma_1 = st.slider("Sigma (Composite)", 0.01, 0.30, 0.06, 0.01, key="mc_comp_sigma")
                with col_c:
                    seed_1 = st.number_input("Seed (optional)", value=0, step=1, key="mc_comp_seed")
                if st.button("Run MC (Composite)", key="mc_comp_run"):
                    st.session_state.mc_comp_res = _mc_rank_from_composite(
                        composite_df,
                        trials=int(trials_1),
                        sigma=float(sigma_1),
                        seed=None if seed_1 == 0 else int(seed_1),
                    )

                res_1 = st.session_state.get("mc_comp_res")
                if isinstance(res_1, pd.DataFrame) and not res_1.empty:
                    st.markdown("**Win / Top3 / Top5 Probabilities**")
                    st.dataframe(res_1, use_container_width=True, height=320)
                    top = res_1.head(10).copy()
                    top["Horse"] = top["Cheval"].astype(str) + " (" + top["N°"].astype(str) + ")"
                    fig = px.bar(top, x="Horse", y="P(win)", title="Top 10 Win Probabilities")
                    st.plotly_chart(fig, use_container_width=True)

            with st.expander("Method 2: Metric Noise (Recompute Composite)", expanded=False):
                st.caption("Adds noise to normalized flat metrics and recomputes composite each trial. Useful to test ranking robustness to small data errors.")
                col_a, col_b, col_c = st.columns(3)
                with col_a:
                    trials_2 = st.number_input("Trials", 200, 20000, 3000, step=200, key="mc_metric_trials")
                with col_b:
                    sigma_2 = st.slider("Sigma (Metrics)", 0.01, 0.30, 0.05, 0.01, key="mc_metric_sigma")
                with col_c:
                    seed_2 = st.number_input("Seed (optional)", value=0, step=1, key="mc_metric_seed")

                flat_cfg = _weights_config("flat")
                flat_keys = set(flat_cfg.keys())
                base_weights = {k: v for k, v in (st.session_state.current_weights or {}).items() if k in flat_keys}
                # Ensure required flat metrics are present with default weights if missing
                required_flat = ["IF", "IC", "n_weight", "Corde", "VALEUR"]
                weights_flat = dict(base_weights)
                for k in required_flat:
                    if k not in weights_flat:
                        weights_flat[k] = flat_cfg[k][1]
                used = [k for k in flat_keys if k in weights_flat]
                if used:
                    st.caption("Metrics used: " + ", ".join(used))

                if st.button("Run MC (Metrics)", key="mc_metric_run"):
                    st.session_state.mc_metric_res = _mc_rank_from_metrics(
                        race_df,
                        composite_df,
                        weights=weights_flat,
                        trials=int(trials_2),
                        sigma=float(sigma_2),
                        seed=None if seed_2 == 0 else int(seed_2),
                    )

                res_2 = st.session_state.get("mc_metric_res")
                if isinstance(res_2, pd.DataFrame) and not res_2.empty:
                    st.markdown("**Win / Top3 / Top5 Probabilities**")
                    st.dataframe(res_2, use_container_width=True, height=320)
                    top = res_2.head(10).copy()
                    top["Horse"] = top["Cheval"].astype(str) + " (" + top["N°"].astype(str) + ")"
                    fig = px.bar(top, x="Horse", y="P(win)", title="Top 10 Win Probabilities (Metrics)")
                    st.plotly_chart(fig, use_container_width=True)

            with st.expander("Method 3: Odds vs Model (Monte Carlo)", expanded=False):
                st.caption("Blends model probabilities with odds-implied probabilities. Alpha controls how much you trust the model vs the market.")
                col_a, col_b, col_c, col_d = st.columns(4)
                with col_a:
                    trials_3 = st.number_input("Trials", 500, 50000, 8000, step=500, key="mc_odds_trials")
                with col_b:
                    alpha_3 = st.slider("Blend α (model weight)", 0.0, 1.0, 0.6, 0.05, key="mc_odds_alpha")
                with col_c:
                    temp_3 = st.slider("Model temperature", 0.05, 0.5, 0.15, 0.01, key="mc_odds_temp")
                with col_d:
                    seed_3 = st.number_input("Seed (optional)", value=0, step=1, key="mc_odds_seed")

                if st.button("Run MC (Odds vs Model)", key="mc_odds_run"):
                    st.session_state.mc_odds_res = _mc_odds_vs_model(
                        race_df,
                        composite_df,
                        trials=int(trials_3),
                        alpha=float(alpha_3),
                        temp=float(temp_3),
                        seed=None if seed_3 == 0 else int(seed_3),
                    )

                res_3 = st.session_state.get("mc_odds_res")
                if isinstance(res_3, pd.DataFrame) and not res_3.empty:
                    st.markdown("**Model vs Odds vs Simulated Win%**")
                    st.dataframe(res_3, use_container_width=True, height=360)
                    top = res_3.head(10).copy()
                    top["Horse"] = top["Cheval"].astype(str) + " (" + top["N°"].astype(str) + ")"
                    fig = px.bar(top, x="Horse", y="P(win_sim)", title="Top 10 Win Probabilities (Blend)")
                    st.plotly_chart(fig, use_container_width=True)

            with st.expander("Method 4: Bet Portfolio EV (Monte Carlo)", expanded=False):
                st.caption("Simulates winners from the blended model and evaluates ticket hit rate and EV. Use generated Top‑K tickets or your last bets.")
                col_a, col_b, col_c, col_d = st.columns(4)
                with col_a:
                    trials_4 = st.number_input("Trials", 500, 50000, 8000, step=500, key="mc_bet_trials")
                with col_b:
                    alpha_4 = st.slider("Blend α (model weight)", 0.0, 1.0, 0.6, 0.05, key="mc_bet_alpha")
                with col_c:
                    temp_4 = st.slider("Model temperature", 0.05, 0.5, 0.15, 0.01, key="mc_bet_temp")
                with col_d:
                    seed_4 = st.number_input("Seed (optional)", value=0, step=1, key="mc_bet_seed")

                use_generated = st.checkbox("Use generated Top-K tickets", value=True, key="mc_bet_use_topk")
                max_k = st.slider("Max K (Top-K tickets)", 2, 8, 5, 1, key="mc_bet_maxk")

                if st.button("Run MC (Bet EV)", key="mc_bet_run"):
                    bets_df = None
                    if use_generated:
                        base = _mc_odds_vs_model(
                            race_df,
                            composite_df,
                            trials=2000,
                            alpha=float(alpha_4),
                            temp=float(temp_4),
                            seed=None if seed_4 == 0 else int(seed_4),
                        )
                        if isinstance(base, pd.DataFrame) and not base.empty:
                            num_col = "N°" if "N°" in base.columns else None
                            if num_col:
                                tickets = []
                                nums = base[num_col].astype(str).tolist()
                                for k in range(2, max_k + 1):
                                    row = nums[:k]
                                    tickets.append(row)
                                # normalize to rectangular df
                                max_len = max(len(r) for r in tickets) if tickets else 0
                                cols = [f"H{i+1}" for i in range(max_len)]
                                padded = [r + [""] * (max_len - len(r)) for r in tickets]
                                bets_df = pd.DataFrame(padded, columns=cols)
                    else:
                        if isinstance(st.session_state.last_bets_df, pd.DataFrame):
                            bets_df = st.session_state.last_bets_df

                    st.session_state.mc_bet_res = _mc_eval_bets(
                        race_df,
                        composite_df,
                        trials=int(trials_4),
                        alpha=float(alpha_4),
                        temp=float(temp_4),
                        bets_df=bets_df,
                        seed=None if seed_4 == 0 else int(seed_4),
                    )

                res_4 = st.session_state.get("mc_bet_res")
                if isinstance(res_4, pd.DataFrame) and not res_4.empty:
                    st.markdown("**Ticket EV (1 unit stake per ticket)**")
                    st.dataframe(res_4, use_container_width=True, height=320)

            with st.expander("Method 5: Scenario Analysis (Weights)", expanded=False):
                st.caption("Randomly perturbs weight settings to see which horses stay strong across scenarios. Corde bias can emphasize draw advantage.")
                col_a, col_b, col_c, col_d = st.columns(4)
                with col_a:
                    scenarios_5 = st.number_input("Scenarios", 200, 20000, 2000, step=200, key="mc_scenarios")
                with col_b:
                    sigma_5 = st.slider("Weight noise (sigma)", 0.01, 0.40, 0.10, 0.01, key="mc_scen_sigma")
                with col_c:
                    corde_bias = st.slider("Corde bias", -0.5, 0.5, 0.0, 0.05, key="mc_scen_corde")
                with col_d:
                    seed_5 = st.number_input("Seed (optional)", value=0, step=1, key="mc_scen_seed")

                if st.button("Run MC (Scenarios)", key="mc_scen_run"):
                    st.session_state.mc_scen_res = _mc_scenario_analysis(
                        race_df,
                        composite_df,
                        base_weights=st.session_state.current_weights or {},
                        scenarios=int(scenarios_5),
                        sigma=float(sigma_5),
                        corde_bias=float(corde_bias),
                        seed=None if seed_5 == 0 else int(seed_5),
                    )

                res_5 = st.session_state.get("mc_scen_res")
                if isinstance(res_5, pd.DataFrame) and not res_5.empty:
                    st.markdown("**Scenario Robustness**")
                    st.dataframe(res_5, use_container_width=True, height=320)
                    top = res_5.head(10).copy()
                    top["Horse"] = top["Cheval"].astype(str) + " (" + top["N°"].astype(str) + ")"
                    fig = px.bar(top, x="Horse", y="P(win)", title="Top 10 Win Probabilities (Scenario Mix)")
                    st.plotly_chart(fig, use_container_width=True)

            with st.expander("Method 6: Historical Similar Races", expanded=False):
                st.caption("Uses historical races from same track and nearby distance to estimate win rates by odds bucket, then simulates winners.")
                col_a, col_b, col_c = st.columns(3)
                with col_a:
                    trials_6 = st.number_input("Trials", 500, 50000, 8000, step=500, key="mc_hist_trials")
                with col_b:
                    dist_window = st.slider("Distance window (m)", 100, 600, 200, 50, key="mc_hist_dist")
                with col_c:
                    seed_6 = st.number_input("Seed (optional)", value=0, step=1, key="mc_hist_seed")

                if st.button("Run MC (Historical)", key="mc_hist_run"):
                    hist_df = st.session_state.get("history_df", pd.DataFrame())
                    res_6, bins_6 = _mc_historical_similarity(
                        hist_df,
                        race_df,
                        composite_df,
                        trials=int(trials_6),
                        distance_window=int(dist_window),
                        seed=None if seed_6 == 0 else int(seed_6),
                    )
                    st.session_state.mc_hist_res = res_6
                    st.session_state.mc_hist_bins = bins_6

                res_6 = st.session_state.get("mc_hist_res")
                if isinstance(res_6, pd.DataFrame) and not res_6.empty:
                    st.markdown("**Historical Win Probabilities**")
                    st.dataframe(res_6, use_container_width=True, height=320)
                    top = res_6.head(10).copy()
                    top["Horse"] = top["Cheval"].astype(str) + " (" + top["N°"].astype(str) + ")"
                    fig = px.bar(top, x="Horse", y="P(win_sim)", title="Top 10 Win Probabilities (Historical)")
                    st.plotly_chart(fig, use_container_width=True)

                bins_6 = st.session_state.get("mc_hist_bins")
                if isinstance(bins_6, pd.DataFrame) and not bins_6.empty:
                    st.markdown("**Historical Odds Bin Stats**")
                    st.dataframe(bins_6, use_container_width=True, height=220)


# ── HEATMAP ──────────────────────────────────────────────────────────────────
with tab_heat:
    st.subheader("Performance Heatmap")
    if no_data_loaded or no_filtered_data:
        st.info("No race data loaded yet.")
    elif composite_df.empty:
        st.info("Run analysis first.")
    else:
        # Build normalised matrix for heatmap
        if race_type == "trot":
            # Trotting-specific heatmap metrics (align with desktop)
            candidate_cols = [
                'FA', 'FM', 'REC.', 'REC', 'avg_rank', 'recent_avg_rank', 'recent_avg',
                'trend_rank_slope', 'trend_slope', 'S_COEFF', 's_COEFF', 'COTE', 'Cote',
                'disq_harness_rate', 'disq_mounted_rate', 'DQ_Risk'
            ]
            metrics = [c for c in candidate_cols if c in race_df.columns]
            if not metrics:
                # Fallback: keyword-based detection (desktop behavior)
                patterns = ['fa', 'fm', 'rec', 'avg_rank', 'recent', 'trend', 's_coeff', 'cote', 'disq', 'dq']
                for c in race_df.columns:
                    low = str(c).lower()
                    if any(p in low for p in patterns) and c not in metrics:
                        metrics.append(c)
            # De-duplicate while preserving order
            seen = set()
            metrics = [m for m in metrics if not (m in seen or seen.add(m))]
            if not metrics:
                st.info("Not enough data to build heatmap.")
            else:
                horse_col = next((c for c in ["CHEVAL", "Cheval"] if c in race_df.columns), None)
                if not horse_col:
                    st.info("Horse name column not available for heatmap.")
                else:
                    st.markdown("**Heatmap Controls**")
                    selected_metrics = st.multiselect(
                        "Metrics",
                        metrics,
                        default=metrics,
                    )
                    sort_by = st.selectbox("Sort rows by", ["(none)"] + selected_metrics, key="trot_heat_sort")
                    vmin, vmax = st.slider("Color scale (normalized)", 0.0, 1.0, (0.0, 1.0), 0.01, key="trot_heat_vminmax")
                    show_values = st.checkbox("Show cell values", value=True, key="trot_heat_show_values")

                    if not selected_metrics:
                        st.info("Select at least one metric.")
                    else:
                        mat = pd.DataFrame(index=race_df[horse_col].astype(str))
                        for col in selected_metrics:
                            # Robust parsing to match desktop logic
                            try:
                                tmp = race_df[col].astype(str).str.strip().str.replace(",", ".")
                                nums = tmp.str.extract(r'(-?\d+\.?\d*)', expand=False)
                                ser = pd.to_numeric(nums, errors="coerce")
                            except Exception:
                                ser = pd.to_numeric(race_df[col], errors="coerce")

                            higher_better = True
                            if col in ('FA', 'FM', 'REC.', 'REC', 'avg_rank', 'recent_avg_rank'):
                                higher_better = False
                            if col in ('trend_rank_slope',):
                                ser = -ser
                                higher_better = True
                            if col in ('disq_harness_rate', 'disq_mounted_rate', 'DQ_Risk'):
                                higher_better = False
                            if col in ('COTE', 'Cote'):
                                ser = -ser
                                higher_better = True

                            if ser.dropna().empty or ser.max() == ser.min():
                                norm = pd.Series(0.5, index=race_df.index)
                            else:
                                norm = (ser - ser.min()) / (ser.max() - ser.min())
                            if not higher_better:
                                norm = 1.0 - norm
                            norm = norm.fillna(0.5)
                            norm.index = race_df[horse_col].astype(str)
                            mat[col] = norm

                        if sort_by != "(none)" and sort_by in mat.columns:
                            mat = mat.sort_values(sort_by, ascending=False)

                        fig = px.imshow(
                            mat,
                            color_continuous_scale="RdYlGn",
                            zmin=vmin,
                            zmax=vmax,
                            aspect="auto",
                        )
                        if show_values:
                            fig.update_traces(text=mat.round(2).astype(str).values, texttemplate="%{text}")
                        fig.update_layout(title="Trotting Heatmap (0=Low, 1=High)")
                        st.plotly_chart(fig, use_container_width=True)
        else:
            # Flat races: use composite_df numeric columns
            cheval_col = next((c for c in ["CHEVAL", "Cheval"] if c in composite_df.columns), None)
            # Flat races: use numeric metrics (exclude rank/horse number columns)
            num_cols = composite_df.select_dtypes(include="number").columns.tolist()
            skip = {"Rank"}
            metric_cols = [c for c in num_cols if c not in skip]
            # Exclude horse number columns from metrics
            metric_cols = [c for c in metric_cols if c not in {"N°", "N", "NUM", "Num", "Numero"}]

            if cheval_col and metric_cols:
                st.markdown("**Heatmap Controls**")
                selected_metrics = st.multiselect(
                    "Metrics",
                    metric_cols,
                    default=metric_cols,
                )
                sort_by = st.selectbox("Sort rows by", ["(none)"] + selected_metrics, key="flat_heat_sort")
                vmin, vmax = st.slider("Color scale (normalized)", 0.0, 1.0, (0.0, 1.0), 0.01, key="flat_heat_vminmax")
                show_values = st.checkbox("Show cell values", value=True, key="flat_heat_show_values")

                if not selected_metrics:
                    st.info("Select at least one metric.")
                else:
                    # Build heatmap matrix aligned to horse names
                    if not cheval_col:
                        st.info("Horse name column not available for heatmap.")
                        mat = None
                    else:
                        mat = composite_df.set_index(cheval_col)[selected_metrics].copy()
                    if mat is None:
                        pass
                    else:
                        # Append horse number to index label if available
                        num_col = _detect_horse_num_col(composite_df)
                        if num_col:
                            try:
                                num_map = composite_df.set_index(cheval_col)[num_col].astype(str).to_dict()
                                mat.index = [f"{name} (N°{num_map.get(name, '')})" for name in mat.index]
                            except Exception:
                                pass
                        # Normalize using pre-normalized columns when available
                        for col in mat.columns:
                            norm_col = f"_norm_{col}"
                            if norm_col in race_df.columns and cheval_col in race_df.columns:
                                series = race_df[norm_col].copy()
                                series.index = race_df[cheval_col].astype(str)
                                series = series.reindex(mat.index, fill_value=0.5)
                                mat[col] = series.values
                                continue

                            series = mat[col].copy()
                            # For COTE, lower is better -> invert
                            if col in ("COTE", "Cote"):
                                series = -series
                            mn, mx = series.min(), series.max()
                            if mx > mn:
                                mat[col] = (series - mn) / (mx - mn)
                            else:
                                mat[col] = 0.5
                        mat = mat.fillna(0.5)

                        if sort_by != "(none)" and sort_by in mat.columns:
                            mat = mat.sort_values(sort_by, ascending=False)

                        fig = px.imshow(
                            mat,
                            color_continuous_scale="RdYlGn",
                            zmin=vmin,
                            zmax=vmax,
                            aspect="auto",
                        )
                        if show_values:
                            fig.update_traces(text=mat.round(2).astype(str).values, texttemplate="%{text}")
                        fig.update_layout(title="Horse Performance Heatmap (0=Low, 1=High)")
                        st.plotly_chart(fig, use_container_width=True)
                        with st.expander("Export Heatmap", expanded=False):
                            if st.button("Prepare Heatmap Export", key="prep_heatmap_export"):
                                # Plotly image export requires kaleido; guard if unavailable
                                try:
                                    st.session_state.prepared_heatmap_png = fig.to_image(format="png", width=1200, height=600, scale=2)
                                    st.session_state.prepared_heatmap_pdf = fig.to_image(format="pdf", width=1200, height=600)
                                except Exception as e:
                                    st.session_state.prepared_heatmap_png = None
                                    st.session_state.prepared_heatmap_pdf = None
                                    st.error(f"Heatmap export requires 'kaleido'. Install with `pip install kaleido`. ({e})")

                            if st.session_state.get("prepared_heatmap_png"):
                                st.download_button(
                                    "Download Heatmap PNG",
                                    data=st.session_state.prepared_heatmap_png,
                                    file_name="race_heatmap.png",
                                    mime="image/png",
                                )
                            if st.session_state.get("prepared_heatmap_pdf"):
                                st.download_button(
                                    "Download Heatmap PDF",
                                    data=st.session_state.prepared_heatmap_pdf,
                                    file_name="race_heatmap.pdf",
                                    mime="application/pdf",
                                )
                            # Plotly handles figure lifecycle
            else:
                st.info("Not enough data to build heatmap.")


# ── TROTTING TABS ─────────────────────────────────────────────────────────────
if race_type == "trot":

    with tab_summary:
        st.subheader("Summary & Prognosis")
        if no_data_loaded or no_filtered_data:
            st.info("No race data loaded yet.")
            st.session_state.last_summary_df = None
        elif composite_df.empty:
            st.info("Run analysis first.")
            st.session_state.last_summary_df = None
        else:
            summary_df = analyze_trotting_summary_prognosis(race_df, composite_df)
            st.session_state.last_summary_df = summary_df if isinstance(summary_df, pd.DataFrame) and not summary_df.empty else None
            if summary_df.empty:
                st.info("No summary data.")
            else:
                for section, color in [("SUMMARY", "#e8f4f8"), ("PROGNOSIS", "#fff9e6"), ("EXCLUSIVES", "#ffe8e8")]:
                    part = summary_df[summary_df["Type"] == section].drop(columns=["Type"], errors="ignore")
                    if not part.empty:
                        labels = {"SUMMARY": "⭐ Top Contenders", "PROGNOSIS": "🔮 Predictions", "EXCLUSIVES": "❌ Avoid"}
                        st.markdown(f"<div style='background:{color};padding:8px;border-radius:4px;font-weight:bold'>{labels[section]}</div>", unsafe_allow_html=True)
                        st.dataframe(part, use_container_width=True)

    with tab_fit:
        st.subheader("Fitness (FA / FM)")
        if no_data_loaded or no_filtered_data:
            st.info("No race data loaded yet.")
        else:
            show_df(analyze_trotting_fitness(race_df))

    with tab_perf:
        st.subheader("Performance — Success Coefficient")
        if no_data_loaded or no_filtered_data:
            st.info("No race data loaded yet.")
        else:
            show_df(analyze_trotting_performance(race_df))

    with tab_trend:
        st.subheader("Form Trend")
        if no_data_loaded or no_filtered_data:
            st.info("No race data loaded yet.")
        else:
            show_df(analyze_trotting_trend(race_df))

    with tab_shoe:
        st.subheader("Shoeing Strategy")
        if no_data_loaded or no_filtered_data:
            st.info("No race data loaded yet.")
        else:
            show_df(analyze_trotting_shoeing(race_df))

    with tab_disq:
        st.subheader("Disqualification Risk")
        if no_data_loaded or no_filtered_data:
            st.info("No race data loaded yet.")
        else:
            show_df(analyze_trotting_disqualification_risk(race_df))

    with tab_bets:
        st.subheader("🎲 Bet Generator")
        if no_data_loaded or no_filtered_data:
            st.info("No race data loaded yet.")
        else:
            col1, col2, col3 = st.columns(3)
            with col1:
                combo_size = st.number_input("Horses per combo", 1, 8, 5)
            with col2:
                max_combos = st.number_input("Max combos", 1, 200, 50)
            with col3:
                manual_base = st.text_input("Mandatory horses (comma-sep)", placeholder="e.g. 2,5")

            if st.button("Generate Trotting Bets", type="primary"):
                combos = generate_trotting_bets(
                    race_df, composite_df,
                    desired_size=int(combo_size),
                    max_combos=int(max_combos),
                    manual_base=manual_base,
                )
                if combos:
                    bets_df = pd.DataFrame(combos, columns=[f"H{i+1}" for i in range(len(combos[0]))])
                    st.success(f"Generated {len(bets_df)} combinations")
                    st.dataframe(bets_df, use_container_width=True)
                    st.session_state.last_bets_df = bets_df
                    st.session_state.last_bets_label = "trotting_bets"
                else:
                    st.warning("Could not generate combinations. Check race data.")


# ── FLAT TABS ─────────────────────────────────────────────────────────────────
else:

    with tab_fit:
        st.subheader("Fitness — IF (lower = better)")
        if no_data_loaded or no_filtered_data:
            st.info("No race data loaded yet.")
        else:
            show_df(analyze_fitness_if(race_df))

    with tab_cls:
        st.subheader("Class — IC (higher = better)")
        if no_data_loaded or no_filtered_data:
            st.info("No race data loaded yet.")
        else:
            show_df(analyze_class_ic(race_df))

    with tab_scoeff:
        st.subheader("Success Coefficient")
        if no_data_loaded or no_filtered_data:
            st.info("No race data loaded yet.")
        else:
            show_df(analyze_success_coeff(race_df))

    with tab_wt:
        st.subheader("Weight Stability")
        if no_data_loaded or no_filtered_data:
            st.info("No race data loaded yet.")
        else:
            show_df(analyze_weight_stability(race_df))

    with tab_lw:
        st.subheader("Light Weight Surprise")
        if no_data_loaded or no_filtered_data:
            st.info("No race data loaded yet.")
        else:
            show_df(analyze_light_weight_surprise(race_df))

    with tab_prog:
        st.subheader("🔮 Prognosis")
        if no_data_loaded or no_filtered_data:
            st.info("No race data loaded yet.")
        elif composite_df.empty:
            st.info("Run analysis first.")
        else:
            prog = _sorted_prognosis_display(race_df, composite_df)
            if prog:
                st.markdown("**Predicted order (best first):**")
                st.write(" → ".join(prog))

            col_a, col_b = st.columns(2)
            with col_a:
                st.markdown("**Outsiders (Odds Divergence)**")
                outsiders_df = analyze_odds_divergence(race_df, composite_df)
                show_df(outsiders_df, height=300)
            with col_b:
                st.markdown("**Consistency Score**")
                consistency_df = analyze_consistency_score(race_df, composite_df)
                show_df(consistency_df, height=300)

            st.markdown("**Underperforming Favourites**")
            underperf_df = analyze_underperforming_favorites(race_df, composite_df)
            show_df(underperf_df, height=250)
            st.session_state.flat_analysis = _build_flat_analysis_payload(race_df, composite_df)

    with tab_trending:
        st.subheader("Trending Odds")
        if no_data_loaded or no_filtered_data:
            st.info("No race data loaded yet.")
        else:
            trending = compute_trending_horses(race_df, top_n=3)
            if trending:
                for item in trending:
                    st.write(f"• {item}")
            else:
                st.info("No trending odds detected for this race.")

    with tab_corde:
        st.subheader("Favorable Cordes")
        if no_data_loaded or no_filtered_data:
            st.info("No race data loaded yet.")
        else:
            hippodrome, distance = _get_race_meta(race_df)
            if not hippodrome or not distance:
                st.info("Hippodrome or distance not available for this race.")
            else:
                temp_df = race_df.copy()
                if "NÂ°" not in temp_df.columns:
                    num_col = _detect_horse_num_col(temp_df)
                    if num_col:
                        temp_df = temp_df.rename(columns={num_col: "NÂ°"})
                if "CORDE" not in temp_df.columns:
                    st.info("CORDE column not available in race data.")
                else:
                    favorable = compute_favorable_corde_horses(temp_df, hippodrome, distance, top_n=3)
                    if favorable:
                        st.markdown(f"**{hippodrome} — {distance}m**")
                        st.write("Favorable horses (by number): " + ", ".join(favorable))
                    else:
                        st.info("No favorable cordes identified for this race.")

    with tab_bets:
        st.subheader("🎲 Bet Generator")
        if no_data_loaded or no_filtered_data:
            st.info("No race data loaded yet.")
        else:
            col1, col2 = st.columns(2)
            with col1:
                combo_size_f = st.number_input("Horses per combo", 1, 8, 5, key="flat_combo")
                max_combos_f = st.number_input("Max combos", 1, 200, 50, key="flat_max")
            with col2:
                manual_base_f = st.text_input("Mandatory horses", placeholder="e.g. 1,3", key="flat_base")
                optional_f = st.text_input("Optional pool (leave blank = auto)", placeholder="e.g. 2,4,5,7", key="flat_opt")

            if st.button("Generate Flat Bets", type="primary"):
                base = [h.strip() for h in manual_base_f.split(",") if h.strip()] if manual_base_f else compute_prognosis(race_df, max_len=2)
                associates = [h.strip() for h in optional_f.split(",") if h.strip()] if optional_f else compute_summary_horses(composite_df)
                associates = [a for a in associates if a not in base]
                combos = reducing_system(base, associates, int(combo_size_f))[:int(max_combos_f)]
                if combos:
                    bets_df = pd.DataFrame(combos, columns=[f"H{i+1}" for i in range(len(combos[0]))])
                    st.success(f"Generated {len(bets_df)} combinations")
                    st.dataframe(bets_df, use_container_width=True)
                    st.session_state.last_bets_df = bets_df
                    st.session_state.last_bets_label = "flat_bets"
                else:
                    st.warning("Could not generate combinations.")
