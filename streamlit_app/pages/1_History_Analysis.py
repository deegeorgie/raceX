import os
import sqlite3
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="RaceX - History & Analysis", page_icon="📊", layout="wide")

st.markdown(
    """
    <style>
    [data-testid="stSidebarNav"] { display: none; }
    </style>
    """,
    unsafe_allow_html=True,
)

with st.sidebar:
    st.title("🏇 RaceX")
    st.markdown("---")
    st.subheader("Pages")
    st.page_link("app.py", label="Race Analysis", icon="🏇")
    st.page_link("pages/1_History_Analysis.py", label="History & Analysis", icon="📊")
    st.page_link("pages/3_Dictionary.py", label="Dictionary", icon="📘")
    st.page_link("pages/4_Insights_PMU.py", label="Insights & PMU", icon="🧠")
    st.page_link("pages/2_About.py", label="About", icon="ℹ️")

st.title("📊 History & Analysis")

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


def _parse_numeric_series(ser: pd.Series) -> pd.Series:
    try:
        if ser is None or ser.empty:
            return pd.Series(dtype=float)
        s = ser.astype(str).str.replace(',', '.').str.extract(r'([0-9]+\.?[0-9]*)')[0]
        return pd.to_numeric(s, errors='coerce')
    except Exception:
        return pd.to_numeric(ser, errors='coerce')


def _extract_distance_m(df: pd.DataFrame) -> pd.Series:
    for col in ("DISTANCE", "DIST", "Distance", "distance"):
        if col in df.columns:
            return _parse_numeric_series(df[col])
    return pd.Series(dtype=float)


def _show_df(df: pd.DataFrame, height: int = 400):
    if df is None or df.empty:
        st.info("No data available.")
    else:
        st.dataframe(df, width="stretch", height=height)


history_df = _load_flat_history()
if history_df is None or history_df.empty:
    st.warning("No historical data found in zone_turf_flat.db (table: plat).")
    st.stop()

st.markdown("**Filters**")
col_a, col_b, col_c = st.columns(3)
with col_a:
    racetype = st.selectbox("Race Type", ["All", "Flat", "Obstacles"], key="dash_racetype")
with col_b:
    hippodromes = ["All"]
    if "HIPPODROME" in history_df.columns:
        hippodromes += sorted(history_df["HIPPODROME"].dropna().unique().tolist())
    hippo = st.selectbox("Hippodrome", hippodromes, key="dash_hippo")
with col_c:
    dist_bin = st.selectbox("Distance", ["All", "Short (<1600m)", "Medium (1600-2400m)", "Long (>2400m)"], key="dash_dist")

data_df = history_df.copy()
if racetype != "All" and "DSCP" in data_df.columns:
    if racetype == "Flat":
        data_df = data_df[data_df["DSCP"] == 1]
    else:
        data_df = data_df[data_df["DSCP"].isin([2, 3, 4])]
if hippo != "All" and "HIPPODROME" in data_df.columns:
    data_df = data_df[data_df["HIPPODROME"] == hippo]

dist_m = _extract_distance_m(data_df)
if not dist_m.empty and dist_bin != "All":
    if dist_bin.startswith("Short"):
        data_df = data_df[dist_m < 1600]
    elif dist_bin.startswith("Medium"):
        data_df = data_df[(dist_m >= 1600) & (dist_m <= 2400)]
    elif dist_bin.startswith("Long"):
        data_df = data_df[dist_m > 2400]

if data_df.empty:
    st.info("No data for the selected filters.")
    st.stop()

with st.expander("Advanced Filters", expanded=False):
    if "COTE" in data_df.columns:
        cote_vals = _parse_numeric_series(data_df["COTE"])
        if not cote_vals.dropna().empty:
            c_min, c_max = float(cote_vals.min()), float(cote_vals.max())
            cote_range = st.slider("COTE range", c_min, c_max, (c_min, c_max))
            mask = cote_vals.between(cote_range[0], cote_range[1])
            data_df = data_df[mask.fillna(False)]
    if "POIDS" in data_df.columns:
        poids_vals = _parse_numeric_series(data_df["POIDS"])
        if not poids_vals.dropna().empty:
            p_min, p_max = float(poids_vals.min()), float(poids_vals.max())
            poids_range = st.slider("POIDS range", p_min, p_max, (p_min, p_max))
            mask = poids_vals.between(poids_range[0], poids_range[1])
            data_df = data_df[mask.fillna(False)]
    dist_vals = _extract_distance_m(data_df)
    if not dist_vals.dropna().empty:
        d_min, d_max = float(dist_vals.min()), float(dist_vals.max())
        if d_min == d_max:
            st.caption(f"Distance fixed at {d_min:.0f}m")
        else:
            dist_range = st.slider("Distance (m) range", d_min, d_max, (d_min, d_max))
            mask = dist_vals.between(dist_range[0], dist_range[1])
            data_df = data_df[mask.fillna(False)]

if data_df.empty:
    st.info("No data for the selected filters.")
    st.stop()

# Summary metrics
odds = _parse_numeric_series(data_df["COTE"]) if "COTE" in data_df.columns else pd.Series(dtype=float)
avg_odds = odds.mean() if not odds.dropna().empty else None
median_odds = odds.median() if not odds.dropna().empty else None

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Rows", f"{len(data_df)}")
c2.metric("Races", f"{data_df['REF_COURSE'].nunique()}" if "REF_COURSE" in data_df.columns else "0")
c3.metric("Tracks", f"{data_df['HIPPODROME'].nunique()}" if "HIPPODROME" in data_df.columns else "0")
c4.metric("Avg Odds", f"{avg_odds:.2f}" if avg_odds is not None else "—")
c5.metric("Median Odds", f"{median_odds:.2f}" if median_odds is not None else "—")

st.markdown("**Historical Data (Preview)**")
_show_df(data_df.head(200), height=320)

st.markdown("**Distributions**")
col1, col2 = st.columns(2)
with col1:
    if not odds.dropna().empty:
        fig = px.histogram(x=odds.dropna(), nbins=20, title="Odds Distribution")
        st.plotly_chart(fig, width="stretch")
    else:
        st.info("No odds data available.")
with col2:
    dist_vals = _extract_distance_m(data_df)
    if not dist_vals.dropna().empty:
        fig = px.histogram(x=dist_vals.dropna(), nbins=20, title="Distance Distribution (m)")
        st.plotly_chart(fig, width="stretch")
    else:
        st.info("No distance data available.")

st.markdown("**Track/Race Type Comparisons**")
compare_by = st.selectbox("Compare by", ["HIPPODROME", "Race Type (DSCP)"], key="dash_compare")
if compare_by == "HIPPODROME" and "HIPPODROME" in data_df.columns:
    group_col = "HIPPODROME"
elif compare_by == "Race Type (DSCP)" and "DSCP" in data_df.columns:
    group_col = "DSCP"
else:
    group_col = None

if group_col:
    grp = data_df.copy()
    grp["_odds"] = _parse_numeric_series(grp["COTE"]) if "COTE" in grp.columns else np.nan
    if "RANG" in grp.columns:
        grp["_rank"] = pd.to_numeric(grp["RANG"], errors="coerce")
        grp["_place"] = (grp["_rank"] <= 3).astype(int)
    else:
        grp["_place"] = np.nan
    summary = grp.groupby(group_col).agg(
        rows=(group_col, "count"),
        avg_odds=("_odds", "mean"),
        place_rate=("_place", "mean"),
    ).sort_values("rows", ascending=False).head(20)
    fig = px.bar(summary, x=summary.index.astype(str), y="avg_odds", title="Average Odds by Group")
    st.plotly_chart(fig, width="stretch")
    if summary["place_rate"].notna().any():
        fig = px.bar(summary, x=summary.index.astype(str), y="place_rate", title="Place Rate (Top 3) by Group")
        st.plotly_chart(fig, width="stretch")
else:
    st.info("Grouping column not available in historical data.")

st.markdown("**Outcome (RANG) vs Factors**")
if "RANG" not in data_df.columns:
    st.info("RANG column not available in historical data.")
else:
    rang = pd.to_numeric(data_df["RANG"], errors="coerce")
    outcome_mode = st.radio("Outcome", ["Mean RANG (lower is better)", "Place Rate (Top 3)"], horizontal=True, key="dash_outcome_mode")
    use_place = outcome_mode.startswith("Place")

    factor_options = [c for c in ["COTE", "POIDS", "N_WEIGHT", "CORDE", "AGE", "SEXE", "HIPPODROME", "DSCP"] if c in data_df.columns]
    if not factor_options:
        st.info("No suitable factors found for bivariate analysis.")
    else:
        factor = st.selectbox("Factor", factor_options, key="dash_bivariate_factor")
        if factor in ["SEXE", "HIPPODROME", "DSCP"]:
            cat = data_df[factor].astype(str)
            df_plot = pd.DataFrame({"RANG": rang, "cat": cat}).dropna()
            if df_plot.empty:
                st.info("Not enough data for this factor.")
            else:
                top_cats = df_plot["cat"].value_counts().head(15).index.tolist()
                df_plot = df_plot[df_plot["cat"].isin(top_cats)]
                if use_place:
                    df_plot["place"] = (df_plot["RANG"] <= 3).astype(int)
                    agg = df_plot.groupby("cat")["place"].mean().sort_values(ascending=False)
                    fig = px.bar(agg, x=agg.index, y=agg.values, title=f"Place Rate by {factor} (Top 15)")
                    fig.update_layout(xaxis_title=factor, yaxis_title="Place Rate", yaxis=dict(range=[0, 1]))
                else:
                    agg = df_plot.groupby("cat")["RANG"].mean().sort_values()
                    fig = px.bar(agg, x=agg.index, y=agg.values, title=f"Mean RANG by {factor} (Top 15)")
                st.plotly_chart(fig, width="stretch")
        else:
            x = _parse_numeric_series(data_df[factor])
            df_plot = pd.DataFrame({"x": x, "RANG": rang}).dropna()
            if df_plot.empty:
                st.info("Not enough data for this factor.")
            else:
                df_plot = df_plot.copy()
                try:
                    df_plot["bin"] = pd.qcut(df_plot["x"], q=10, duplicates="drop")
                    if use_place:
                        df_plot["place"] = (df_plot["RANG"] <= 3).astype(int)
                        agg = df_plot.groupby("bin")["place"].mean()
                        y_title = "Place Rate"
                    else:
                        agg = df_plot.groupby("bin")["RANG"].mean()
                        y_title = "Mean RANG"
                    bin_centers = [float(b.mid) for b in agg.index.categories]
                    fig = px.line(x=bin_centers, y=agg.values, markers=True, title=f"{y_title} vs {factor} (binned)")
                    st.plotly_chart(fig, width="stretch")
                except Exception:
                    fig = px.scatter(df_plot, x="x", y="RANG", opacity=0.3, title=f"RANG vs {factor}")
                    st.plotly_chart(fig, width="stretch")

st.markdown("**2D Outcome Heatmap**")
numeric_factors = [c for c in ["COTE", "POIDS", "N_WEIGHT", "CORDE", "AGE"] if c in data_df.columns]
if "RANG" not in data_df.columns or len(numeric_factors) < 2:
    st.info("Not enough numeric factors for 2D heatmap.")
else:
    fx = st.selectbox("X factor", numeric_factors, key="dash_heat_x")
    fy = st.selectbox("Y factor", [f for f in numeric_factors if f != fx], key="dash_heat_y")
    df_plot = data_df.copy()
    df_plot["x"] = _parse_numeric_series(df_plot[fx])
    df_plot["y"] = _parse_numeric_series(df_plot[fy])
    df_plot["RANG"] = pd.to_numeric(df_plot["RANG"], errors="coerce")
    df_plot = df_plot.dropna(subset=["x", "y", "RANG"])
    if df_plot.empty:
        st.info("Not enough data for heatmap.")
    else:
        df_plot["xbin"] = pd.qcut(df_plot["x"], q=6, duplicates="drop")
        df_plot["ybin"] = pd.qcut(df_plot["y"], q=6, duplicates="drop")
        df_plot["place"] = (df_plot["RANG"] <= 3).astype(int)
        if use_place:
            pivot = df_plot.groupby(["ybin", "xbin"])["place"].mean().unstack()
            title = "Place Rate Heatmap"
        else:
            pivot = df_plot.groupby(["ybin", "xbin"])["RANG"].mean().unstack()
            title = "Mean RANG Heatmap"
        # Convert Interval index/columns to strings for JSON serialization
        pivot_plot = pivot.copy()
        pivot_plot.index = pivot_plot.index.astype(str)
        pivot_plot.columns = pivot_plot.columns.astype(str)
        fig = px.imshow(pivot_plot, title=title, aspect="auto")
        st.plotly_chart(fig, width="stretch")

