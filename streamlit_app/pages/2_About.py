import streamlit as st

st.set_page_config(page_title="RaceX - About", page_icon="ℹ️", layout="wide")

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

st.title("ℹ️ About RaceX")

st.markdown(
    """
RaceX is an analysis tool for horse racing that combines data scraping, scoring, and
visual analysis to support betting and race review.

**How to use the app**
- **Scrape a meeting**: Pick a date, load meetings, select one, then click **Scrape & Analyse**.
- **Auto race type**: Race type is detected automatically and shown in the banner.
- **Filter by race**: Use the REF_COURSE selector to focus on a single race.
- **Composite scoring**: Adjust weights, review rankings, and cross‑check with odds.
- **Heatmap**: Compare normalized metrics across horses to spot strengths/weaknesses.
- **Monte Carlo**: Use the MC tools for win/top‑3 probabilities, odds vs model, and bet EV.
- **Exports**: Download CSVs, PDF reports, and heatmaps from the sidebar when available.

**Pages**
- **History & Analysis**: Distributions and bivariate outcomes from historical flat data.
- **Dictionary**: Explanations of racing, analysis, and ML concepts.
- **Insights & PMU**: Race types, track effects, and PMU betting system.

**Notes**
- Flat and trotting races use different metrics and scoring logic.
- Historical data uses the local `zone_turf_flat.db` database.

For questions or support, refer to the project documentation in the root folder.
"""
)
