import os
import streamlit as st

st.set_page_config(page_title="RaceX - Dictionary", page_icon="📘", layout="wide")

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

st.title("📘 Racing, Data, and ML Dictionary")

img_path = os.path.join(os.path.dirname(__file__), "..", "assets", "vincennes.jpg")
st.image(img_path, caption="Vincennes (placeholder)", use_container_width=True)

st.markdown("**Racing Concepts**")
st.markdown(
    """
- **Hippodrome**: The racetrack (venue). Track shape and surface affect outcomes.
- **Corde**: Starting post / stall. Low cordes can be advantageous on tight tracks.
- **Cote (Odds)**: Market price of a horse. Lower odds imply higher perceived chances.
- **Musique**: Recent form string (sequence of finishes, DQ, etc.).
- **REF_COURSE**: Race identifier (used to group runners by race).
- **RANG**: Final finishing position (1 = winner).
"""
)

st.markdown("**Race Types**")
st.markdown(
    """
- **Flat**: Gallop races without obstacles.
- **Trot**: Harness or mounted racing; disqualification risk is higher.
- **Obstacles**: Jump races (haies/steeple/cross), different rhythm and risk profile.
"""
)

st.markdown("**Core Metrics (RaceX)**")
st.markdown(
    """
- **IF**: Fitness indicator derived from recent form.
- **IC**: Class indicator (e.g., weight vs odds).
- **S_COEFF**: Success coefficient from performance strings.
- **n_weight**: Weight variation vs previous weight.
- **VALEUR**: Official rating / ability.
- **DQ_Risk**: Disqualification risk (trot).
"""
)

st.markdown("**Data & ML Concepts**")
st.markdown(
    """
- **Normalization**: Transforming values to a common scale (0–1).
- **Correlation**: Measures how strongly two variables move together.
- **Monte Carlo**: Random simulation to estimate probabilities.
- **Overfitting**: A model fits noise rather than signal.
- **Calibration**: Whether predicted probabilities match observed outcomes.
"""
)
