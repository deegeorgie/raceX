import os
import streamlit as st

st.set_page_config(page_title="RaceX - Insights & PMU", page_icon="🧠", layout="wide")

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

st.title("🧠 Race Insights & PMU System")

img_path = os.path.join(os.path.dirname(__file__), "..", "assets", "obst.jpg")
st.image(img_path, caption="Obstacles (placeholder)", use_container_width=True)

st.markdown("**Race Types and Characteristics**")
st.markdown(
    """
- **Flat**: Speed and positioning dominate. Corde and pace bias can be decisive.
- **Trot (Attelé/Monté)**: Higher DQ risk, shoeing strategy matters, form stability is critical.
- **Obstacles**: Stamina and jumping technique, higher variance.
"""
)

st.markdown("**Distance and Track Effects**")
st.markdown(
    """
- **Short (<1600m)**: Speed and start are amplified. Low cordes often help.
- **Medium (1600–2400m)**: Balanced profile; class and fitness matter most.
- **Long (>=2400m)**: Stamina, consistency, and pace management dominate.
"""
)

st.markdown("**PMU Betting System (France)**")
st.markdown(
    """
PMU is a **pari-mutuel** system: payouts depend on the total pool and the number of winners.

Common bet types:
- **Simple Gagnant/Plac&eacute;**: Win / place.
- **Coupl&eacute;**: Pick top-2 (order or any order depending on option).
- **Tierc&eacute; / Quart&eacute;+ / Quint&eacute;+**: Pick top-3/4/5 (ordered or unordered variants).
- **2sur4**: Pick any 2 of the top 4.

Key implication: **odds are dynamic** and reflect market behavior, not fixed bookmaker prices.
"""
)

st.markdown("**Practical Tips**")
st.markdown(
    """
- Use **Composite + Odds** to find overpriced contenders.
- In trot, prioritize **DQ_Risk** and **recent form stability**.
- For large fields, prefer **top‑3 probability** over just win probability.
"""
)
