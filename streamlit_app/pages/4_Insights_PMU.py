import os
import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(page_title="RaceX - Insights & PMU", page_icon="ðŸ§ ", layout="wide")

st.markdown(
    """
    <style>
    [data-testid="stSidebarNav"] { display: none; }
    </style>
    """,
    unsafe_allow_html=True,
)

with st.sidebar:
    st.title("ðŸ‡ RaceX")
    st.markdown("---")
    st.subheader("Pages")
    st.page_link("app.py", label="Race Analysis", icon="ðŸ‡")
    st.page_link("pages/1_History_Analysis.py", label="History & Analysis", icon="ðŸ“Š")
    st.page_link("pages/3_Dictionary.py", label="Dictionary", icon="ðŸ“˜")
    st.page_link("pages/4_Insights_PMU.py", label="Insights & PMU", icon="ðŸ§ ")
    st.page_link("pages/2_About.py", label="About", icon="â„¹ï¸")

st.title("ðŸ§  Race Insights & PMU System")

img_path = os.path.join(os.path.dirname(__file__), "..", "assets", "obst.jpg")
st.image(img_path, caption="Obstacles (placeholder)", width="stretch")

st.markdown("**Race Types and Characteristics**")
st.markdown(
    """
- **Flat**: Speed and positioning dominate. Corde and pace bias can be decisive.
- **Trot (AttelÃ©/MontÃ©)**: Higher DQ risk, shoeing strategy matters, form stability is critical.
- **Obstacles**: Stamina and jumping technique, higher variance.
"""
)

st.markdown("**Distance and Track Effects**")
st.markdown(
    """
- **Short (<1600m)**: Speed and start are amplified. Low cordes often help.
- **Medium (1600â€“2400m)**: Balanced profile; class and fitness matter most.
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
- For large fields, prefer **topâ€‘3 probability** over just win probability.
"""
)

st.markdown("---")
st.header("Zone Turf Quinte du Jour")
st.markdown(
    """
If Zone Turf provides a JavaScript embed snippet for the Quinte du Jour module, it can be rendered below using Streamlit components.
Replace the placeholder HTML with the actual code from Zone Turf.
"""
)

zone_turf_quinte_widget = """
<div id=\"zone-turf-direct\">
  <script language=\"javascript\" type=\"text/javascript\" src=\"https://www.zone-turf.fr/module/module_webmaster.php?e=partants\"></script>
</div>
<div id=\"zone-turf-fallback\" style=\"display:none; padding-top: 16px;\">
  <div style=\"margin-bottom: 0.5rem; color: #555; font-size: 0.95rem;\">Direct script load failed â€” using iframe fallback.</div>
  <iframe width=\"100%\" height=\"700\" frameborder=\"0\" scrolling=\"no\"
    srcdoc=\"<html><body><script language='javascript' type='text/javascript' src='https://www.zone-turf.fr/module/module_webmaster.php?e=partants'></script></body></html>\">
  </iframe>
</div>
<script>
  window.setTimeout(function() {
    var direct = document.querySelector('#zone-turf-direct');
    if (direct && direct.innerHTML.trim() === '') {
      direct.style.display = 'none';
      var fallback = document.querySelector('#zone-turf-fallback');
      if (fallback) fallback.style.display = 'block';
    }
  }, 1200);
</script>
"""

components.html(zone_turf_quinte_widget, height=800, scrolling=True)

