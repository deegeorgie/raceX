# RaceX Streamlit App

Web version of the RaceX desktop app with the same analysis features.

## Setup

```bash
cd streamlit_app
pip install -r requirements.txt
```

## Run

```bash
# From the project root (zone_turf/)
streamlit run streamlit_app/app.py
```

## Features

| Tab | Trot | Flat |
|-----|------|------|
| Raw Data | ✅ | ✅ |
| Composite Score | ✅ | ✅ |
| Heatmap | ✅ | ✅ |
| Dashboard & History | — | ✅ |
| Summary & Prognosis | ✅ | — |
| Fitness (FA/FM / IF) | ✅ | ✅ |
| Performance (S_COEFF / IC) | ✅ | ✅ |
| Form Trend | ✅ | — |
| Shoeing Strategy | ✅ | — |
| Disqualification Risk | ✅ | — |
| Trending Odds | ✅ | ✅ |
| Favorable Cordes | ✅ | ✅ |
| Weight Stability | — | ✅ |
| Light Weight Surprise | — | ✅ |
| Prognosis + Outsiders | — | ✅ |
| Bet Generator | ✅ | ✅ |
| Export (CSV/Heatmap PNG/PDF) | ✅ | ✅ |
| Metric Weights UI | ✅ | ✅ |

## Notes

- All business logic is imported directly from the parent `zone_turf/` modules — no duplication.
- The app must be run from the `zone_turf/` root so Python can resolve the sibling imports.
- PDF export includes the heatmap and flat-race analysis (CSV downloads cover tabular data).
