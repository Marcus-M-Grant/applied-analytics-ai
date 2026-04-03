# Applied Analytics & AI

A collection of production-grade analytics and AI projects spanning player segmentation, LLM-powered reporting, economic modeling, NLP, and fraud detection. Built using Python, SQL, BigQuery, and Google Vertex AI.

---

## Projects

### 1. [Player Segmentation Pipeline](./01-player-segmentation/)
End-to-end ML segmentation pipeline built on BigQuery. Develops a K-Means baseline model using PCA dimensionality reduction across 14 behavioral features, applies it on a rolling 30-day basis, and tracks cluster migration over time via Sankey diagrams and transition matrices.

**Stack:** Python, scikit-learn, BigQuery, GCP, PCA, K-Means, Plotly

---

### 2. [AI-Powered Daily Reporting Agent](./02-ai-reporting-agent/)
Automated analytics agent that pulls daily KPIs from BigQuery, computes day-over-day and rolling period comparisons, and uses Gemini 2.5 Pro (Vertex AI) to generate a concise narrative summary — flagging anomalies and surfacing key movers without human intervention.

**Stack:** Python, BigQuery, Google Vertex AI, Gemini 2.5 Pro

---

### 3. [Token Economy Efficiency Model](./03-token-economy-efficiency/)
Multi-CTE BigQuery SQL model tracking cost-per-token-issued across all issuance channels (chests, dungeons, crafting, portals). Incorporates live token price feeds and produces 7-, 30-, and 90-day rolling efficiency metrics for economic health monitoring.

**Stack:** BigQuery SQL, CTEs, rolling window functions

---

### 4. [Community Sentiment Analysis](./04-sentiment-analysis/)
NLP pipeline that scrapes 24-hour community messages, cleans and chunks text, generates an abstractive summary via BART-large-cnn, and runs per-message sentiment classification to surface negative feedback themes for community and product teams.

**Stack:** Python, Hugging Face Transformers, BART, Discord API

---

### 5. [Fraud & Abuse Detection](./05-fraud-detection/)
Two-part anomaly detection suite: (1) statistical analysis of speed hack detection events across 750k+ telemetry records to profile cheat behavior distributions; (2) percentile-threshold detection of reward grant abuse with automated suspension duration recommendations exported to CSV.

**Stack:** Python, BigQuery, pandas, matplotlib

---

## Skills Demonstrated

| Area | Tools |
|---|---|
| Machine Learning | scikit-learn, K-Means, PCA, StandardScaler |
| Data Engineering | BigQuery, GCP, rolling pipelines |
| AI / LLM Integration | Vertex AI, Gemini 2.5 Pro, Hugging Face |
| NLP | BART, sentiment classification |
| SQL | Multi-CTE BigQuery, window functions |
| Visualization | Plotly, Sankey diagrams, matplotlib |
