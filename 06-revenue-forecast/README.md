# Revenue Forecast: Model Ensemble

A three-model ensemble revenue forecast projecting monthly topline revenue — New Premium, Renewal Premium, and Fee Revenue — across a 36-month horizon. Built as a take-home analytics exercise for an insurance company.

---

## Objective

Forecast three revenue streams 3 years out using independent modeling approaches, then combine them into a weighted ensemble to reduce model-specific bias and produce more robust predictions.

---

## Approach

Three models were built in parallel, each capturing different dynamics:

| Model | Type | Strengths |
|---|---|---|
| **Prophet** | Bayesian additive regression | Changepoint detection, strong seasonality modeling |
| **STL + LSTM** | Decomposition + neural network | Learns complex residual patterns after trend/seasonal removal |
| **SARIMA** | Classical statistical (ARIMAX) | Established econometric baseline, interpretable orders |

Each model was trained on a series-specific window to exclude startup-ramp distortion. A 12-month holdout was used to compute validation MAPEs for SARIMA and STL+LSTM. The final ensemble uses **equal weighting (1/3 per model)** — a well-established robust baseline recommended by the M4 forecasting competition when model quality is uncertain and a fair holdout comparison is not feasible across all models.

Fee Revenue was handled separately via a **fee rate model** (fees / total_premium), isolating the client's pricing policy from premium volume dynamics before forecasting.

---

## Files

| File | Description |
|---|---|
| `revenue_forecast_ensemble.ipynb` | Full annotated notebook: data prep, all three models, ensemble, and summary tables |
| `revenue_forecast_ensemble.html` | Rendered HTML version for browser viewing (outputs included) |

> **Note:** The source dataset is proprietary and not included in this repository.

---

## Stack

**Python** — pandas, NumPy, PyTorch, statsmodels, pmdarima, Prophet, scikit-learn, matplotlib
