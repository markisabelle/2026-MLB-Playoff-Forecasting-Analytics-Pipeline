# 2026-MLB-Playoff-Forecasting-Analytics-Pipeline
From MLB API data to automated forecasts, simulations, and an interactive Power BI dashboard.

# ⚾ MLB Data Pipeline & Postseason Monte Carlo Simulator

An end-to-end data engineering, statistical forecasting, and interactive analytics pipeline built in **Azure Databricks** and **Power BI**. The system ingests raw data from the **MLB Stats API**, processes it through a Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold), simulates remaining schedule outcomes **10,000 times** using a Python Monte Carlo engine, and visualizes executive-level insights in Power BI.

---

## 📌 Executive Summary & Core Objective

Static standings only reflect past results. To answer the question **"Given where every MLB team stands right now, what could the rest of the season look like?"**, this system automates end-to-end ingestion, cleanses raw API payloads, models floor/median/ceiling win projections, and dynamically evaluates postseason odds.

* **Portfolio Case Study:** [View Live Portfolio Write-Up](https://markisabelle.carrd.co/)

---

## 🛠 Tech Stack & Tools

* **Cloud Data Platform:** Azure Databricks (Serverless Compute)
* **Languages & Engines:** Python, PySpark, SQL, DAX
* **Data Architecture:** Medallion Architecture (Bronze / Silver / Gold Delta Lakes)
* **Modeling & Simulation:** Python (Monte Carlo simulation running 10,000 season iterations), Pythagorean Expected Win %
* **Business Intelligence:** Power BI (Desktop & Service)

---

## 🏗 End-to-End System Architecture

                              +---------------------+
                              |    MLB Stats API    |
                              +----------+----------+
                                         |
                                         v
                       +-----------------------------------+
                       |           BRONZE LAYER            |
                       | Raw JSON Ingestion (Serverless)   |
                       +-----------------+-----------------+
                                         |
                                         v
                       +-----------------------------------+
                       |           SILVER LAYER            |
                       | Structured & Cleaned (PySpark)    |
                       +-----------------+-----------------+
                                         |
                                         v
                       +-----------------------------------+
                       |            GOLD LAYER             |
                       | Analytical Models & Star Schema   |
                       +--------+-----------------+--------+
                                |                 |
                                v                 v
             +----------------------+   +-----------------------+
             |  SQL FORECASTING     |   | PYTHON MONTE CARLO    |
             |  Pythagorean Win %   |   | 10,000 Simulations    |
             +----------+-----------+   +-----------+-----------+
                        |                           |
                        +-------------+-------------+
                                      |
                                      v
                       +-----------------------------------+
                       |         POWER BI DASHBOARD        |
                       | Macro View & Micro Team Outlooks  |
                       +-----------------------------------+


---

## 📂 Pipeline Layers & Data Model

The pipeline processes data incrementally into structured Delta Lake tables:

1. **Bronze Layer:** Raw API responses stored in JSON format without loss of metadata to ensure source-level traceability.
2. **Silver Layer:** Cleaned, typed, and deduplicated tables. Standardizes rosters, schedules, box scores, and game PKs.
3. **Gold Layer:** Business-ready dimensional star schema serving analytical reporting and mathematical models:
   * **Fact Tables:** `Gold_Fact_Batting`, `Gold_Fact_Pitching`
   * **Dimension Tables:** `Gold_Dim_Players`, `Gold_Dim_Games`, `Gold_Dim_Players_History`, `Gold_Dim_Teams`
   * **Analytical Aggregates:** `Records_Standings`, `statistical_leaders`, `Record_Predictions`

### 📊 Dataset Volume Metrics

| Dataset / Metric | Record Count / Volume |
| :--- | :--- |
| **Games** | `2,069` |
| **Players** | `1,419` |
| **Batting Records** | `43,849` |
| **Pitching Records** | `17,554` |
| **Boxscore Records** | `5,551` |
| **Distinct Game PKs** | `2,457` |
| **Daily Pipeline Volume** | `434,696 rows written` |

---

## 🔍 Key Data Engineering Callout: Root-Cause Data Quality Bug

During model validation, discrepancies surfaced between actual schedule totals and downstream analytical tables. 

* **The Problem:** Specific game records were being silently dropped during transformations, distorting downstream standings and simulation accuracy.
* **The Solution:** Traced missing game identifiers back through Silver joins to API ingestion edge-cases. Refactored join strategies and validation logic, recovering dozens of omitted games across the dataset.
* **Key Insight:** *A sophisticated predictive model is only as accurate as the data pipeline feeding it.*

---

## 🎲 Monte Carlo Simulation Engine

The forecasting engine simulates every remaining game on the schedule **10,000 times** based on team strength, run differentials (Pythagorean expectations), schedule strength, and historical win probabilities.

```python
num_simulations = 10000
all_simulations = []

for sim in range(num_simulations):
    sim_records = copy.deepcopy(starting_records)
    for game_number, game in enumerate(games, start=1):
        away_prob = game["away_probability"]
        
        # Simulate individual game outcome
        if random.random() < away_prob:
            winner, loser = game["away_team"], game["home_team"]
        else:
            winner, loser = game["home_team"], game["away_team"]
            
        sim_records[winner]["wins"] += 1
        sim_records[loser]["losses"] += 1

    for team, record in sim_records.items():
        all_simulations.append({
            "simulation": sim + 1,
            "team": team,
            "wins": record["wins"],
            "losses": record["losses"]
        })
