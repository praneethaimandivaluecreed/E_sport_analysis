
# Esports Tournament Analytics

## 1. Project Overview

This project uses **Apache PySpark and Spark SQL** to analyse esports tournament data while identifying data-quality issues that could affect analytical results.

The pipeline covers ingestion, transformation, validation, cleaning, quarantine, analytics, and Parquet output generation across five datasets.

## 2. Technologies

- Python and PySpark
- Spark SQL
- CSV and Apache Parquet
- Pytest

## 3. Project Structure

```text
E_sport_analysis/
├── data/                  # Five input CSV files
├── src/
│   ├── ingest.py
│   ├── transform.py
│   ├── validate.py
│   ├── clean.py
│   ├── analytics.py
│   ├── sql_analytics.py
│   └── main.py
├── tests/
│   └── test_esport_project.py
├── logs/                  # Timestamped pipeline logs
└── outputs/
    ├── analytics/
    ├── cleaned/
    ├── quality_issues/
    ├── quarantined/
    └── sql_analytics/
```

## 4. Input Data

| Dataset | Initial rows | Purpose |
|---|---:|---|
| `teams.csv` | 8 | Team identities and details |
| `players.csv` | 80 | Players, roles, teams and statuses |
| `matches.csv` | 402 | Match details, scores and statuses |
| `player_match_stats.csv` | 1,803 | Player kills, deaths, assists, damage and rounds |
| `match_events.csv` | 1,000 | Match events, timestamps and durations |
| **Total** | **3,293** | |

These counts are from the pipeline log recorded on October 26, 2026.

## 5. Pipeline Workflow

The pipeline in `src/main.py` executes these stages:

1. **Ingestion:** Reads the five CSV datasets using explicit schemas.
2. **Transformation:** Trims text, standardises selected categories, parses timestamps and casts numeric values while retaining source values.
3. **Validation:** Checks duplicates, missing fields, invalid values, negative metrics and unmatched references.
4. **Cleaning and quarantine:** Separates records accepted by cleaning rules from records requiring investigation.
5. **DataFrame analytics:** Calculates team and player performance, event summaries and team form.
6. **Spark SQL:** Executes three analytical queries using temporary views.
7. **Output and read-back:** Saves Parquet datasets and checks that player-performance output can be read again.
8. **Cleanup:** Stops the Spark session.

## 6. Data Quality and Cleaning Results

The validation module checks duplicate IDs and player-match combinations, missing identities and roles, invalid timestamps, unparseable or negative numeric values, same-team matches, and unknown team, player or match references.

The pipeline log confirms that the corresponding quality-issue Parquet outputs were saved. Individual issue counts were not included in the supplied log, so they must be obtained from those output datasets before reporting exact counts.

### Cleaning summary

| Dataset | Input rows | Cleaned rows | Quarantined rows |
|---|---:|---:|---:|
| Teams | 8 | 8 | 0 |
| Players | 80 | 79 | 1 |
| Matches | 402 | 399 | 3 |
| Player-match statistics | 1,803 | 1,670 | 133 |
| Match events | 1,000 | 997 | 3 |
| **Total** | **3,293** | **3,153** | **140** |

The logged counts reconcile: 3,153 cleaned rows + 140 quarantined rows = 3,293 input rows.

Approximately 95.75% of input rows were retained in cleaned outputs. Player-match statistics account for 133 of the 140 quarantined rows, making this dataset a priority for reviewing data-quality rules.

Quarantined records are preserved with a `quarantine_reason`. Quality-issue outputs record validation findings, whereas quarantine outputs preserve records rejected by the cleaning rules. The exact reason counts should be confirmed from the generated outputs; issue categories can overlap and should not be summed as unique affected records.

## 7. Analytics and Window Functions

The reusable calculations in `src/analytics.py` include:

- **Team performance:** Represents both participating teams consistently, then calculates matches played, wins, losses, draws and win rate for eligible completed matches with usable scores.
- **Player performance:** Joins player statistics with player and match data, then calculates kills, deaths, assists, damage and per-round metrics. Rankings support comparisons, but should be interpreted within appropriate games or roles.
- **Event analysis:** Summarises events by game type, month, duration and team exposure. These patterns do not establish causation.
- **Team form:** Orders match results chronologically and calculates recent performance over the latest three matches.

Window calculations support player rankings and rolling team form without collapsing all records into one aggregate. Ordering, partitioning and tie-handling affect the results and should be reviewed in the implementation.

## 8. Spark SQL Analytics

`src/sql_analytics.py` registers temporary views and produces three SQL outputs:

1. `match_outcomes_by_game`
2. `event_intensity_by_game`
3. `role_performance_by_game`

The queries use joins, grouping, conditional aggregation and common table expressions. The match-outcomes SQL result is compared with a DataFrame result using `exceptAll` and an assertion. Agreement should be claimed only when the comparison passes.

## 9. Execution Plans and Performance

The pipeline calls `explain("formatted")` for team-performance and player-performance calculations.

Execution plans can reveal the physical join strategies, aggregation operators, exchanges/shuffles and repeated work selected by Spark. Joins, aggregations and window calculations may require data movement, depending on the plan and configuration.

**Before final submission:** inspect the actual formatted plans and document the operators observed. The supplied log confirms the analytics ran, but does not contain the plan text needed to substantiate specific join or shuffle claims.

## 10. Output Data and Why Parquet?

| Output directory | Contents |
|---|---|
| `outputs/cleaned/` | Records accepted by cleaning rules |
| `outputs/quarantined/` | Rejected records and quarantine reasons |
| `outputs/quality_issues/` | Validation findings |
| `outputs/analytics/` | DataFrame analytical results |
| `outputs/sql_analytics/` | Spark SQL results |

### Why Parquet suits this project

Parquet is a **columnar format** suited to analytical workloads. In this project it:

- Stores schema and typed values for downstream processing.
- Supports column pruning when queries need only selected fields.
- Supports compression, potentially reducing storage and read I/O.
- Allows cleaned and aggregated datasets to be reused without repeatedly parsing the source CSV files.

Parquet does not guarantee correct results or automatically eliminate expensive joins and shuffles. Actual performance depends on query patterns, data layout and execution plans.

Spark generally writes Parquet datasets as directories containing one or more `part-*.parquet` files. In the recorded run, the saved player-performance dataset was read back successfully with **304 rows**. This verifies basic read-back, not the correctness of every analytical metric.

The pipeline uses overwrite mode, so rerunning it replaces existing output data at the relevant destinations.

## 11. Logging Evidence

The recorded run shows that:

- All five input datasets were read and transformed without a row-count change during transformation.
- Validation and cleaning/quarantine stages completed.
- The DataFrame and SQL analytical outputs were saved.
- The player-performance Parquet output was read back with 304 rows.
- The pipeline completed and stopped the Spark session.

These observations demonstrate successful execution of the recorded run, but do not independently prove every business rule or metric is correct.

## 12. Running the Project

Run from the project root:

```bash
python src/main.py
```

To run the tests, when the dependencies are installed:

```bash
pytest
```

The pipeline expects the source CSV files under `data/` and writes logs and outputs within the project directory.

## 13. Assumptions and Limitations

- Failed timestamp and numeric conversions become null in derived columns and are subject to validation.
- Cleaning and quarantine decisions follow the rules implemented in `src/clean.py`.
- Team win rates depend on eligible match definitions and valid scores.
- Player metrics may not be directly comparable across games or roles.
- Event summaries identify patterns, not causal effects.
- Rolling team form depends on chronological ordering and tie-handling.
- Validation issue counts can overlap.
- Cleaned/quarantined count reconciliation does not alone prove correct classification.

Before submission, inspect individual quality-issue counts, quarantine reasons, actual execution plans, test results and analytical values. Document business findings only when supported by the generated outputs.
