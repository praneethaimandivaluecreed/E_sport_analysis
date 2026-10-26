
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

| Dataset | Initial Rows | Purpose |
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

## 6. Data Quality and Cleaning

The pipeline applies validation and cleaning rules before producing analytics outputs. Invalid records are quarantined rather than silently included in the cleaned datasets.

### 6.1 Record Reconciliation

| Dataset | Raw Records | Clean Records | Quarantined Records |
|---|---:|---:|---:|
| Teams | 8 | 8 | 0 |
| Players | 80 | 79 | 1 |
| Matches | 402 | 399 | 3 |
| Player Statistics | 1,803 | 1,670 | 133 |
| Events | 1,000 | 997 | 3 |
| **Total** | **3,293** | **3,153** | **140** |

**Reconciliation:** 3,293 raw records = 3,153 clean records + 140 quarantined records.

Approximately **95.75%** of the input records were retained in the cleaned outputs.

Player-match statistics account for 133 of the 140 quarantined records, making this dataset a priority for reviewing the implemented data-quality rules.

### 6.2 Quality Checks Performed

- **Missing and blank values:** Required identifiers and fields are checked for null or blank values. Text values are trimmed, and blank strings are converted to null where applicable.
- **Duplicate records:** Duplicate records, identifiers and player-match combinations are identified and handled according to the implemented dataset-specific cleaning rules.
- **Reference integrity:** Player team IDs and match team IDs are validated against the cleaned teams reference data. Player statistics are checked against valid match references.
- **Match validation:** Matches are checked for missing identifiers, identical team references, and completed-match status where required by the relevant analytics.
- **Type and value validation:** Match timestamps and numeric scores are parsed into appropriate types. Player statistics are converted to integers, with negative or otherwise invalid metrics excluded from relevant calculations.
- **Event validation:** Event fields and duration values are checked before event-level analytics are calculated.

### 6.3 Quarantine Summary

A total of **140 records were quarantined** across four datasets:

- Players: 1 record
- Matches: 3 records
- Player statistics: 133 records
- Events: 3 records

Teams had no quarantined records.

Quarantined records are preserved with a `quarantine_reason`. Quality-issue outputs record validation findings, while quarantine outputs preserve records rejected by the cleaning rules.

The supplied run log confirms dataset-level quarantine counts but does not provide a breakdown by individual rejection reason. Exact reason-level counts should be reported only after inspecting the generated quarantine or quality-report outputs.

Quality-issue categories may overlap, so their counts should not automatically be summed as unique affected records.

The row-count reconciliation confirms that the input rows are accounted for, but does not by itself prove that every record was classified correctly.

## 7. Analytics and Window Functions

The reusable calculations in `src/analytics.py` include:

- **Team performance:** Represents both participating teams consistently, then calculates matches played, wins, losses, draws and win rate for eligible completed matches with usable scores.
- **Player performance:** Joins player statistics with player and match data, then calculates kills, deaths, assists, damage and per-round metrics. Rankings support comparisons within games; comparisons across games or roles may not be directly equivalent.
- **Event analysis:** Summarises events by game type, month, duration and team exposure. These patterns do not establish causation.
- **Team form:** Orders match results chronologically and calculates recent performance over a rolling window of the latest three matches.

Window calculations support player rankings and rolling team form without collapsing all records into a single aggregate. Partitioning, ordering and tie-handling affect the results and should be reviewed in the implementation.

## 8. Spark SQL Analytics

`src/sql_analytics.py` registers temporary views and produces three SQL outputs:

1. `match_outcomes_by_game`
2. `event_intensity_by_game`
3. `role_performance_by_game`

The queries use joins, grouping, conditional aggregation and common table expressions.

The match-outcomes SQL result is compared with a DataFrame result using `exceptAll` and an assertion. The recorded run reported that the SQL and DataFrame match-outcome counts agreed.

## 9. Spark Execution Plan Analysis

The pipeline uses `explain("formatted")` to inspect Spark's physical execution plan and understand how transformations, joins, aggregations and window functions are executed.

The observations below describe operators present in the supplied formatted plan. They do not, by themselves, prove runtime performance improvements.

### 9.1 Broadcast Hash Joins

The execution plan contains several `BroadcastHashJoin` operators.

Examples include joining match team IDs with known team IDs from `teams.csv`, joining player statistics with valid match IDs, and enriching player metrics with player details.

- `BroadcastExchange` prepares the smaller side of a join for broadcasting.
- `BroadcastHashJoin` uses the broadcast hash table to match keys without requiring both join sides to be shuffled for that join.
- `Inner` joins retain matching records only.
- A `LeftOuter` join can preserve left-side performance records when corresponding player details are missing.

**Why it matters:** Broadcasting a suitably small reference dataset can reduce join-related shuffle traffic. The observed plan shows that Spark selected broadcast joins for these operations.

### 9.2 Exchanges and Aggregations

The plan contains `Exchange` operators with hash partitioning, for example:

```text
hashpartitioning(player_id, game, 200)
```

These exchanges redistribute records across partitions so records with the same grouping keys can be processed together.

The plan also contains `HashAggregate` operators. For player performance, Spark performs partial aggregation and then final aggregation after the exchange.

**Why it matters:** Partial aggregation can reduce the amount of data transferred between partitions. Exchanges still introduce shuffle overhead, so they are important operators to inspect when diagnosing performance.

### 9.3 Window Functions and Ranking

The player-performance plan contains `Sort` and `Window` operators using `dense_rank()` partitioned by game.

The pipeline calculates rankings for:

- Kills per round
- Assists per round
- Damage per round
- Deaths per round, ordered ascending

Spark sorts data according to the window partitioning and ordering requirements before calculating ranks. Depending on the data and plan, these operations can require redistribution and sorting and may become expensive at larger scale.

### 9.4 Adaptive Query Execution

The plan contains an `AdaptiveSparkPlan` operator, indicating that Spark's Adaptive Query Execution (AQE) framework is involved.

AQE can revise parts of a physical plan using runtime statistics when supported and applicable.

The supplied plan reports `isFinalPlan=false`, so this output should not be described as the final, runtime-adapted plan.

### 9.5 CSV Scans and Data Preparation

The plan shows `Scan csv`, `Filter` and `Project` operators.

These represent reading CSV data, filtering records according to conditions, and selecting or transforming columns. The plan includes operations such as trimming strings, normalising game and status values, parsing timestamps, and casting scores to integers.

This shows that data-preparation operations are represented in Spark's execution plan rather than being performed solely through manual row-by-row processing.

## 10. Analytics Outputs

The pipeline generates the following DataFrame-based analytics:

| Output | Description |
|---|---|
| `team_performance` | Matches played, wins, losses, draws and win rate |
| `player_performance` | Aggregated player statistics, per-round metrics and game-specific rankings |
| `events_by_game_type` | Event summaries by game type |
| `events_by_month` | Event summaries by month |
| `event_durations` | Valid duration counts, average duration and maximum duration |
| `event_exposure_by_team` | Event exposure summarised by team |
| `team_form_over_time` | Match sequence and rolling win rate |

The pipeline also produces three Spark SQL analytics outputs:

1. `match_outcomes_by_game` — match outcomes grouped by game.
2. `event_intensity_by_game` — event counts and average event intensity by game.
3. `role_performance_by_game` — player performance summarised by role and game.

All listed analytics outputs are written to Parquet under `outputs/analytics/` or `outputs/sql_analytics/`.

## 11. Output Validation and Results

The recorded pipeline run completed successfully.

- **Data quality reconciliation:** 3,293 input records were accounted for as 3,153 clean records and 140 quarantined records.
- **SQL/DataFrame consistency:** The run reported that SQL and DataFrame match-outcome counts agreed.
- **Parquet read-back:** The saved player-performance output was read back successfully with 304 rows and the expected schema.
- **Output format:** Analytics results were saved as Parquet, preserving column data types and supporting column-oriented analytical reads.

The 304-row read-back confirms that the player-performance output could be read successfully. It does not independently prove the correctness of every analytical metric.

## 12. Why Parquet?

Parquet is suitable for these outputs because the pipeline produces structured analytical datasets that may be read repeatedly.

- **Columnar storage:** Queries can read required columns without processing every column.
- **Typed schema:** Numeric, string and ranking columns retain their data types.
- **Compression:** Columnar encoding and compression can reduce storage requirements.
- **Reusable outputs:** Downstream analytics can read cleaned and aggregated datasets without repeatedly parsing the original CSV files.

Parquet does not guarantee correct results or automatically eliminate expensive joins and shuffles. Actual performance depends on query patterns, data layout and execution plans.

Spark generally writes Parquet datasets as directories containing one or more `part-*.parquet` files.

The pipeline uses overwrite mode, so rerunning it replaces existing data at the relevant output destinations.

## 13. Logging and Reliability

The pipeline uses logging and staged processing to make execution easier to monitor and troubleshoot.

The recorded run shows that:

- All five input datasets were read and transformed without a row-count change during transformation.
- Validation and cleaning/quarantine stages completed.
- The DataFrame and SQL analytical outputs were saved.
- The player-performance Parquet output was read back successfully with 304 rows.
- The pipeline reached its completion message and stopped the Spark session.

These observations demonstrate successful execution of the recorded run, but do not independently prove every business rule or metric is correct.

## 14. Running the Project

Run from the project root:

```bash
python src/main.py
```

To run the tests, when the dependencies are installed:

```bash
pytest
```

The pipeline expects the source CSV files under `data/` and writes logs and outputs within the project directory.

## 15. Assumptions and Limitations

- Failed timestamp and numeric conversions become null in derived columns and are subject to validation.
- Cleaning and quarantine decisions follow the rules implemented in `src/clean.py`.
- Team win rates depend on eligible match definitions and valid scores.
- Player metrics may not be directly comparable across games or roles.
- Event summaries identify patterns, not causal effects.
- Rolling team form depends on chronological ordering and tie-handling.
- Validation issue counts can overlap.
- Cleaned/quarantined count reconciliation does not alone prove correct classification.
- The supplied execution-plan output shows `isFinalPlan=false`; therefore, it is not evidence of the final AQE-adapted plan.
- The run log does not provide individual quarantine-reason counts or prove every metric independently.

Before final submission, inspect individual quality-issue counts, quarantine reasons, actual execution plans, test results and analytical values. Document business findings only when supported by the generated outputs.
