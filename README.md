
# Esports Tournament Analytics

## 1. Project Overview

This project uses **Apache PySpark and Spark SQL** to analyse esports tournament data while identifying data-quality issues that could affect analytical results.

The pipeline covers ingestion, transformation, validation, cleaning, quarantine, analytics, and Parquet output generation across five datasets.

## 2. Technologies

- Python and PySpark
- Spark SQL
- Apache Parquet
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


## Data Quality Checks

### 1. Data Standardization and Transformation

- **Whitespace trimming:** Removes leading and trailing whitespace from all columns.
- **Blank-value standardization:** Converts empty and whitespace-only strings to `NULL`.
- **Category standardization:** Converts player roles, player statuses, match statuses, game names, and event types to lowercase.
- **Date parsing:** Converts player join dates into date values.
- **Timestamp parsing:** Parses match and event timestamps into timestamp values.
- **Numeric type conversion:** Uses `try_cast` to convert match scores, player statistics, and event durations into integer columns.
- **Invalid conversion handling:** Failed date, timestamp, or numeric conversions produce `NULL` in the parsed or converted columns.
- **Raw-value preservation:** Retains original columns alongside parsed or converted columns for investigation.

### 2. Dataset-Level Validation

#### Teams

- Detects duplicate `team_id` values.
- Identifies missing or blank `team_id` values.
- Identifies missing or blank `team_name` values.

#### Players

- Detects duplicate `player_id` values.
- Identifies missing or blank `player_id` values.
- Identifies missing or blank `player_name` values.
- Identifies missing player roles.
- Detects invalid player join dates.
- Checks for references to unknown teams.

#### Matches

- Detects duplicate `match_id` values.
- Identifies missing or blank `match_id`, `team_a_id`, `team_b_id`, and game values.
- Detects invalid match timestamps.
- Detects invalid numeric conversions for team scores.
- Identifies negative team scores.
- Detects matches where both participating team IDs are identical.
- Checks for references to unknown teams.

#### Player-Match Statistics

- Detects duplicate `stat_id` values.
- Detects duplicate `(match_id, player_id)` combinations.
- Identifies missing or blank `stat_id`, `match_id`, and `player_id` values.
- Detects invalid numeric conversions for `kills`, `deaths`, `assists`, `damage`, and `rounds_played`.
- Identifies negative values in these five statistical fields.
- Checks for references to unknown players and matches.
- Identifies statistics associated with players whose team references are unknown.

#### Match Events

- Detects duplicate `event_id` values.
- Identifies missing or blank `event_id` and `match_id` values.
- Detects invalid event timestamps.
- Detects invalid numeric conversions for event durations.
- Identifies negative event durations.
- Checks for references to unknown matches.

### 3. Cross-File Referential Integrity

- **Players → Teams:** Checks whether each player's team reference exists.
- **Matches → Teams:** Checks whether both participating team references exist.
- **Player Statistics → Players:** Checks whether referenced players exist.
- **Player Statistics → Matches:** Checks whether referenced matches exist.
- **Player Statistics → Team Assignments:** Checks for statistics associated with players whose team references are unknown.
- **Match Events → Matches:** Checks whether each event references an existing match.

Left anti joins are used to identify unmatched references across related datasets.

### 4. Data Cleaning and Quarantine

- **Missing required fields:** Quarantines records with missing or blank identifiers and other required fields covered by the cleaning rules.
- **Exact duplicate removal:** Retains one copy of an exact duplicate record and quarantines excess identical copies.
- **Duplicate player-match combinations:** Quarantines all records belonging to duplicated `(match_id, player_id)` groups.
- **Unknown references:** Quarantines records that reference unknown teams, players, or matches, according to the implemented cleaning rules.
- **Rejected parent references:** Distinguishes references to records rejected during upstream cleaning from references to IDs that are entirely unknown.
- **Invalid match relationships:** Quarantines matches where both participating team IDs are identical.
- **Quarantine reasons:** Adds a reason to quarantined records to support investigation and traceability.
- **Dependency-aware cleaning:** Processes datasets in the order Teams → Players → Matches → Player Statistics → Match Events, allowing downstream checks to use cleaned parent datasets.

### 5. Validation and Quarantine Scope

Validation findings and quarantine rules are not identical. The validation functions detect issues, while the cleaning functions quarantine records only when an explicit cleaning rule is implemented.

- Invalid timestamps, unparseable numeric values, and negative scores, statistics, or durations are detected by validation but are not explicitly quarantined by the cleaning functions shown.
- Duplicate-key validation does not mean every record with a duplicated key is automatically quarantined. General duplicate removal handles exact duplicate rows, while duplicate player-match combinations have a separate quarantine rule.
- Missing `event_type` is handled by event cleaning, although the supplied event validation function does not explicitly check it.


### 6.3 Quarantine Summary

A total of **140 records were quarantined** across four datasets:

- Players: 1 record
- Matches: 3 records
- Player statistics: 133 records
- Events: 3 records

Teams had no quarantined records.

Quarantined records are preserved with a `quarantine_reason`. Quality-issue outputs record validation findings, while quarantine outputs preserve records rejected by the cleaning rules.

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

