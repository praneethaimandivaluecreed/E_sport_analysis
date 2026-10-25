# Esports Tournament Analytics

## 1. Project Overview

This project analyses esports tournament data using  Apache PySpark, and Spark SQL. The objective is to produce reliable team and player performance metrics while identifying data-quality problems that may affect the interpretation of tournament results.

The project works with five datasets covering teams, players, matches, player-match statistics, and match events. It implements data ingestion, transformation, validation, cleaning, quarantine handling, analytical processing, and Parquet output generation.

### Objectives

- Profile the quality and consistency of the supplied datasets.
- Identify duplicate records, invalid values, missing fields, and unmatched references.
- Preserve questionable records for investigation through quality-issue and quarantine outputs.
- Calculate team and player performance metrics.
- Analyse match events and team performance over time.
- Use Spark SQL and DataFrame transformations for analytical questions.
- Apply window functions for ranking and rolling performance calculations.
- Inspect Spark execution plans and save analytical results in Parquet format.

## 2. Technologies Used

- Python
- Apache PySpark
- Spark SQL
- CSV
- Apache Parquet
- Pytest


## 3. Project Structure

```text
E_sport_analysis/
│
├── data/
│   ├── teams.csv
│   ├── players.csv
│   ├── matches.csv
│   ├── player_match_stats.csv
│   └── match_events.csv
│
├── src/
│   ├── ingest.py
│   ├── transform.py
│   ├── validate.py
│   ├── clean.py
│   ├── analytics.py
│   ├── sql_analytics.py
│   └── main.py
│
├── tests/
│   └── test_esport_project.py
│
├── outputs/
│   ├── analytics/
│   ├── cleaned/
│   ├── quality_issues/
│   ├── quarantined/
│   └── sql_analytics/
│
|
|
├── tests/
└
```

## 4. Input Datasets

| Dataset | Description |
|---|---|
| `teams.csv` | Team identities, names, regions, and coaches |
| `players.csv` | Player identities, team references, roles, join dates, and statuses |
| `matches.csv` | Match timestamps, games, participating teams, scores, and match statuses |
| `player_match_stats.csv` | Player-level kills, deaths, assists, damage, and rounds played |
| `match_events.csv` | Match-linked events, timestamps, event types, and durations |

The initial dataset sizes observed during ingestion were:

| Dataset | Rows |
|---|---:|
| Teams | 8 |
| Players | 80 |
| Matches | 402 |
| Player-match statistics | 1,803 |
| Match events | 1,000 |

## 5. Pipeline Architecture

The main pipeline is implemented in `src/main.py`.

The workflow follows these stages:

1. **Ingestion:** Load the five CSV datasets using PySpark.
2. **Transformation:** Standardise selected text fields and create parsed timestamp and numeric columns.
3. **Validation:** Identify data-quality issues and unmatched relationships.
4. **Cleaning and quarantine:** Separate records that satisfy the cleaning rules from records requiring investigation.
5. **DataFrame analytics:** Calculate team performance, player performance, event summaries, and team form over time.
6. **Spark SQL analytics:** Execute analytical queries using temporary views.
7. **Output generation:** Save quality issues, cleaned datasets, quarantined records, and analytical results as Parquet.
8. **Resource cleanup:** Stop the Spark session when pipeline execution finishes.

## 6. Ingestion and Transformation

### Ingestion

The `src/ingest.py` module defines explicit schemas for the five datasets and provides reusable functions for reading the CSV files.

The input columns are initially represented as strings. This preserves the original source values so malformed timestamps and numeric values can be investigated rather than being silently converted during ingestion.

The ingestion process checks that the expected files exist before reading them.

### Transformation

The `src/transform.py` module performs standardisation and type preparation.

The transformations include:

- Trimming text values.
- Converting blank strings to null values.
- Standardising selected category fields, such as roles, statuses, games, and event types.
- Parsing player join dates and match/event timestamps.
- Creating integer-cast columns for scores and player/event metrics.
- Retaining original columns alongside parsed or cast columns.

Invalid conversions can produce null values in the derived columns. The original source values remain available for validation and investigation.

## 7. Data Quality Validation

The `src/validate.py` module checks data quality before cleaning.

The checks cover:

- Duplicate team, player, match, statistic, and event identifiers.
- Missing required values and keys.
- Missing player roles.
- Invalid timestamps and numeric values.
- Negative values where non-negative metrics are expected.
- Teams appearing on both sides of a match.
- Unknown team, player, and match references.
- Duplicate player-match statistic records.

The `validate_all_datasets()` function coordinates the validation checks, while `show_issue_summary()` displays the resulting issue counts.

An issue count does not necessarily represent the number of unique affected records. A single record can violate multiple rules, and duplicate-key checks can identify multiple rows associated with a duplicated key.

## 8. Cleaning and Quarantine

The `src/clean.py` module contains dataset-specific cleaning functions and reusable helpers for identifying records that should not proceed into the relevant cleaned dataset.

The cleaning process addresses:

- Exact duplicate records.
- Missing required identity fields.
- Invalid records according to the defined cleaning rules.
- Unknown references between related datasets.

Quarantined records receive a `quarantine_reason` describing why they were rejected.

This approach makes questionable records available for review instead of silently discarding them. The quality-issue outputs and quarantine outputs serve different purposes: quality outputs document validation findings, while quarantine outputs preserve records rejected by cleaning rules.

## 9. DataFrame Analytics

The `src/analytics.py` module contains reusable functions for the main analytical calculations.

### Team Performance

Completed matches with usable scores are represented from each participating team's perspective.

Team A and Team B records are combined into a consistent team-level representation. The resulting aggregation calculates:

- Matches played.
- Wins.
- Losses.
- Draws.
- Win-rate percentage.

Representing both sides consistently allows team results to be aggregated without treating Team A and Team B differently.

### Player Performance

Player statistics are joined with match and player information. The analysis filters to completed matches and aggregates statistics by player and game.

Metrics include:

- Total kills.
- Total deaths.
- Total assists.
- Total damage.
- Kills per round.
- Assists per round.
- Damage per round.
- Deaths per round.

Per-round metrics help account for differences in the number of rounds played. They should still be interpreted within an appropriate game or role because scoring systems and gameplay differ between games.

The module also ranks players using performance metrics. Deaths per round are ranked in ascending order so lower values receive better ranks.

### Match Event Analysis

Event analysis summarises match events using several perspectives:

- Events by game type.
- Events by month.
- Event durations.
- Event exposure by team.

These summaries help identify patterns in the available event records. They do not, by themselves, establish that an event caused a particular match result.

### Team Form Over Time

The project calculates chronological team records and a rolling win rate based on each team's most recent three matches.

This provides a view of recent team performance in addition to overall win rate. The interpretation depends on consistent chronological ordering and the handling of matches with identical timestamps.

## 10. Window Functions

Window functions are used to calculate rankings and time-based performance measures.

The player-performance analysis includes rankings for kills per round, assists per round, damage per round, and deaths per round.

The team-form analysis uses chronological window calculations to track match sequence and rolling performance over the most recent three matches.

These calculations allow comparisons across player groups and changes in team performance over time without collapsing all records into a single overall aggregate.

## 11. Spark SQL Analytics

The `src/sql_analytics.py` module registers temporary views for the teams, players, matches, player-match statistics, and match-event datasets.

It implements three Spark SQL analyses:

1. Match outcomes by game.
2. Event intensity by game.
3. Role performance by game.

The SQL queries use operations such as joins, common table expressions, conditional aggregation, grouping, and ordering.

For the match-outcomes analysis, the project compares the SQL result against a corresponding DataFrame-based calculation using `exceptAll` and an assertion.

The comparison must pass during execution before the SQL/DataFrame agreement can be reported as verified.

## 12. Execution Plans and Performance

The project calls `explain("formatted")` for two substantial DataFrame analytics:

- Team performance.
- Player performance.

These execution plans help inspect the physical operations selected by Spark.

Joins, aggregations, and window calculations can introduce data exchanges or shuffle stages. The actual operators and shuffle behaviour depend on the query plan, input data, Spark configuration, and available statistics.

The execution plans are printed during execution. They should be reviewed to explain the selected join strategies, aggregation stages, exchanges, and any repeated work.

## 13. Output Datasets

The pipeline writes outputs beneath the `outputs/` directory.

| Directory | Purpose |
|---|---|
| `outputs/quality_issues/` | Validation issue datasets |
| `outputs/cleaned/` | Cleaned datasets |
| `outputs/quarantined/` | Rejected records and quarantine reasons |
| `outputs/analytics/` | DataFrame-based analytical datasets |
| `outputs/sql_analytics/` | Spark SQL analytical datasets |

The analytical outputs include team performance, player performance, team form over time, and event summaries.

The pipeline uses Parquet output with overwrite mode. Running the pipeline again replaces output data at the corresponding destinations.

### Why Parquet?

Parquet is a columnar storage format suitable for analytical workloads. It stores schema information and supports efficient access to selected columns, making it useful for repeatedly querying cleaned and aggregated datasets without reparsing the original CSV files.

Spark generally writes Parquet datasets as directories containing one or more part files rather than as a single standalone file.

## 14. Running the Project

Run the commands from the project root directory.

### Execute the pipeline

```bash
python src/main.py
```

The script resolves the project root and expects the source CSV files in the `data/` directory. Generated outputs are written under `outputs/`.

If working from inside the `src/` directory, the equivalent command is:

```bash
python main.py
```

## 15. Assumptions and Limitations

- Original source columns are retained alongside derived parsed or cast columns where applicable.
- Failed numeric or timestamp conversions are represented as null in the derived columns and are subject to validation.
- Cleaning and quarantine decisions are based on explicit rules implemented in the cleaning module.
- Team win-rate calculations depend on the definition of eligible completed matches and valid scores.
- Player metrics should be compared within relevant game or role groups where scoring systems differ.
- Event summaries identify patterns but do not establish causation.
- Rolling team performance depends on the chronological ordering of match records.
- Validation issue counts may overlap and should not be summed as though every issue represents a different record.

