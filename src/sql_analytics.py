
"""
sql_analytics.py
----------------
Purpose:
    Perform additional esports tournament analyses using Spark SQL.

Analyses:
    1. Match outcomes by game.
    2. Event intensity per match by game.
    3. Player-role performance by game.
    4. Validate SQL results against a DataFrame calculation.

Assumptions:
    - Input DataFrames have already passed through the cleaning pipeline.
    - Only completed matches are used for player performance.
    - Event duration_seconds represents event duration, NOT match duration.
    - Player performance is normalized by rounds played where possible.
    - Missing or invalid metrics are not silently replaced with zero.
"""

from pyspark.sql import DataFrame, SparkSession, functions as F


# ============================================================
# 1. REGISTER TEMPORARY VIEWS
# ============================================================

def register_sql_views(cleaned: dict) -> None:
    """
    Register cleaned DataFrames as Spark SQL temporary views.

    Expected dictionary keys:
        teams, players, matches, player_match_stats, match_events
    """

    view_mapping = {
        "teams": "teams",
        "players": "players",
        "matches": "matches",
        "player_match_stats": "player_match_stats",
        "match_events": "match_events",
    }

    for dataframe_key, view_name in view_mapping.items():
        if dataframe_key not in cleaned:
            raise KeyError(
                f"Missing '{dataframe_key}' in the cleaned DataFrames."
            )

        cleaned[dataframe_key].createOrReplaceTempView(view_name)

    print("Spark SQL temporary views registered successfully.")


# ============================================================
# 2. MATCH OUTCOMES BY GAME
# ============================================================

def analyze_match_outcomes_by_game(spark: SparkSession) -> DataFrame:
    """
    Compare completion, cancellation, and forfeit rates by game.

    Denominator:
        All retained cleaned matches for each game.

    This analysis includes cancelled and forfeited matches rather
    than restricting the analysis to completed matches.
    """

    query = """
        SELECT
            game,

            COUNT(*) AS total_matches,

            SUM(
                CASE WHEN status = 'completed'
                     THEN 1 ELSE 0 END
            ) AS completed_matches,

            SUM(
                CASE WHEN status = 'cancelled'
                     THEN 1 ELSE 0 END
            ) AS cancelled_matches,

            SUM(
                CASE WHEN status = 'forfeit'
                     THEN 1 ELSE 0 END
            ) AS forfeited_matches,

            ROUND(
                100.0 * SUM(
                    CASE WHEN status = 'completed'
                         THEN 1 ELSE 0 END
                ) / COUNT(*),
                2
            ) AS completion_rate_pct,

            ROUND(
                100.0 * SUM(
                    CASE WHEN status = 'cancelled'
                         THEN 1 ELSE 0 END
                ) / COUNT(*),
                2
            ) AS cancellation_rate_pct,

            ROUND(
                100.0 * SUM(
                    CASE WHEN status = 'forfeit'
                         THEN 1 ELSE 0 END
                ) / COUNT(*),
                2
            ) AS forfeit_rate_pct

        FROM matches
        GROUP BY game
        ORDER BY cancellation_rate_pct DESC
    """

    return spark.sql(query)


# ============================================================
# 3. EVENT INTENSITY PER MATCH BY GAME
# ============================================================

def analyze_event_intensity_by_game(spark: SparkSession) -> DataFrame:
    """
    Measure event intensity for each game.

    Method:
        1. Count valid event records for each match.
        2. Left join those counts to cleaned matches.
        3. Treat a match with no recorded events as zero events.
        4. Calculate average events per retained match.

    Event records are counted, regardless of event duration.
    """

    query = """
        WITH event_counts AS (
            SELECT
                match_id,
                COUNT(*) AS event_count
            FROM match_events
            WHERE match_id IS NOT NULL
            GROUP BY match_id
        ),

        match_event_counts AS (
            SELECT
                m.match_id,
                m.game,
                m.status,
                COALESCE(e.event_count, 0) AS event_count
            FROM matches m
            LEFT JOIN event_counts e
                ON m.match_id = e.match_id
        )

        SELECT
            game,

            COUNT(*) AS total_matches,

            SUM(event_count) AS total_events,

            ROUND(
                AVG(event_count),
                2
            ) AS avg_events_per_match,

            SUM(
                CASE WHEN event_count = 0
                     THEN 1 ELSE 0 END
            ) AS matches_without_events,

            ROUND(
                100.0 * SUM(
                    CASE WHEN event_count = 0
                         THEN 1 ELSE 0 END
                ) / COUNT(*),
                2
            ) AS pct_matches_without_events

        FROM match_event_counts
        GROUP BY game
        ORDER BY avg_events_per_match DESC
    """

    return spark.sql(query)


# ============================================================
# 4. PLAYER-ROLE PERFORMANCE BY GAME
# ============================================================

def analyze_role_performance_by_game(spark: SparkSession) -> DataFrame:
    """
    Compare player roles within each game.

    Rules:
        - Only completed matches are included.
        - Negative metric values are excluded from their respective
          metric calculations.
        - Per-round rates require rounds_played_int > 0.
        - A missing metric remains missing; it is not changed to zero.
        - Counts describe eligible player-match rows, not unique players.

    Expected parsed columns in player_match_stats:
        kills_int, assists_int, deaths_int,
        damage_int, rounds_played_int

    Expected player column:
        role
    """

    query = """
        WITH eligible_stats AS (
            SELECT
                s.match_id,
                s.player_id,
                p.role,
                m.game,

                s.kills_int,
                s.assists_int,
                s.deaths_int,
                s.damage_int,
                s.rounds_played_int

            FROM player_match_stats s

            INNER JOIN matches m
                ON s.match_id = m.match_id

            INNER JOIN players p
                ON s.player_id = p.player_id

            WHERE m.status = 'completed'
              AND p.role IS NOT NULL
        )

        SELECT
            game,
            role,

            COUNT(*) AS player_match_rows,

            COUNT(DISTINCT player_id) AS unique_players,

            COUNT(
                CASE
                    WHEN kills_int >= 0 THEN 1
                END
            ) AS valid_kill_records,

            ROUND(
                AVG(
                    CASE
                        WHEN kills_int >= 0
                         AND rounds_played_int > 0
                        THEN 1.0 * kills_int / rounds_played_int
                    END
                ),
                3
            ) AS avg_kills_per_round,

            ROUND(
                AVG(
                    CASE
                        WHEN assists_int >= 0
                         AND rounds_played_int > 0
                        THEN 1.0 * assists_int / rounds_played_int
                    END
                ),
                3
            ) AS avg_assists_per_round,

            ROUND(
                AVG(
                    CASE
                        WHEN deaths_int >= 0
                         AND rounds_played_int > 0
                        THEN 1.0 * deaths_int / rounds_played_int
                    END
                ),
                3
            ) AS avg_deaths_per_round,

            ROUND(
                AVG(
                    CASE
                        WHEN damage_int >= 0
                         AND rounds_played_int > 0
                        THEN 1.0 * damage_int / rounds_played_int
                    END
                ),
                2
            ) AS avg_damage_per_round,

            ROUND(
                AVG(
                    CASE
                        WHEN kills_int >= 0
                         AND deaths_int >= 0
                        THEN 1.0 * kills_int / (deaths_int + 1)
                    END
                ),
                3
            ) AS avg_kills_per_death_adjusted

        FROM eligible_stats
        GROUP BY game, role
        ORDER BY game, avg_damage_per_round DESC
    """

    return spark.sql(query)


# ============================================================
# 5. SQL VS DATAFRAME CONSISTENCY CHECK
# ============================================================

def validate_match_outcomes_sql_vs_dataframe(
    cleaned: dict,
    sql_result: DataFrame,
) -> bool:
    """
    Recalculate match outcome counts with DataFrame operations.

    Compare the result against the SQL counts.
    This validates the main counts, not every percentage column.
    """

    matches = cleaned["matches"]

    dataframe_result = (
        matches
        .groupBy("game")
        .agg(
            F.count("*").alias("total_matches"),

            F.sum(
                F.when(F.col("status") == "completed", 1)
                .otherwise(0)
            ).alias("completed_matches"),

            F.sum(
                F.when(F.col("status") == "cancelled", 1)
                .otherwise(0)
            ).alias("cancelled_matches"),

            F.sum(
                F.when(F.col("status") == "forfeit", 1)
                .otherwise(0)
            ).alias("forfeited_matches"),
        )
    )

    comparison_columns = [
        "game",
        "total_matches",
        "completed_matches",
        "cancelled_matches",
        "forfeited_matches",
    ]

    sql_counts = sql_result.select(*comparison_columns)
    dataframe_counts = dataframe_result.select(*comparison_columns)

    # exceptAll checks row-level differences while preserving duplicates.
    sql_only = sql_counts.exceptAll(dataframe_counts)
    dataframe_only = dataframe_counts.exceptAll(sql_counts)

    passed = sql_only.count() == 0 and dataframe_only.count() == 0

    if passed:
        print("PASS: SQL and DataFrame match outcome counts agree.")
    else:
        print("FAIL: SQL and DataFrame match outcome counts differ.")
        print("Rows found only in SQL:")
        sql_only.show(truncate=False)
        print("Rows found only in DataFrame results:")
        dataframe_only.show(truncate=False)

    return passed


# ============================================================
# 6. RUN ALL SQL ANALYSES
# ============================================================

def run_sql_analytics(
    spark: SparkSession,
    cleaned: dict,
) -> dict:
    """
    Register views and run all SQL analyses.

    Returns a dictionary of result DataFrames so that the caller
    can display, test, or write them to Parquet.
    """

    register_sql_views(cleaned)

    results = {
        "match_outcomes_by_game":
            analyze_match_outcomes_by_game(spark),

        "event_intensity_by_game":
            analyze_event_intensity_by_game(spark),

        "role_performance_by_game":
            analyze_role_performance_by_game(spark),
    }

    return results


# ============================================================
# 7. DISPLAY RESULTS
# ============================================================

def display_sql_analytics(
    spark: SparkSession,
    cleaned: dict,
) -> dict:
    """
    Run and display all SQL analyses.
    """

    results = run_sql_analytics(spark, cleaned)

    for analysis_name, result_df in results.items():
        print("\n" + "=" * 70)
        print(analysis_name.upper().replace("_", " "))
        print("=" * 70)

        result_df.show(100, truncate=False)

    parity_passed = validate_match_outcomes_sql_vs_dataframe(
        cleaned,
        results["match_outcomes_by_game"],
    )

    if not parity_passed:
        raise AssertionError(
            "SQL and DataFrame match outcome counts do not agree."
        )

    return results
