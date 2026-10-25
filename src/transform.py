
"""
transform.py

Purpose:
    Standardize source values and create safely parsed columns.

Core principles:
    1. Keep original columns when parsing uncertain values.
    2. Do not silently drop records.
    3. Do not replace invalid values with invented values.
    4. Leave quality decisions and quarantine handling
       to the validation/cleaning stages.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ---------------------------------------------------------
# 1. REUSABLE TEXT CLEANING
# ---------------------------------------------------------

def trim_text_columns(
    df: DataFrame,
    columns: list[str],
) -> DataFrame:
    """
    Remove leading and trailing spaces from selected text columns.

    Blank strings become NULL so that missing-value checks
    can treat blanks consistently.

    Original nonblank text is preserved apart from whitespace.
    """

    for column in columns:
        df = df.withColumn(
            column,
            F.when(
                F.trim(F.col(column)) == "",
                F.lit(None).cast("string"),
            ).otherwise(F.trim(F.col(column)))
        )

    return df


def standardize_category(
    df: DataFrame,
    column: str,
) -> DataFrame:
    """
    Standardize a category by trimming spaces and using lowercase.

    Example:
        'Active', ' active ' and 'ACTIVE' become 'active'.

    This does not decide whether the resulting category is valid.
    That check belongs in validate.py.
    """

    return df.withColumn(
        column,
        F.when(
            F.col(column).isNull(),
            F.lit(None).cast("string"),
        ).otherwise(F.lower(F.trim(F.col(column))))
    )


# ---------------------------------------------------------
# 2. TEAMS
# ---------------------------------------------------------

def transform_teams(df: DataFrame) -> DataFrame:
    """
    Clean team text fields.

    A missing coach is allowed to remain NULL.
    We do not invent a coach name or drop the team.
    """

    return trim_text_columns(
        df,
        ["team_id", "team_name", "region", "coach"],
    )


# ---------------------------------------------------------
# 3. PLAYERS
# ---------------------------------------------------------

def transform_players(df: DataFrame) -> DataFrame:
    """
    Standardize player fields and parse join_date.

    The original join_date string is retained.
    parsed_join_date contains the parsed date or NULL
    when the source value cannot be parsed.
    """

    df = trim_text_columns(
        df,
        ["player_id", "player_name", "team_id",
         "role", "join_date", "status"],
    )

    df = standardize_category(df, "role")
    df = standardize_category(df, "status")

    # The expected source format is yyyy-MM-dd.
    # try_to_timestamp returns NULL instead of failing the
    # transformation for malformed timestamp/date strings.
    df = df.withColumn(
        "parsed_join_date",
        F.to_date(
            F.try_to_timestamp(
                F.col("join_date"),
                F.lit("yyyy-MM-dd"),
            )
        )
    )

    return df


# ---------------------------------------------------------
# 4. MATCHES
# ---------------------------------------------------------

def transform_matches(df: DataFrame) -> DataFrame:
    """
    Standardize match status, parse match time and parse scores.

    Raw score and timestamp columns are retained for auditing.
    Parsed columns are used later for validation and analysis.
    """

    df = trim_text_columns(
        df,
        [
            "match_id", "match_time", "game",
            "team_a_id", "team_b_id",
            "team_a_score", "team_b_score",
            "match_status",
        ],
    )

    # Use a consistent internal name regardless of the
    # original source header.
    df = df.withColumnRenamed("match_status", "status")

    df = standardize_category(df, "status")
    df = standardize_category(df, "game")

    # Parse the expected timestamp format safely.
    df = df.withColumn(
        "parsed_match_time",
        F.try_to_timestamp(
            F.col("match_time"),
            F.lit("yyyy-MM-dd HH:mm:ss"),
        )
    )

    # Parse scores as integers.
    # Values such as 'unknown', 'N/A' and '??' become NULL.
    # The raw string columns remain available for investigation.
    df = df.withColumn(
        "team_a_score_int",
        F.expr("try_cast(team_a_score AS INT)"),
    )

    df = df.withColumn(
        "team_b_score_int",
        F.expr("try_cast(team_b_score AS INT)"),
    )

    return df


# ---------------------------------------------------------
# 5. PLAYER-MATCH STATISTICS
# ---------------------------------------------------------

def transform_player_match_stats(df: DataFrame) -> DataFrame:
    """
    Parse player performance metrics into numeric columns.

    Raw source columns are retained.
    Invalid strings become NULL in the parsed columns.
    Negative values are not automatically removed here;
    validation will determine which values are implausible.
    """

    df = trim_text_columns(
        df,
        [
            "stat_id", "match_id", "player_id",
            "kills", "deaths", "assists",
            "damage", "rounds_played",
        ],
    )

    # try_cast safely returns NULL for non-numeric strings.
    # It also avoids failing the whole job because of a
    # placeholder such as 'unknown' or 'lots'.
    numeric_columns = {
        "kills": "kills_int",
        "deaths": "deaths_int",
        "assists": "assists_int",
        "damage": "damage_int",
        "rounds_played": "rounds_played_int",
    }

    for source_column, parsed_column in numeric_columns.items():
        df = df.withColumn(
            parsed_column,
            F.expr(f"try_cast(`{source_column}` AS INT)"),
        )

    return df


# ---------------------------------------------------------
# 6. MATCH EVENTS
# ---------------------------------------------------------

def transform_match_events(df: DataFrame) -> DataFrame:
    """
    Standardize event type and safely parse event fields.

    Invalid event times and durations become NULL in their
    parsed columns; their raw source values are preserved.
    """

    df = trim_text_columns(
        df,
        [
            "event_id", "match_id", "event_time",
            "event_type", "duration_seconds",
        ],
    )

    df = standardize_category(df, "event_type")

    df = df.withColumn(
        "parsed_event_time",
        F.try_to_timestamp(
            F.col("event_time"),
            F.lit("yyyy-MM-dd HH:mm:ss"),
        )
    )

    df = df.withColumn(
        "duration_seconds_int",
        F.expr("try_cast(duration_seconds AS INT)"),
    )

    return df


# ---------------------------------------------------------
# 7. TRANSFORM ALL DATASETS
# ---------------------------------------------------------

def transform_all_datasets(
    datasets: dict[str, DataFrame],
) -> dict[str, DataFrame]:
    """
    Apply the appropriate transformations to each source dataset.

    The input and output are dictionaries keyed by dataset name.
    This keeps the pipeline easy to extend and maintain.
    """

    transformed = {
        "teams": transform_teams(datasets["teams"]),
        "players": transform_players(datasets["players"]),
        "matches": transform_matches(datasets["matches"]),
        "player_match_stats": transform_player_match_stats(
            datasets["player_match_stats"]
        ),
        "match_events": transform_match_events(
            datasets["match_events"]
        ),
    }

    return transformed
