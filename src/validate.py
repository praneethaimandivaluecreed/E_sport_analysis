
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ============================================================
# COMMON VALIDATION HELPERS
# ============================================================

def count_rows(df: DataFrame) -> int:
    """Return the number of rows in a DataFrame."""
    return df.count()


def find_duplicate_keys(df: DataFrame, key_columns: list) -> DataFrame:
    """
    Find duplicate business keys.

    This identifies duplicated key combinations but does not
    delete any rows.
    """
    duplicate_keys = (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
    )

    # Return every original row whose key is duplicated.
    return df.join(
        duplicate_keys.select(*key_columns),
        on=key_columns,
        how="inner"
    )


def find_missing_values(
    df: DataFrame,
    columns: list,
    dataset_name: str
) -> DataFrame:
    """
    Find rows where one or more selected columns are NULL,
    empty strings, or whitespace-only strings.
    """
    missing_condition = reduce(
        lambda condition, column: condition | (
            F.col(column).isNull()
            | (F.trim(F.col(column)) == "")
        ),
        columns,
        F.lit(False)
    )

    return (
        df.filter(missing_condition)
        .withColumn(
            "validation_issue",
            F.lit(f"MISSING_REQUIRED_VALUE: {dataset_name}")
        )
    )


def find_invalid_numeric_values(
    df: DataFrame,
    raw_column: str,
    parsed_column: str,
    allow_null: bool = False
) -> DataFrame:
    """
    Find non-empty source values that failed numeric parsing.

    This checks parsing only. Numeric business rules, such as
    negative kills, are checked separately.
    """
    raw_value = F.trim(F.col(raw_column))

    condition = (
        F.col(parsed_column).isNull()
        & F.col(raw_column).isNotNull()
        & (raw_value != "")
    )

    if allow_null:
        condition = (
            condition
            & (~raw_value.isin("unknown", "n/a", "-", "??"))
        )

    return (
        df.filter(condition)
        .withColumn(
            "validation_issue",
            F.lit(f"UNPARSEABLE_NUMERIC: {raw_column}")
        )
    )


def find_invalid_timestamp_values(
    df: DataFrame,
    raw_column: str,
    parsed_column: str
) -> DataFrame:
    """Find non-empty timestamp strings that failed parsing."""
    return (
        df.filter(
            F.col(parsed_column).isNull()
            & F.col(raw_column).isNotNull()
            & (F.trim(F.col(raw_column)) != "")
        )
        .withColumn(
            "validation_issue",
            F.lit(f"UNPARSEABLE_TIMESTAMP: {raw_column}")
        )
    )


def show_issue_summary(issue_frames: dict) -> None:
    """Print the number of rows flagged by each validation check."""
    print("\n" + "=" * 65)
    print("VALIDATION ISSUE SUMMARY")
    print("=" * 65)

    for issue_name, issue_df in issue_frames.items():
        print(f"{issue_name}: {issue_df.count()} rows flagged")


# ============================================================
# INDIVIDUAL DATASET VALIDATIONS
# ============================================================

def validate_teams(teams: DataFrame) -> dict:
    issues = {}

    # team_id should identify one team.
    issues["duplicate_team_ids"] = find_duplicate_keys(
        teams, ["team_id"]
    )

    # Missing coach is allowed; team identity fields are required.
    issues["missing_team_identity"] = find_missing_values(
        teams,
        ["team_id", "team_name"],
        "teams"
    )

    return issues


def validate_players(players: DataFrame) -> dict:
    issues = {}

    issues["duplicate_player_ids"] = find_duplicate_keys(
        players, ["player_id"]
    )

    issues["missing_player_identity"] = find_missing_values(
        players,
        ["player_id", "player_name"],
        "players"
    )

    # A player may have a missing role, but flag it for review.
    issues["missing_player_role"] = (
        players.filter(
            F.col("role").isNull() | (F.trim(F.col("role")) == "")
        )
        .withColumn(
            "validation_issue",
            F.lit("MISSING_PLAYER_ROLE")
        )
    )

    # A bad date should be flagged, not silently replaced.
    issues["invalid_player_join_date"] = (
        players.filter(F.col("parsed_join_date").isNull())
        .withColumn(
            "validation_issue",
            F.lit("INVALID_PLAYER_JOIN_DATE")
        )
    )

    return issues


def validate_matches(matches: DataFrame) -> dict:
    issues = {}

    issues["duplicate_match_ids"] = find_duplicate_keys(
        matches, ["match_id"]
    )

    issues["missing_match_identity"] = find_missing_values(
        matches,
        ["match_id", "team_a_id", "team_b_id", "game"],
        "matches"
    )

    issues["invalid_match_timestamps"] = (
        find_invalid_timestamp_values(
            matches,
            "match_time",
            "parsed_match_time"
        )
    )

    issues["invalid_team_a_scores"] = (
        find_invalid_numeric_values(
            matches, "team_a_score", "team_a_score_int"
        )
    )

    issues["invalid_team_b_scores"] = (
        find_invalid_numeric_values(
            matches, "team_b_score", "team_b_score_int"
        )
    )

    # A team cannot play against itself in a normal match.
    # This flags the row; it does not remove it.
    issues["same_team_on_both_sides"] = (
        matches.filter(
            F.col("team_a_id").isNotNull()
            & F.col("team_b_id").isNotNull()
            & (F.col("team_a_id") == F.col("team_b_id"))
        )
        .withColumn(
            "validation_issue",
            F.lit("SAME_TEAM_ON_BOTH_SIDES")
        )
    )

    # Negative scores are numeric but invalid under this rule.
    issues["negative_scores"] = (
        matches.filter(
            (F.col("team_a_score_int") < 0)
            | (F.col("team_b_score_int") < 0)
        )
        .withColumn(
            "validation_issue",
            F.lit("NEGATIVE_SCORE")
        )
    )

    return issues


def validate_player_match_stats(stats: DataFrame) -> dict:
    issues = {}

    issues["duplicate_stat_ids"] = find_duplicate_keys(
        stats, ["stat_id"]
    )

    # Expected grain: one row per player per match.
    issues["duplicate_player_match_combinations"] = (
        find_duplicate_keys(stats, ["match_id", "player_id"])
    )

    issues["missing_stat_identity"] = find_missing_values(
        stats,
        ["stat_id", "match_id", "player_id"],
        "player_match_stats"
    )

    numeric_columns = [
        ("kills", "kills_int"),
        ("deaths", "deaths_int"),
        ("assists", "assists_int"),
        ("damage", "damage_int"),
        ("rounds_played", "rounds_played_int"),
    ]

    # Keep parsing problems separate by source field.
    for raw_column, parsed_column in numeric_columns:
        issues[f"unparseable_{raw_column}"] = (
            find_invalid_numeric_values(
                stats, raw_column, parsed_column
            )
        )

    # A negative kill count is numeric but violates a business rule.
    issues["negative_kills"] = (
        stats.filter(F.col("kills_int") < 0)
        .withColumn(
            "validation_issue",
            F.lit("FLAGGED_NEGATIVE_KILLS")
        )
    )

    for column in ["deaths_int", "assists_int", "damage_int",
                   "rounds_played_int"]:
        issues[f"negative_{column}"] = (
            stats.filter(F.col(column) < 0)
            .withColumn(
                "validation_issue",
                F.lit(f"NEGATIVE_VALUE: {column}")
            )
        )

    return issues


def validate_match_events(events: DataFrame) -> dict:
    issues = {}

    issues["duplicate_event_ids"] = find_duplicate_keys(
        events, ["event_id"]
    )

    issues["missing_event_identity"] = find_missing_values(
        events,
        ["event_id", "match_id"],
        "match_events"
    )

    issues["invalid_event_timestamps"] = (
        find_invalid_timestamp_values(
            events,
            "event_time",
            "parsed_event_time"
        )
    )

    issues["unparseable_event_durations"] = (
        find_invalid_numeric_values(
            events,
            "duration_seconds",
            "duration_seconds_int"
        )
    )

    issues["negative_event_durations"] = (
        events.filter(F.col("duration_seconds_int") < 0)
        .withColumn(
            "validation_issue",
            F.lit("NEGATIVE_EVENT_DURATION")
        )
    )

    # Do not use event duration as the overall match duration.
    return issues


# ============================================================
# CROSS-FILE / REFERENTIAL VALIDATIONS
# ============================================================

def validate_references(datasets: dict) -> dict:
    """
    Check whether IDs in child datasets exist in parent datasets.

    These are reference checks only. They do not delete records.
    """
    teams = datasets["teams"]
    players = datasets["players"]
    matches = datasets["matches"]
    stats = datasets["player_match_stats"]
    events = datasets["match_events"]

    issues = {}

    # Players -> teams
    known_team_ids = teams.select("team_id").distinct()

    issues["players_with_unknown_team"] = (
        players.join(known_team_ids, on="team_id", how="left_anti")
        .withColumn(
            "validation_issue",
            F.lit("UNKNOWN_TEAM_ID")
        )
    )

    # Matches -> teams (check both participating team columns)
    issues["matches_with_unknown_team_a"] = (
        matches.select("match_id", "team_a_id")
        .join(
            known_team_ids,
            F.col("team_a_id") == F.col("team_id"),
            "left_anti"
        )
        .withColumn(
            "validation_issue",
            F.lit("UNKNOWN_TEAM_A_ID")
        )
    )

    issues["matches_with_unknown_team_b"] = (
        matches.select("match_id", "team_b_id")
        .join(
            known_team_ids,
            F.col("team_b_id") == F.col("team_id"),
            "left_anti"
        )
        .withColumn(
            "validation_issue",
            F.lit("UNKNOWN_TEAM_B_ID")
        )
    )

    # Stats -> players
    # PL999 should be unknown if it does not exist in players.csv.
    known_player_ids = players.select("player_id").distinct()

    issues["stats_with_unknown_player"] = (
        stats.join(known_player_ids, on="player_id", how="left_anti")
        .withColumn(
            "validation_issue",
            F.lit("UNKNOWN_PLAYER_ID")
        )
    )

    # Separate: a player exists but has a broken team reference.
    # This helps distinguish PL006 from an unknown ID like PL999.
    invalid_team_players = (
        players.join(
            known_team_ids,
            on="team_id",
            how="left_anti"
        )
        .select("player_id")
        .distinct()
    )

    issues["stats_for_players_with_invalid_team"] = (
        stats.join(
            invalid_team_players,
            on="player_id",
            how="inner"
        )
        .withColumn(
            "validation_issue",
            F.lit("PLAYER_HAS_INVALID_TEAM_REFERENCE")
        )
    )

    # Stats -> matches
    known_match_ids = matches.select("match_id").distinct()

    issues["stats_with_unknown_match"] = (
        stats.join(known_match_ids, on="match_id", how="left_anti")
        .withColumn(
            "validation_issue",
            F.lit("UNKNOWN_MATCH_ID")
        )
    )

    # Events -> matches
    issues["events_with_unknown_match"] = (
        events.join(known_match_ids, on="match_id", how="left_anti")
        .withColumn(
            "validation_issue",
            F.lit("UNKNOWN_MATCH_ID")
        )
    )

    return issues


# ============================================================
# RUN ALL VALIDATIONS
# ============================================================

def validate_all_datasets(datasets: dict) -> dict:
    """
    Run dataset-level and cross-file validations.

    Returns DataFrames containing flagged rows. Nothing is dropped.
    """
    all_issues = {}

    validation_functions = {
        "teams": validate_teams,
        "players": validate_players,
        "matches": validate_matches,
        "player_match_stats": validate_player_match_stats,
        "match_events": validate_match_events,
    }

    for dataset_name, validation_function in validation_functions.items():
        dataset_issues = validation_function(datasets[dataset_name])

        for issue_name, issue_df in dataset_issues.items():
            all_issues[issue_name] = issue_df

    reference_issues = validate_references(datasets)
    all_issues.update(reference_issues)

    return all_issues
