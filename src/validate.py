
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# Count the number of rows in a DataFrame
def count_rows(df):
    return df.count()


# Find rows with duplicate values in the given key columns
def find_duplicate_keys(df, key_columns):
    duplicate_keys = (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .drop("count")
    )

    # Return all rows that contain duplicate keys
    return df.join(duplicate_keys, on=key_columns, how="inner")


# Find rows with NULL, empty, or whitespace-only values
def find_missing_values(df, columns, dataset_name):
    missing_condition = None

    for column in columns:
        condition = (
            F.col(column).isNull()
            | (F.trim(F.col(column).cast("string")) == "")
        )

        if missing_condition is None:
            missing_condition = condition
        else:
            missing_condition = missing_condition | condition

    # Keep rows where at least one checked column is missing
    return (
        df.filter(missing_condition)
        .withColumn("validation_issue", F.lit(f"{dataset_name}: missing values"))
    )


# Find values that could not be converted to the expected numeric type
def find_invalid_numeric_values(
    df,
    raw_column,
    parsed_column,
    allow_null=False
):
    raw_value = F.trim(F.col(raw_column).cast("string"))

    # Ignore missing values because they are checked separately
    invalid_condition = (
        raw_value.isNotNull()
        & (raw_value != "")
        & F.col(parsed_column).isNull()
    )

    if allow_null:
        # Ignore common placeholders when NULL values are allowed
        placeholders = ["unknown", "n/a", "-", "??"]

        invalid_condition = invalid_condition & (
            F.lower(raw_value).isin(placeholders) == False
        )

    return (
        df.filter(invalid_condition)
        .withColumn(
            "validation_issue",
            F.lit(f"Invalid numeric value in {raw_column}")
        )
    )


# Find timestamps that could not be parsed
def find_invalid_timestamp_values(df, raw_column, parsed_column):
    raw_value = F.trim(F.col(raw_column).cast("string"))

    invalid_condition = (
        raw_value.isNotNull()
        & (raw_value != "")
        & F.col(parsed_column).isNull()
    )

    return (
        df.filter(invalid_condition)
        .withColumn(
            "validation_issue",
            F.lit(f"Invalid timestamp in {raw_column}")
        )
    )


# Display the number of rows found by each validation check
def show_issue_summary(issue_frames):
    print("\nVALIDATION SUMMARY")

    for issue_name, issue_df in issue_frames.items():
        print(f"{issue_name}: {issue_df.count()} issue rows")


# Validate teams.csv
def validate_teams(teams):
    issues = {}

    # Check for duplicate team IDs
    issues["duplicate_team_ids"] = find_duplicate_keys(
        teams, ["team_id"]
    )

    # Check required team information
    issues["missing_team_values"] = find_missing_values(
        teams,
        ["team_id", "team_name"],
        "teams"
    )

    return issues


# Validate players.csv
def validate_players(players):
    issues = {}

    # Check for duplicate player IDs
    issues["duplicate_player_ids"] = find_duplicate_keys(
        players, ["player_id"]
    )

    # Check required player information
    issues["missing_player_values"] = find_missing_values(
        players,
        ["player_id", "player_name"],
        "players"
    )

    # Check whether player roles are missing
    issues["missing_player_roles"] = find_missing_values(
        players,
        ["role"],
        "players"
    )

    # Check whether join dates could not be parsed
    issues["invalid_player_join_dates"] = (
        players.filter(
            F.col("join_date").isNotNull()
            & (F.trim(F.col("join_date").cast("string")) != "")
            & F.col("parsed_join_date").isNull()
        )
        .withColumn(
            "validation_issue",
            F.lit("Invalid player join date")
        )
    )

    return issues


# Validate matches.csv
def validate_matches(matches):
    issues = {}

    # Check for duplicate match IDs
    issues["duplicate_match_ids"] = find_duplicate_keys(
        matches, ["match_id"]
    )

    # Check required match information
    issues["missing_match_values"] = find_missing_values(
        matches,
        ["match_id", "team_a_id", "team_b_id", "game"],
        "matches"
    )

    # Check whether match times could not be parsed
    issues["invalid_match_times"] = find_invalid_timestamp_values(
        matches,
        "match_time",
        "parsed_match_time"
    )

    # Check whether either team's score could not be parsed
    issues["invalid_team_a_scores"] = find_invalid_numeric_values(
        matches,
        "team_a_score",
        "team_a_score_int"
    )

    issues["invalid_team_b_scores"] = find_invalid_numeric_values(
        matches,
        "team_b_score",
        "team_b_score_int"
    )

    # A team should not play against itself
    issues["same_team_in_match"] = (
        matches.filter(
            F.col("team_a_id").isNotNull()
            & F.col("team_b_id").isNotNull()
            & (F.col("team_a_id") == F.col("team_b_id"))
        )
        .withColumn(
            "validation_issue",
            F.lit("Both teams have the same team ID")
        )
    )

    # Scores cannot be negative
    issues["negative_team_a_scores"] = (
        matches.filter(F.col("team_a_score_int") < 0)
        .withColumn(
            "validation_issue",
            F.lit("Negative team A score")
        )
    )

    issues["negative_team_b_scores"] = (
        matches.filter(F.col("team_b_score_int") < 0)
        .withColumn(
            "validation_issue",
            F.lit("Negative team B score")
        )
    )

    return issues


# Validate player_match_stats.csv
def validate_player_match_stats(stats):
    issues = {}

    # Check for duplicate stat IDs
    issues["duplicate_stat_ids"] = find_duplicate_keys(
        stats, ["stat_id"]
    )

    # Each player should have at most one stats record per match
    issues["duplicate_player_match_stats"] = find_duplicate_keys(
        stats, ["match_id", "player_id"]
    )

    # Check required identifiers
    issues["missing_stats_keys"] = find_missing_values(
        stats,
        ["stat_id", "match_id", "player_id"],
        "player_match_stats"
    )

    # Check whether numeric statistics could not be parsed
    numeric_columns = [
        ("kills", "kills_int"),
        ("deaths", "deaths_int"),
        ("assists", "assists_int"),
        ("damage", "damage_int"),
        ("rounds_played", "rounds_played_int")
    ]

    for raw_column, parsed_column in numeric_columns:
        issues[f"invalid_{raw_column}"] = find_invalid_numeric_values(
            stats,
            raw_column,
            parsed_column
        )

    # Kills cannot be negative
    issues["negative_kills"] = (
        stats.filter(F.col("kills_int") < 0)
        .withColumn(
            "validation_issue",
            F.lit("Negative kills")
        )
    )

    # Other numeric statistics cannot be negative
    for column in [
        "deaths_int",
        "assists_int",
        "damage_int",
        "rounds_played_int"
    ]:
        issues[f"negative_{column}"] = (
            stats.filter(F.col(column) < 0)
            .withColumn(
                "validation_issue",
                F.lit(f"Negative value in {column}")
            )
        )

    return issues


# Validate match_events.csv
def validate_match_events(events):
    issues = {}

    # Check for duplicate event IDs
    issues["duplicate_event_ids"] = find_duplicate_keys(
        events, ["event_id"]
    )

    # Check required event identifiers
    issues["missing_event_keys"] = find_missing_values(
        events,
        ["event_id", "match_id"],
        "match_events"
    )

    # Check whether event timestamps could not be parsed.
    # Use the actual column names created by the transformation layer.
    issues["invalid_event_timestamps"] = find_invalid_timestamp_values(
        events,
        "event_time",
        "parsed_event_time"
    )

    # Check whether event durations could not be parsed.
    # The raw duration is duration_seconds and its parsed integer is
    # duration_seconds_int.
    issues["invalid_event_durations"] = find_invalid_numeric_values(
        events,
        "duration_seconds",
        "duration_seconds_int"
    )

    # Event duration cannot be negative
    issues["negative_event_durations"] = (
        events.filter(F.col("duration_seconds_int") < 0)
        .withColumn(
            "validation_issue",
            F.lit("Negative event duration")
        )
    )

    return issues


# Find non-empty IDs that do not exist in the parent dataset
def find_unknown_references(
    child_df,
    child_column,
    parent_df,
    parent_column,
    issue_name
):
    # Keep only valid parent IDs for matching
    parent_keys = (
        parent_df
        .select(F.col(parent_column).alias(child_column))
        .filter(
            F.col(child_column).isNotNull()
            & (F.trim(F.col(child_column).cast("string")) != "")
        )
        .distinct()
    )

    # Ignore missing child IDs because missing values are checked separately
    child_rows = child_df.filter(
        F.col(child_column).isNotNull()
        & (F.trim(F.col(child_column).cast("string")) != "")
    )

    # Find child rows whose IDs are absent from the parent dataset
    return (
        child_rows.join(
            parent_keys,
            on=child_column,
            how="left_anti"
        )
        .withColumn("validation_issue", F.lit(issue_name))
    )


# Validate relationships between the five datasets
def validate_references(datasets):
    teams = datasets["teams"]
    players = datasets["players"]
    matches = datasets["matches"]
    stats = datasets["player_match_stats"]
    events = datasets["match_events"]

    issues = {}

    # Check whether each player's team exists
    issues["players_unknown_teams"] = find_unknown_references(
        players,
        "team_id",
        teams,
        "team_id",
        "Player references an unknown team"
    )

    # Check whether team A exists for each match
    issues["matches_unknown_team_a"] = find_unknown_references(
        matches,
        "team_a_id",
        teams,
        "team_id",
        "Match references an unknown team A"
    )

    # Check whether team B exists for each match
    issues["matches_unknown_team_b"] = find_unknown_references(
        matches,
        "team_b_id",
        teams,
        "team_id",
        "Match references an unknown team B"
    )

    # Check whether each stats record references an existing player
    issues["stats_unknown_players"] = find_unknown_references(
        stats,
        "player_id",
        players,
        "player_id",
        "Stats reference an unknown player"
    )

    # Find stats records belonging to players with unknown team IDs
    invalid_team_players = (
        issues["players_unknown_teams"]
        .select("player_id")
        .distinct()
    )

    issues["stats_players_with_unknown_teams"] = (
        stats.join(
            invalid_team_players,
            on="player_id",
            how="inner"
        )
        .withColumn(
            "validation_issue",
            F.lit("Stats belong to a player with an unknown team")
        )
    )

    # Check whether each stats record references an existing match
    issues["stats_unknown_matches"] = find_unknown_references(
        stats,
        "match_id",
        matches,
        "match_id",
        "Stats reference an unknown match"
    )

    # Check whether each event references an existing match
    issues["events_unknown_matches"] = find_unknown_references(
        events,
        "match_id",
        matches,
        "match_id",
        "Event references an unknown match"
    )

    return issues


# Run all individual and cross-file validation checks
def validate_all_datasets(datasets):
    all_issues = {}

    # Validate each dataset independently
    all_issues.update(validate_teams(datasets["teams"]))
    all_issues.update(validate_players(datasets["players"]))
    all_issues.update(validate_matches(datasets["matches"]))
    all_issues.update(
        validate_player_match_stats(datasets["player_match_stats"])
    )
    all_issues.update(
        validate_match_events(datasets["match_events"])
    )

    # Validate relationships between datasets
    all_issues.update(validate_references(datasets))

    return all_issues
