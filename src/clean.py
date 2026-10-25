
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ============================================================
# 1. COMMON HELPER FUNCTIONS
# ============================================================

def add_quarantine_reason(df: DataFrame, reason: str) -> DataFrame:
    """
    Add a column explaining why a record was quarantined.

    We preserve the original columns so that rejected records
    can still be investigated later.
    """
    return df.withColumn(
        "quarantine_reason",
        F.lit(reason)
    )


def split_exact_duplicates(
    df: DataFrame,
    reason: str = "EXACT_DUPLICATE_ROW"
):
    """
    Keep one copy of each exact row and quarantine excess copies.

    Example:
        A, B, C
        A, B, C
        D, E, F

    Cleaned:     A, B, C and D, E, F
    Quarantined: one excess copy of A, B, C
    """
    cleaned_df = df.dropDuplicates()

    duplicate_rows = df.exceptAll(cleaned_df)

    duplicate_rows = add_quarantine_reason(
        duplicate_rows,
        reason
    )

    return cleaned_df, duplicate_rows


def union_quarantines(dataframes, schema_df: DataFrame) -> DataFrame:
    """
    Combine quarantine DataFrames while allowing different
    datasets to have different columns.

    If no records were quarantined, return an empty DataFrame
    with the original dataset columns plus quarantine_reason.
    """
    valid_dataframes = [
        df for df in dataframes
        if df is not None
    ]

    if not valid_dataframes:
        return add_quarantine_reason(
            schema_df.limit(0),
            "NO_ISSUE"
        )

    result = valid_dataframes[0]

    for df in valid_dataframes[1:]:
        result = result.unionByName(
            df,
            allowMissingColumns=True
        )

    return result


def missing_identity_condition(df: DataFrame, columns: list) :
    """
    Return a condition identifying rows with missing identity
    fields. Empty strings and NULL values both count as missing.
    """
    condition = None

    for column_name in columns:
        current_condition = (
            F.col(column_name).isNull()
            | (F.trim(F.col(column_name)) == "")
        )

        if condition is None:
            condition = current_condition
        else:
            condition = condition | current_condition

    return condition


def split_missing_identity(
    df: DataFrame,
    identity_columns: list,
    reason: str
):
    """Separate rows with missing identity from usable rows."""
    condition = missing_identity_condition(
        df,
        identity_columns
    )

    quarantined = add_quarantine_reason(
        df.filter(condition),
        reason
    )

    cleaned = df.filter(~condition)

    return cleaned, quarantined


def split_unknown_references(
    df: DataFrame,
    reference_column: str,
    known_ids: DataFrame,
    known_id_column: str,
    reason: str
):
    """
    Quarantine references that do not exist in the supplied
    reference dataset.

    The reference IDs should be distinct before joining.
    """
    reference_lookup = (
        known_ids
        .select(
            F.col(known_id_column).alias("_known_reference_id")
        )
        .filter(F.col("_known_reference_id").isNotNull())
        .distinct()
    )

    joined = df.join(
        reference_lookup,
        df[reference_column] == F.col("_known_reference_id"),
        "left"
    )

    unknown_rows = joined.filter(
        F.col(reference_column).isNull()
        | F.col("_known_reference_id").isNull()
    ).drop("_known_reference_id")

    valid_rows = joined.filter(
        F.col(reference_column).isNotNull()
        & F.col("_known_reference_id").isNotNull()
    ).drop("_known_reference_id")

    return (
        valid_rows,
        add_quarantine_reason(unknown_rows, reason)
    )


# ============================================================
# 2. CLEAN TEAMS
# ============================================================

def clean_teams(teams: DataFrame) -> dict:
    """
    Rules:
    - Team ID and team name are required.
    - Exact duplicate rows are reduced to one copy.
    - A missing coach is allowed; it does not invalidate a team.
    """

    working_df, missing_identity = split_missing_identity(
        teams,
        ["team_id", "team_name"],
        "MISSING_TEAM_IDENTITY"
    )

    cleaned, exact_duplicates = split_exact_duplicates(
        working_df,
        "EXACT_DUPLICATE_TEAM_ROW"
    )

    quarantined = union_quarantines(
        [missing_identity, exact_duplicates],
        teams
    )

    return {
        "cleaned": cleaned,
        "quarantined": quarantined
    }


# ============================================================
# 3. CLEAN PLAYERS
# ============================================================

def clean_players(
    players: DataFrame,
    teams: DataFrame
) -> dict:
    """
    Rules:
    - Player ID and player name are required.
    - Exact duplicate rows are reduced to one copy.
    - Team IDs must exist in the cleaned teams dataset.
    - A missing role is allowed and preserved.
    """

    working_df, missing_identity = split_missing_identity(
        players,
        ["player_id", "player_name"],
        "MISSING_PLAYER_IDENTITY"
    )

    working_df, exact_duplicates = split_exact_duplicates(
        working_df,
        "EXACT_DUPLICATE_PLAYER_ROW"
    )

    working_df, unknown_teams = split_unknown_references(
        working_df,
        "team_id",
        teams,
        "team_id",
        "UNKNOWN_TEAM_ID"
    )

    quarantined = union_quarantines(
        [
            missing_identity,
            exact_duplicates,
            unknown_teams
        ],
        players
    )

    return {
        "cleaned": working_df,
        "quarantined": quarantined
    }


# ============================================================
# 4. CLEAN MATCHES
# ============================================================

def clean_matches(
    matches: DataFrame,
    teams: DataFrame
) -> dict:
    """
    Rules:
    - Match ID, team A ID and team B ID are required.
    - Exact duplicate rows are reduced to one copy.
    - Both team IDs must exist in the cleaned teams dataset.
    - A team cannot play against itself.
    - Invalid timestamps and scores remain available for review.
      Their parsed columns can be NULL without silently dropping
      the original match record.
    """

    working_df, missing_identity = split_missing_identity(
        matches,
        ["match_id", "team_a_id", "team_b_id"],
        "MISSING_MATCH_IDENTITY"
    )

    working_df, exact_duplicates = split_exact_duplicates(
        working_df,
        "EXACT_DUPLICATE_MATCH_ROW"
    )

    working_df, unknown_team_a = split_unknown_references(
        working_df,
        "team_a_id",
        teams,
        "team_id",
        "UNKNOWN_TEAM_A_ID"
    )

    working_df, unknown_team_b = split_unknown_references(
        working_df,
        "team_b_id",
        teams,
        "team_id",
        "UNKNOWN_TEAM_B_ID"
    )

    same_team_condition = (
        F.col("team_a_id") == F.col("team_b_id")
    )

    same_team_matches = add_quarantine_reason(
        working_df.filter(same_team_condition),
        "SAME_TEAM_ON_BOTH_SIDES"
    )

    working_df = working_df.filter(
        ~same_team_condition
    )

    quarantined = union_quarantines(
        [
            missing_identity,
            exact_duplicates,
            unknown_team_a,
            unknown_team_b,
            same_team_matches
        ],
        matches
    )

    return {
        "cleaned": working_df,
        "quarantined": quarantined
    }


# ============================================================
# 5. CLEAN PLAYER-MATCH STATS
# ============================================================

def clean_player_match_stats(
    stats: DataFrame,
    raw_players: DataFrame,
    cleaned_players: DataFrame,
    raw_matches: DataFrame,
    cleaned_matches: DataFrame
) -> dict:
    """
    Rules:
    - Stat ID, match ID and player ID are required.
    - Excess exact duplicate rows are quarantined.
    - All rows in a repeated (match_id, player_id) group are
      quarantined, because the intended grain is one row per
      player per match.
    - Unknown player/match IDs are distinguished from references
      to records that exist in the source but were quarantined.
    - Players whose team reference was invalid are not mislabeled
      as unknown players.
    - Numeric parsing and negative-value flags are handled in
      transformation/validation, not silently dropped here.
    """

    working_df, missing_identity = split_missing_identity(
        stats,
        ["stat_id", "match_id", "player_id"],
        "MISSING_STAT_IDENTITY"
    )

    # Step 1: remove excess exact duplicate rows.
    working_df, exact_duplicates = split_exact_duplicates(
        working_df,
        "EXACT_DUPLICATE_STAT_ROW"
    )

    # Step 2: quarantine every row belonging to a repeated
    # match/player combination.
    duplicate_combinations = (
        working_df
        .groupBy("match_id", "player_id")
        .count()
        .filter(F.col("count") > 1)
        .select("match_id", "player_id")
    )

    duplicate_group_rows = working_df.join(
        duplicate_combinations,
        on=["match_id", "player_id"],
        how="inner"
    )

    duplicate_group_rows = add_quarantine_reason(
        duplicate_group_rows,
        "DUPLICATE_MATCH_PLAYER_COMBINATION"
    )

    working_df = working_df.join(
        duplicate_combinations,
        on=["match_id", "player_id"],
        how="left_anti"
    )

    # Step 3: classify player references using BOTH source and
    # cleaned players.
    #
    # In source but not cleaned -> PLAYER_REFERENCE_QUARANTINED
    # Not in source              -> UNKNOWN_PLAYER_ID
    # In cleaned                 -> valid player reference

    raw_player_ids = (
        raw_players
        .select("player_id")
        .filter(F.col("player_id").isNotNull())
        .distinct()
    )

    cleaned_player_ids = (
        cleaned_players
        .select("player_id")
        .filter(F.col("player_id").isNotNull())
        .distinct()
    )

    working_df = working_df.join(
        raw_player_ids.withColumn(
            "_player_exists_in_source",
            F.lit(True)
        ),
        on="player_id",
        how="left"
    )

    working_df = working_df.join(
        cleaned_player_ids.withColumn(
            "_player_is_clean",
            F.lit(True)
        ),
        on="player_id",
        how="left"
    )

    player_quarantined_condition = (
        F.col("_player_exists_in_source").isNotNull()
        & F.col("_player_is_clean").isNull()
    )

    player_quarantined_rows = add_quarantine_reason(
        working_df.filter(player_quarantined_condition)
        .drop("_player_exists_in_source", "_player_is_clean"),
        "PLAYER_REFERENCE_QUARANTINED"
    )

    working_df = working_df.filter(
        ~player_quarantined_condition
    )

    unknown_player_condition = (
        F.col("_player_exists_in_source").isNull()
    )

    unknown_player_rows = add_quarantine_reason(
        working_df.filter(unknown_player_condition)
        .drop("_player_exists_in_source", "_player_is_clean"),
        "UNKNOWN_PLAYER_ID"
    )

    working_df = working_df.filter(
        ~unknown_player_condition
    ).drop("_player_exists_in_source", "_player_is_clean")

    # Step 4: classify match references using BOTH source and
    # cleaned matches.
    #
    # MT0013 exists in source but was quarantined -> reference
    # to quarantined match.
    # MT9999 absent from source -> unknown match.

    raw_match_ids = (
        raw_matches
        .select("match_id")
        .filter(F.col("match_id").isNotNull())
        .distinct()
    )

    cleaned_match_ids = (
        cleaned_matches
        .select("match_id")
        .filter(F.col("match_id").isNotNull())
        .distinct()
    )

    working_df = working_df.join(
        raw_match_ids.withColumn(
            "_match_exists_in_source",
            F.lit(True)
        ),
        on="match_id",
        how="left"
    )

    working_df = working_df.join(
        cleaned_match_ids.withColumn(
            "_match_is_clean",
            F.lit(True)
        ),
        on="match_id",
        how="left"
    )

    match_quarantined_condition = (
        F.col("_match_exists_in_source").isNotNull()
        & F.col("_match_is_clean").isNull()
    )

    match_quarantined_rows = add_quarantine_reason(
        working_df.filter(match_quarantined_condition)
        .drop("_match_exists_in_source", "_match_is_clean"),
        "MATCH_REFERENCE_QUARANTINED"
    )

    working_df = working_df.filter(
        ~match_quarantined_condition
    )

    unknown_match_condition = (
        F.col("_match_exists_in_source").isNull()
    )

    unknown_match_rows = add_quarantine_reason(
        working_df.filter(unknown_match_condition)
        .drop("_match_exists_in_source", "_match_is_clean"),
        "UNKNOWN_MATCH_ID"
    )

    working_df = working_df.filter(
        ~unknown_match_condition
    ).drop("_match_exists_in_source", "_match_is_clean")

    # Step 5: combine quarantine branches.
    quarantined = union_quarantines(
        [
            missing_identity,
            exact_duplicates,
            duplicate_group_rows,
            player_quarantined_rows,
            unknown_player_rows,
            match_quarantined_rows,
            unknown_match_rows
        ],
        stats
    )

    return {
        "cleaned": working_df,
        "quarantined": quarantined
    }


# ============================================================
# 6. CLEAN MATCH EVENTS
# ============================================================

def clean_match_events(
    events: DataFrame,
    raw_matches: DataFrame,
    cleaned_matches: DataFrame
) -> dict:
    """
    Rules:
    - Event ID, match ID and event type are required.
    - Exact duplicate rows are reduced to one copy.
    - A reference to a nonexistent match is UNKNOWN_MATCH_ID.
    - A reference to a source match that was quarantined is
      MATCH_REFERENCE_QUARANTINED.
    - Invalid timestamps/durations remain for investigation;
      this function does not silently drop them.
    """

    working_df, missing_identity = split_missing_identity(
        events,
        ["event_id", "match_id", "event_type"],
        "MISSING_EVENT_IDENTITY"
    )

    working_df, exact_duplicates = split_exact_duplicates(
        working_df,
        "EXACT_DUPLICATE_EVENT_ROW"
    )

    # Identify matches that existed in the original source.
    raw_match_ids = (
        raw_matches
        .select("match_id")
        .filter(F.col("match_id").isNotNull())
        .distinct()
    )

    # Identify matches that survived cleaning.
    cleaned_match_ids = (
        cleaned_matches
        .select("match_id")
        .filter(F.col("match_id").isNotNull())
        .distinct()
    )

    working_df = working_df.join(
        raw_match_ids.withColumn(
            "_match_exists_in_source",
            F.lit(True)
        ),
        on="match_id",
        how="left"
    )

    working_df = working_df.join(
        cleaned_match_ids.withColumn(
            "_match_is_clean",
            F.lit(True)
        ),
        on="match_id",
        how="left"
    )

    # Match exists in raw source, but was rejected by cleaning.
    quarantined_match_condition = (
        F.col("_match_exists_in_source").isNotNull()
        & F.col("_match_is_clean").isNull()
    )

    quarantined_match_events = add_quarantine_reason(
        working_df.filter(quarantined_match_condition)
        .drop("_match_exists_in_source", "_match_is_clean"),
        "MATCH_REFERENCE_QUARANTINED"
    )

    working_df = working_df.filter(
        ~quarantined_match_condition
    )

    # Match ID does not exist in the original source at all.
    unknown_match_condition = (
        F.col("_match_exists_in_source").isNull()
    )

    unknown_match_events = add_quarantine_reason(
        working_df.filter(unknown_match_condition)
        .drop("_match_exists_in_source", "_match_is_clean"),
        "UNKNOWN_MATCH_ID"
    )

    working_df = working_df.filter(
        ~unknown_match_condition
    ).drop("_match_exists_in_source", "_match_is_clean")

    quarantined = union_quarantines(
        [
            missing_identity,
            exact_duplicates,
            quarantined_match_events,
            unknown_match_events
        ],
        events
    )

    return {
        "cleaned": working_df,
        "quarantined": quarantined
    }


# ============================================================
# 7. RUN ALL CLEANING FUNCTIONS
# ============================================================

def clean_all_datasets(datasets: dict) -> dict:
    """
    Run cleaning in dependency order.

    Keep both original transformed datasets and cleaned datasets:
    the originals are required to distinguish unknown references
    from references to records that were quarantined.
    """

    # Keep original transformed datasets for source-existence checks.
    raw_teams = datasets["teams"]
    raw_players = datasets["players"]
    raw_matches = datasets["matches"]

    # 1. Teams
    teams_result = clean_teams(raw_teams)

    # 2. Players use cleaned teams.
    players_result = clean_players(
        raw_players,
        teams_result["cleaned"]
    )

    # 3. Matches use cleaned teams.
    matches_result = clean_matches(
        raw_matches,
        teams_result["cleaned"]
    )

    # 4. Stats compare original and cleaned players/matches.
    stats_result = clean_player_match_stats(
        datasets["player_match_stats"],
        raw_players,
        players_result["cleaned"],
        raw_matches,
        matches_result["cleaned"]
    )

    # 5. Events compare original and cleaned matches.
    events_result = clean_match_events(
        datasets["match_events"],
        raw_matches,
        matches_result["cleaned"]
    )

    return {
        "cleaned": {
            "teams": teams_result["cleaned"],
            "players": players_result["cleaned"],
            "matches": matches_result["cleaned"],
            "player_match_stats": stats_result["cleaned"],
            "match_events": events_result["cleaned"]
        },
        "quarantined": {
            "teams": teams_result["quarantined"],
            "players": players_result["quarantined"],
            "matches": matches_result["quarantined"],
            "player_match_stats": stats_result["quarantined"],
            "match_events": events_result["quarantined"]
        }
    }
