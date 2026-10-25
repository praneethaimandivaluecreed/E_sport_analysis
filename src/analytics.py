
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# ANALYTICS 1: TEAM PERFORMANCE
# ============================================================

def build_team_match_records(matches: DataFrame) -> DataFrame:
    """
    Convert each match into one record per participating team.

    INPUT:
        One row per match, with Team A and Team B in separate
        columns.

    OUTPUT:
        Two rows per eligible match: one for each team.

    ASSUMPTIONS:
        1. We use only matches with status = 'completed'.
        2. Both scores must be successfully parsed integers.
        3. Forfeits are excluded because our assignment does not
           define how their official winner should be determined.
        4. Cancelled matches are excluded from performance
           calculations.
        5. These are analytical assumptions, not official
           tournament rules.
    """

    # Keep completed matches with both scores available.
    # Invalid or missing scores must not be treated as zero.
    eligible_matches = matches.filter(
        (F.col("status") == "completed")
        & F.col("team_a_score_int").isNotNull()
        & F.col("team_b_score_int").isNotNull()
    )

    # --------------------------------------------------------
    # STEP 1: Represent every eligible match from Team A's
    # perspective.
    # --------------------------------------------------------
    team_a_records = eligible_matches.select(
        "match_id",
        "game",
        "parsed_match_time",
        F.col("team_a_id").alias("team_id"),
        F.col("team_b_id").alias("opponent_id"),
        F.col("team_a_score_int").alias("team_score"),
        F.col("team_b_score_int").alias("opponent_score")
    )

    # --------------------------------------------------------
    # STEP 2: Represent every eligible match from Team B's
    # perspective.
    #
    # Notice that Team B's score becomes team_score, while
    # Team A's score becomes opponent_score.
    # --------------------------------------------------------
    team_b_records = eligible_matches.select(
        "match_id",
        "game",
        "parsed_match_time",
        F.col("team_b_id").alias("team_id"),
        F.col("team_a_id").alias("opponent_id"),
        F.col("team_b_score_int").alias("team_score"),
        F.col("team_a_score_int").alias("opponent_score")
    )

    # --------------------------------------------------------
    # STEP 3: Stack both perspectives vertically.
    #
    # unionByName matches columns by their names.
    # Each eligible match now contributes two team records.
    # --------------------------------------------------------
    team_match_records = team_a_records.unionByName(
        team_b_records
    )

    return team_match_records


def calculate_team_performance(
    team_match_records: DataFrame
) -> DataFrame:
    """
    Calculate wins, losses, draws, matches played and win rate
    for each team.

    ASSUMPTIONS:
        1. A higher score means a win.
        2. Equal scores mean a draw.
        3. Matches played includes wins, losses and draws.
        4. Win rate = wins / matches played * 100.
        5. Draws count in the denominator but not in the
           numerator.
        6. Teams with no eligible matches will not appear in
           this aggregated DataFrame; join to the teams table
           later if they must appear with zero counts.
    """

    # Label each team's result from its own perspective.
    results = team_match_records.withColumn(
        "result",
        F.when(
            F.col("team_score") > F.col("opponent_score"),
            F.lit("win")
        ).when(
            F.col("team_score") < F.col("opponent_score"),
            F.lit("loss")
        ).otherwise(
            F.lit("draw")
        )
    )

    # Aggregate the team-level records into one row per team.
    team_summary = results.groupBy("team_id").agg(
        F.countDistinct("match_id").alias("matches_played"),

        F.sum(
            F.when(F.col("result") == "win", 1).otherwise(0)
        ).alias("wins"),

        F.sum(
            F.when(F.col("result") == "loss", 1).otherwise(0)
        ).alias("losses"),

        F.sum(
            F.when(F.col("result") == "draw", 1).otherwise(0)
        ).alias("draws")
    )

    # Calculate win rate as a percentage.
    # The NULLIF-style check prevents division by zero.
    team_summary = team_summary.withColumn(
        "win_rate_pct",
        F.when(
            F.col("matches_played") > 0,
            F.round(
                F.col("wins") / F.col("matches_played") * 100,
                2
            )
        ).otherwise(F.lit(0.0))
    )

    # Explain the existing team performance aggregation
    print("\n========== TEAM PERFORMANCE EXECUTION PLAN ==========")
    team_summary.explain("formatted")

    return team_summary

        




def calculate_player_performance(stats, matches, players):
    """
    Analyze and rank player performance within each game.

    ASSUMPTIONS:
    1. Only stats from completed matches are included in the main analysis.
       Cancelled matches are excluded; forfeits are excluded because their
       player-performance eligibility is not explicitly defined.

    2. Valid non-negative metric values contribute to their respective totals.
       A NULL in one metric does not invalidate the player's other metrics.

    3. Per-round rates require both a valid metric and positive rounds_played.
       Missing rounds exclude a record from the rate calculation, not from
       an otherwise valid metric total.

    4. Players are ranked within each game because different games can use
       different scoring systems. These ranks are not directly comparable
       across games.

    5. Missing metric values are not replaced with zero. If no valid values
       exist, the corresponding total or rate remains NULL.
    """

    # Keep only the match fields needed for this analysis.
    match_info = matches.select("match_id", "game", "status")

    # Join stats to matches so each player record has its game's context.
    # An invalid timestamp alone does not disqualify an otherwise usable
    # performance record because timestamps are not required here.
    performance = stats.join(
        match_info,
        on="match_id",
        how="inner"
    )

    # ASSUMPTION: only completed matches are eligible for ranking.
    performance = performance.filter(
        F.col("status") == "completed"
    )

    # Check each metric independently.
    valid_kills = (
        F.col("kills_int").isNotNull()
        & (F.col("kills_int") >= 0)
    )
    valid_deaths = (
        F.col("deaths_int").isNotNull()
        & (F.col("deaths_int") >= 0)
    )
    valid_assists = (
        F.col("assists_int").isNotNull()
        & (F.col("assists_int") >= 0)
    )
    valid_damage = (
        F.col("damage_int").isNotNull()
        & (F.col("damage_int") >= 0)
    )
    valid_rounds = (
        F.col("rounds_played_int").isNotNull()
        & (F.col("rounds_played_int") > 0)
    )

    # Aggregate by player AND game so different games aren't mixed.
    player_game = performance.groupBy(
        "player_id", "game"
    ).agg(
        F.sum(
            F.when(valid_kills, F.col("kills_int"))
        ).alias("total_kills"),

        F.count(
            F.when(valid_kills, F.lit(1))
        ).alias("valid_kills_records"),

        F.sum(
            F.when(valid_deaths, F.col("deaths_int"))
        ).alias("total_deaths"),

        F.sum(
            F.when(valid_assists, F.col("assists_int"))
        ).alias("total_assists"),

        F.sum(
            F.when(valid_damage, F.col("damage_int"))
        ).alias("total_damage"),

        # Numerator and denominator use the same valid records.
        F.sum(
            F.when(
                valid_kills & valid_rounds,
                F.col("kills_int")
            )
        ).alias("kills_for_rate"),

        F.sum(
            F.when(
                valid_kills & valid_rounds,
                F.col("rounds_played_int")
            )
        ).alias("rounds_for_kills_rate"),

        F.sum(
            F.when(
                valid_assists & valid_rounds,
                F.col("assists_int")
            )
        ).alias("assists_for_rate"),

        F.sum(
            F.when(
                valid_assists & valid_rounds,
                F.col("rounds_played_int")
            )
        ).alias("rounds_for_assists_rate"),

        F.sum(
            F.when(
                valid_damage & valid_rounds,
                F.col("damage_int")
            )
        ).alias("damage_for_rate"),

        F.sum(
            F.when(
                valid_damage & valid_rounds,
                F.col("rounds_played_int")
            )
        ).alias("rounds_for_damage_rate"),

        F.sum(
            F.when(
                valid_deaths & valid_rounds,
                F.col("deaths_int")
            )
        ).alias("deaths_for_rate"),

        F.sum(
            F.when(
                valid_deaths & valid_rounds,
                F.col("rounds_played_int")
            )
        ).alias("rounds_for_deaths_rate")
    )

    # Calculate per-round rates.
    # A missing or invalid denominator produces NULL.
    player_game = (
        player_game
        .withColumn(
            "kills_per_round",
            F.when(
                F.col("rounds_for_kills_rate") > 0,
                F.col("kills_for_rate")
                / F.col("rounds_for_kills_rate")
            )
        )
        .withColumn(
            "assists_per_round",
            F.when(
                F.col("rounds_for_assists_rate") > 0,
                F.col("assists_for_rate")
                / F.col("rounds_for_assists_rate")
            )
        )
        .withColumn(
            "damage_per_round",
            F.when(
                F.col("rounds_for_damage_rate") > 0,
                F.col("damage_for_rate")
                / F.col("rounds_for_damage_rate")
            )
        )
        .withColumn(
            "deaths_per_round",
            F.when(
                F.col("rounds_for_deaths_rate") > 0,
                F.col("deaths_for_rate")
                / F.col("rounds_for_deaths_rate")
            )
        )
    )

    # Add player details. A missing role does not disqualify a player.
    player_info = players.select(
        "player_id",
        "player_name",
        "team_id",
        "role"
    )

    player_game = player_game.join(
        player_info,
        on="player_id",
        how="left"
    )

    # Rank players independently within each game.
    # NULL rates are placed last and remain unranked.
    game_window_kills = Window.partitionBy("game").orderBy(
        F.col("kills_per_round").desc_nulls_last()
    )

    game_window_assists = Window.partitionBy("game").orderBy(
        F.col("assists_per_round").desc_nulls_last()
    )

    game_window_damage = Window.partitionBy("game").orderBy(
        F.col("damage_per_round").desc_nulls_last()
    )

    game_window_deaths = Window.partitionBy("game").orderBy(
        F.col("deaths_per_round").asc_nulls_last()
    )

    player_game = (
        player_game
        .withColumn(
            "kills_per_round_rank",
            F.when(
                F.col("kills_per_round").isNotNull(),
                F.dense_rank().over(game_window_kills)
            )
        )
        .withColumn(
            "assists_per_round_rank",
            F.when(
                F.col("assists_per_round").isNotNull(),
                F.dense_rank().over(game_window_assists)
            )
        )
        .withColumn(
            "damage_per_round_rank",
            F.when(
                F.col("damage_per_round").isNotNull(),
                F.dense_rank().over(game_window_damage)
            )
        )
        .withColumn(
            "deaths_per_round_rank",
            F.when(
                F.col("deaths_per_round").isNotNull(),
                F.dense_rank().over(game_window_deaths)
            )
        )
    )

    # Select the final analytical output.
    # Ranks use unrounded rates; rounding is only for presentation.
    result = player_game.select(
        "player_id",
        "player_name",
        "team_id",
        "role",
        "game",
        "total_kills",
        "total_deaths",
        "total_assists",
        "total_damage",
        F.round("kills_per_round", 3).alias("kills_per_round"),
        F.round("assists_per_round", 3).alias("assists_per_round"),
        F.round("damage_per_round", 3).alias("damage_per_round"),
        F.round("deaths_per_round", 3).alias("deaths_per_round"),
        "kills_per_round_rank",
        "assists_per_round_rank",
        "damage_per_round_rank",
        "deaths_per_round_rank"
    )

    # ENGINEERING ANALYSIS:
    # Inspect the physical execution plan for the existing transformation.
    print("\n========== PLAYER PERFORMANCE EXECUTION PLAN ==========")
    result.explain("formatted")

    return result




def summarize_events_by_game_type(events, matches):
    """
    Count events by game and event type.

    ASSUMPTIONS:
    1. Input events and matches are cleaned DataFrames.
    2. Only events linked to retained matches can be assigned to a game.
    3. An invalid event timestamp does not prevent an event from being
       counted by game or event type.
    4. Counts describe recorded events, not necessarily disruptions
       experienced by a particular player or team.
    """

    match_info = matches.select("match_id", "game")

    # Join to learn which game each event belongs to.
    # The inner join excludes events whose match is unknown or quarantined.
    event_data = events.join(
        match_info,
        on="match_id",
        how="inner"
    )

    return (
        event_data
        .groupBy("game", "event_type")
        .agg(
            F.count("*").alias("event_count")
        )
        .orderBy("game", F.desc("event_count"))
    )


def summarize_events_by_month(events, matches):
    """
    Count events by calendar month and event type.

    ASSUMPTIONS:
    1. The event timestamp determines the month, not the match timestamp.
    2. Events with invalid timestamps are excluded from this time-based
       summary only. They can still appear in other valid summaries.
    3. The source timestamps are assumed to use a consistent time zone.
       No time-zone conversion is performed here.
    """

    match_info = matches.select("match_id", "game")

    event_data = (
        events
        .join(match_info, on="match_id", how="inner")
        .filter(F.col("parsed_event_time").isNotNull())
        .withColumn(
            "event_month",
            F.date_format("parsed_event_time", "yyyy-MM")
        )
    )

    return (
        event_data
        .groupBy("event_month", "game", "event_type")
        .agg(
            F.count("*").alias("event_count")
        )
        .orderBy("event_month", "game", F.desc("event_count"))
    )


def summarize_event_durations(events, matches):
    """
    Summarize valid event durations by game and event type.

    ASSUMPTIONS:
    1. Only non-negative, successfully parsed durations contribute.
    2. Missing or malformed duration does not remove an event from
       event-count summaries.
    3. duration_seconds describes the event, NOT the match duration.
    """

    match_info = matches.select("match_id", "game")

    event_data = events.join(
        match_info,
        on="match_id",
        how="inner"
    )

    valid_durations = event_data.filter(
        F.col("duration_seconds_int").isNotNull()
        & (F.col("duration_seconds_int") >= 0)
    )

    return (
        valid_durations
        .groupBy("game", "event_type")
        .agg(
            F.count("*").alias("events_with_valid_duration"),
            F.round(
                F.avg("duration_seconds_int"), 2
            ).alias("avg_duration_seconds"),
            F.max("duration_seconds_int").alias("max_duration_seconds")
        )
        .orderBy("game", "event_type")
    )


def summarize_event_exposure_by_team(events, matches):
    """
    Count match events associated with each participating team.

    ASSUMPTION:
    Events are recorded at match level and do not identify the affected
    team. Each event is therefore attributed to BOTH participating teams.
    This measures match-event exposure, NOT events caused by or affecting
    a specific team. Team counts should not be added together to calculate
    the total number of unique events.
    """

    match_info = matches.select(
        "match_id",
        "team_a_id",
        "team_b_id"
    )

    event_data = events.join(
        match_info,
        on="match_id",
        how="inner"
    )

    team_a_events = event_data.select(
        F.col("team_a_id").alias("team_id"),
        "event_type",
        "event_id"
    )

    team_b_events = event_data.select(
        F.col("team_b_id").alias("team_id"),
        "event_type",
        "event_id"
    )

    team_events = team_a_events.unionByName(team_b_events)

    return (
        team_events
        .groupBy("team_id", "event_type")
        .agg(
            F.countDistinct("event_id").alias("match_event_count")
        )
        .orderBy("team_id", F.desc("match_event_count"))
    )




def calculate_team_form_over_time(team_match_records):
    """
    Calculate each team's rolling form over its latest three eligible matches.

    ASSUMPTIONS:
    1. Input contains one record per team per eligible match.
    2. Only records with valid match timestamps are included.
    3. A win earns 1 win; a draw or loss earns 0 wins.
    4. Draws are included in the rolling-match denominator.
    5. The current match is included in the three-match window.
    6. Match ID breaks timestamp ties consistently. It makes ordering
       deterministic, but does not establish the true order of tied matches.
    """

    # A time-based analysis requires a valid timestamp.
    time_records = team_match_records.filter(
        F.col("parsed_match_time").isNotNull()
    )

    # Determine the result from the team's perspective.
    results = time_records.withColumn(
        "is_win",
        F.when(
            F.col("team_score") > F.col("opponent_score"), 1
        ).otherwise(0)
    )

    # Each team's matches are ordered independently.
    # match_id provides a deterministic tie-breaker when timestamps match.
    team_window = (
        Window
        .partitionBy("team_id")
        .orderBy(
            F.col("parsed_match_time").asc(),
            F.col("match_id").asc()
        )
    )

    # Current match plus the previous two matches = up to three matches.
    rolling_window = team_window.rowsBetween(-2, 0)

    form = (
        results
        .withColumn(
            "matches_in_window",
            F.count("*").over(rolling_window)
        )
        .withColumn(
            "wins_in_window",
            F.sum("is_win").over(rolling_window)
        )
        .withColumn(
            "rolling_win_rate_pct",
            F.round(
                F.col("wins_in_window")
                / F.col("matches_in_window") * 100,
                2
            )
        )
        .withColumn(
            "match_number_for_team",
            F.row_number().over(team_window)
        )
    )

    return form.select(
        "team_id",
        "match_id",
        "parsed_match_time",
        "team_score",
        "opponent_score",
        "is_win",
        "match_number_for_team",
        "matches_in_window",
        "wins_in_window",
        "rolling_win_rate_pct"
    )
