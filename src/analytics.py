from pyspark.sql import functions as F
from pyspark.sql.window import Window



# ANALYTICS 1: TEAM PERFORMANCE

# fxn for converting each match into one record per participating team
def build_team_match_records(matches):

    # keeping only completed matches where both scores were parsed successfully
    # a NULL score must not be treated as zero because that would create
    # a result that was never actually recorded
    # cancelled matches are excluded because they should not contribute
    # to normal team performance calculations
    eligible_matches = matches.filter(
        (F.col("status") == "completed")
        & F.col("team_a_score_int").isNotNull()
        & F.col("team_b_score_int").isNotNull()
    )

    
    # creating records from Team A's perspective

    # team_a_id becomes team_id because Team A is the team represented
    # by this row
    # team_b_id becomes opponent_id because Team B is the opponent
    # Team A's score becomes team_score, and Team B's score becomes
    # opponent_score
    
    team_a_records = eligible_matches.select(
        "match_id",
        "game",
        "parsed_match_time",
        F.col("team_a_id").alias("team_id"),
        F.col("team_b_id").alias("opponent_id"),
        F.col("team_a_score_int").alias("team_score"),
        F.col("team_b_score_int").alias("opponent_score")
    )

    # creating records from Team B's perspective
    
    team_b_records = eligible_matches.select(
        "match_id",
        "game",
        "parsed_match_time",
        F.col("team_b_id").alias("team_id"),
        F.col("team_a_id").alias("opponent_id"),
        F.col("team_b_score_int").alias("team_score"),
        F.col("team_a_score_int").alias("opponent_score")
    )

    
    # combining both team perspectives
    # stacking Team A and Team B records vertically
    # unionByName() matches columns by name rather than column position
    # each eligible match now contributes two rows, one for each team
    team_match_records = team_a_records.unionByName(
        team_b_records
    )

    # returning the team-perspective records for further analysis
    return team_match_records


# fxn for calculating wins, losses, draws, matches played and win rate
def calculate_team_performance(team_match_records):

    # creating a result column from each team's own perspective
    # if the team's score is higher, the result is a win
    # if the team's score is lower, the result is a loss
    # if neither condition is true, the scores are equal and the result
    # is classified as a draw
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

    # grouping all team-perspective records by team_id
    # this produces one summary row per team
    team_summary = results.groupBy("team_id").agg(

        # counting unique match IDs to calculate matches played
        # countDistinct prevents a repeated match ID from being counted
        # more than once in this particular metric
        F.countDistinct("match_id").alias("matches_played"),

        # counting wins by assigning 1 to wins and 0 to all other results
        # sum() adds those values to get the total number of wins
        F.sum(
            F.when(F.col("result") == "win", 1).otherwise(0)
        ).alias("wins"),

        # counting losses using the same conditional aggregation approach
        F.sum(
            F.when(F.col("result") == "loss", 1).otherwise(0)
        ).alias("losses"),

        # counting draws in the same way
        F.sum(
            F.when(F.col("result") == "draw", 1).otherwise(0)
        ).alias("draws")
    )

    # calculating the win rate as a percentage
    # wins are divided by matches played and multiplied by 100
    # draws count toward matches played but do not count as wins
    # the condition prevents division by zero if matches_played is zero
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

    # printing the formatted physical execution plan for inspection
    # this helps us examine how Spark plans the aggregation
   
    print("\nTEAM PERFORMANCE EXECUTION PLAN ")
    team_summary.explain("formatted")

    # returning the team-level summary dataframe
    return team_summary



# ANALYTICS 2: PLAYER PERFORMANCE

# fxn for calculating and ranking player performance within each game
def calculate_player_performance(stats, matches, players):

    # selecting only the match columns needed for this analysis
    # match_id is used to join statistics to their corresponding match
    # game is used to group and rank players within a game
    # status is used to keep only eligible completed matches
    match_info = matches.select("match_id", "game", "status")

    # joining player statistics with match information
    # an inner join keeps only statistics records that have a matching
    # match_id in the matches dataframe
    # this adds game and status to each player's statistics record
    # timestamp validity is not checked here because the timestamp is
    # not needed for these performance calculations
    performance = stats.join(
        match_info,
        on="match_id",
        how="inner"
    )

    # keeping statistics associated with completed matches only
    # cancelled and other non-completed matches are excluded from rankings
    performance = performance.filter(
        F.col("status") == "completed"
    )

    
    #  defining validation conditions for each metric
    # kills is valid when it was parsed successfully and is not negative
    # NULL and negative values are excluded from kill calculations
    valid_kills = (
        F.col("kills_int").isNotNull()
        & (F.col("kills_int") >= 0)
    )

    # deaths is checked independently from kills
    # an invalid death value does not automatically invalidate other metrics
    valid_deaths = (
        F.col("deaths_int").isNotNull()
        & (F.col("deaths_int") >= 0)
    )

    # assists is valid when present and non-negative
    valid_assists = (
        F.col("assists_int").isNotNull()
        & (F.col("assists_int") >= 0)
    )

    # damage is valid when present and non-negative
    valid_damage = (
        F.col("damage_int").isNotNull()
        & (F.col("damage_int") >= 0)
    )

    # rounds_played must be present and greater than zero
    # a positive round count is required to calculate per-round rates
    valid_rounds = (
        F.col("rounds_played_int").isNotNull()
        & (F.col("rounds_played_int") > 0)
    )

    
    #  aggregating player statistics by player and game
    # grouping by both player_id and game keeps statistics for different
    # games separate instead of combining their metrics together
    player_game = performance.groupBy(
        "player_id", "game"
    ).agg(

        # summing only valid kills
        # when the condition is false or NULL, when() produces NULL
        # sum() ignores NULL values when valid values are available
        F.sum(
            F.when(valid_kills, F.col("kills_int"))
        ).alias("total_kills"),

        # counting how many records contain valid kill values
        # this can help show how much valid data contributed to the total
        F.count(
            F.when(valid_kills, F.lit(1))
        ).alias("valid_kills_records"),

        # calculating the total valid deaths
        F.sum(
            F.when(valid_deaths, F.col("deaths_int"))
        ).alias("total_deaths"),

        # calculating the total valid assists
        F.sum(
            F.when(valid_assists, F.col("assists_int"))
        ).alias("total_assists"),

        # calculating the total valid damage
        F.sum(
            F.when(valid_damage, F.col("damage_int"))
        ).alias("total_damage"),

        # collecting valid kills only from records that also have
        # a valid positive round count
        # these values will form the numerator for kills per round
        F.sum(
            F.when(
                valid_kills & valid_rounds,
                F.col("kills_int")
            )
        ).alias("kills_for_rate"),

        # collecting the round counts from the same records used above
        # using the same condition keeps the numerator and denominator
        # aligned to the same set of valid records
        F.sum(
            F.when(
                valid_kills & valid_rounds,
                F.col("rounds_played_int")
            )
        ).alias("rounds_for_kills_rate"),

        # collecting valid assists from records with valid round counts
        # these values form the numerator for assists per round
        F.sum(
            F.when(
                valid_assists & valid_rounds,
                F.col("assists_int")
            )
        ).alias("assists_for_rate"),

        # collecting round counts from those same valid assist records
        F.sum(
            F.when(
                valid_assists & valid_rounds,
                F.col("rounds_played_int")
            )
        ).alias("rounds_for_assists_rate"),

        # collecting valid damage from records with valid round counts
        F.sum(
            F.when(
                valid_damage & valid_rounds,
                F.col("damage_int")
            )
        ).alias("damage_for_rate"),

        # collecting the corresponding round counts for valid damage
        F.sum(
            F.when(
                valid_damage & valid_rounds,
                F.col("rounds_played_int")
            )
        ).alias("rounds_for_damage_rate"),

        # collecting valid deaths from records with valid round counts
        F.sum(
            F.when(
                valid_deaths & valid_rounds,
                F.col("deaths_int")
            )
        ).alias("deaths_for_rate"),

        # collecting the corresponding round counts for valid deaths
        F.sum(
            F.when(
                valid_deaths & valid_rounds,
                F.col("rounds_played_int")
            )
        ).alias("rounds_for_deaths_rate")
    )

    
    #  calculating per-round rates

    # kills per round = valid kills / corresponding valid rounds
    # when the denominator is NULL or zero, the rate remains NULL
    # NULL is preserved rather than replacing a missing rate with zero
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

        # assists per round uses only the valid assist records
        .withColumn(
            "assists_per_round",
            F.when(
                F.col("rounds_for_assists_rate") > 0,
                F.col("assists_for_rate")
                / F.col("rounds_for_assists_rate")
            )
        )

        # damage per round uses only the valid damage records
        .withColumn(
            "damage_per_round",
            F.when(
                F.col("rounds_for_damage_rate") > 0,
                F.col("damage_for_rate")
                / F.col("rounds_for_damage_rate")
            )
        )

        # deaths per round uses only the valid death records
        .withColumn(
            "deaths_per_round",
            F.when(
                F.col("rounds_for_deaths_rate") > 0,
                F.col("deaths_for_rate")
                / F.col("rounds_for_deaths_rate")
            )
        )
    )

    
    # adding player details

    # selecting descriptive player information from the players dataframe
    # these columns help identify the player in the final analytics output
    player_info = players.select(
        "player_id",
        "player_name",
        "team_id",
        "role"
    )

    # joining the descriptive details to the aggregated player-game data
    # a left join preserves the aggregated performance record even if
    # player details are unexpectedly missing
    # a missing role does not prevent a player from appearing
    player_game = player_game.join(
        player_info,
        on="player_id",
        how="left"
    )

    # --------------------------------------------------------
    # Step 5: creating ranking windows for each game
    # --------------------------------------------------------

    # partitionBy("game") creates an independent ranking group for each game
    # players are ranked from highest to lowest kills per round
    # desc_nulls_last() places missing rates after valid rates
    game_window_kills = Window.partitionBy("game").orderBy(
        F.col("kills_per_round").desc_nulls_last()
    )

    # ranking players within each game by assists per round
    game_window_assists = Window.partitionBy("game").orderBy(
        F.col("assists_per_round").desc_nulls_last()
    )

    # ranking players within each game by damage per round
    game_window_damage = Window.partitionBy("game").orderBy(
        F.col("damage_per_round").desc_nulls_last()
    )

    # ranking deaths per round from lowest to highest
    # this ordering treats a lower death rate as a better rank
    game_window_deaths = Window.partitionBy("game").orderBy(
        F.col("deaths_per_round").asc_nulls_last()
    )

    #  assigning ranks

    # dense_rank() assigns the same rank to tied values
    # unlike rank(), dense_rank() does not skip rank numbers after ties
    # the when() condition prevents NULL rates from receiving a rank
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

    

    # keeping player identity, game, totals, rates and rankings
    # the intermediate numerator and denominator columns are not included
    # in the final output because they were used only to calculate rates
    #
    # rounding rates to three decimal places is for presentation
    # the rankings above were calculated using the unrounded rates
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

    # printing the formatted execution plan to inspect Spark's physical plan
    # this helps us investigate joins, aggregations and window operations
    print("\nPLAYER PERFORMANCE EXECUTION PLAN")
    result.explain("formatted")

    # returning the final player performance dataframe
    return result



# ANALYTICS 3: EVENT SUMMARIES


# fxn for counting events by game and event type
def summarize_events_by_game_type(events, matches):

    # selecting match_id and game so each event can be associated with
    # the game in which its match took place
    match_info = matches.select("match_id", "game")

    # joining events to match information using match_id
    # inner join excludes events without a matching retained match
    # no timestamp validation is required for this count-based summary
    event_data = events.join(
        match_info,
        on="match_id",
        how="inner"
    )

    # grouping by game and event_type, then counting event records
    # orderBy() sorts by game and places the most frequent event types first
    return (
        event_data
        .groupBy("game", "event_type")
        .agg(
            F.count("*").alias("event_count")
        )
        .orderBy("game", F.desc("event_count"))
    )


# fxn for counting events by calendar month and event type
def summarize_events_by_month(events, matches):

    # selecting the match information needed to associate events with games
    match_info = matches.select("match_id", "game")

    # joining events to matches, then keeping only valid parsed event timestamps
    # an event without a valid timestamp cannot be assigned to a calendar month
    # date_format() converts the timestamp to a year-month string such as 2026-10
    event_data = (
        events
        .join(match_info, on="match_id", how="inner")
        .filter(F.col("parsed_event_time").isNotNull())
        .withColumn(
            "event_month",
            F.date_format("parsed_event_time", "yyyy-MM")
        )
    )

    # counting events for each month, game and event type
    # sorting by the yyyy-MM string gives chronological month order
    return (
        event_data
        .groupBy("event_month", "game", "event_type")
        .agg(
            F.count("*").alias("event_count")
        )
        .orderBy("event_month", "game", F.desc("event_count"))
    )


# fxn for summarizing valid event durations by game and event type
def summarize_event_durations(events, matches):

    # selecting the match ID and game for each match
    match_info = matches.select("match_id", "game")

    # joining each event to its match so the event can be grouped by game
    event_data = events.join(
        match_info,
        on="match_id",
        how="inner"
    )

    # keeping only successfully parsed, non-negative event durations
    # negative durations and NULL values are excluded from duration metrics
    # the event itself is not removed from other event summaries
    valid_durations = event_data.filter(
        F.col("duration_seconds_int").isNotNull()
        & (F.col("duration_seconds_int") >= 0)
    )

    # grouping valid durations by game and event type
    # count() gives the number of events with usable durations
    # avg() calculates their average duration
    # max() identifies the longest recorded duration in each group
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


# fxn for counting match-event exposure by participating team
def summarize_event_exposure_by_team(events, matches):

    # selecting match IDs and both participating team IDs
    # these fields let us associate each match-level event with both teams
    match_info = matches.select(
        "match_id",
        "team_a_id",
        "team_b_id"
    )

    # joining event records to their corresponding match and team IDs
    event_data = events.join(
        match_info,
        on="match_id",
        how="inner"
    )

    # creating one dataframe from Team A's perspective
    # team_a_id is renamed to team_id so it can be combined with Team B records
    # event_type and event_id are retained for grouping and counting
    team_a_events = event_data.select(
        F.col("team_a_id").alias("team_id"),
        "event_type",
        "event_id"
    )

    # creating the equivalent dataframe from Team B's perspective
    team_b_events = event_data.select(
        F.col("team_b_id").alias("team_id"),
        "event_type",
        "event_id"
    )

    # stacking Team A and Team B event records
    # every event is associated with both teams participating in its match
    team_events = team_a_events.unionByName(team_b_events)

    # grouping records by team and event type
    # countDistinct() counts unique event IDs within each group
    # this is match-event exposure, not proof that a particular team
    # caused or directly experienced the event
    return (
        team_events
        .groupBy("team_id", "event_type")
        .agg(
            F.countDistinct("event_id").alias("match_event_count")
        )
        .orderBy("team_id", F.desc("match_event_count"))
    )



# ANALYTICS 4: TEAM FORM OVER TIME

# fxn for calculating each team's rolling form over its latest three matches
def calculate_team_form_over_time(team_match_records):

    # keeping only records with a valid parsed match timestamp
    # chronological analysis requires a timestamp to determine match order
    time_records = team_match_records.filter(
        F.col("parsed_match_time").isNotNull()
    )

    # creating a numeric indicator for whether the team won each match
    # a win is assigned 1; a draw or loss is assigned 0
    # this allows sum() to count wins inside a rolling window
    results = time_records.withColumn(
        "is_win",
        F.when(
            F.col("team_score") > F.col("opponent_score"), 1
        ).otherwise(0)
    )

    # defining a separate ordered sequence of matches for each team
    # partitionBy() prevents records from different teams mixing together
    # matches are sorted by timestamp from earliest to latest
    # match_id breaks timestamp ties deterministically
    # the tie-breaker gives consistent ordering but does not prove the
    # true chronological order of matches sharing the same timestamp
    team_window = (
        Window
        .partitionBy("team_id")
        .orderBy(
            F.col("parsed_match_time").asc(),
            F.col("match_id").asc()
        )
    )

    # defining a rolling window containing the current row and two rows before it
    # rowsBetween(-2, 0) means up to three records:
    # the previous two matches plus the current match
    # at the start of a team's history, fewer than three rows are available
    rolling_window = team_window.rowsBetween(-2, 0)

    # calculating rolling form using the window defined above
    form = (
        results
        .withColumn(
            # counting matches in the current rolling window
            "matches_in_window",
            F.count("*").over(rolling_window)
        )
        .withColumn(
            # summing is_win values to count wins in the rolling window
            "wins_in_window",
            F.sum("is_win").over(rolling_window)
        )
        .withColumn(
            # calculating rolling win rate as a percentage
            # the denominator includes wins, losses and draws
            "rolling_win_rate_pct",
            F.round(
                F.col("wins_in_window")
                / F.col("matches_in_window") * 100,
                2
            )
        )
        .withColumn(
            # numbering each team's matches in chronological order
            # row_number starts at 1 for each team
            "match_number_for_team",
            F.row_number().over(team_window)
        )
    )

    # selecting the columns needed to understand the team's form over time
    # intermediate columns not needed for the final output are excluded
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