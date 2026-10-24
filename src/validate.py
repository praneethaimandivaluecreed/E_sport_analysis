"""
Module: validate.py
Description: Implements all single-file and cross-file business rule validations,
             foreign key checks, and explicit quarantine isolation logic 
             matching the tournament analytics notebook.
"""

from pyspark.sql import functions as F

def isolate_match_quarantines(df_matches_processed):
    """
    Quarantines matches where:
    - Team A and Team B are identical (Self-matches like MT0013).
    - Completed matches have missing or invalid scores.
    """
    df_match_outcome_check = df_matches_processed.withColumn(
        "score_relationship",
        F.when(F.col("clean_team_a_score") > F.col("clean_team_b_score"), "TEAM_A_WIN")
         .when(F.col("clean_team_a_score") < F.col("clean_team_b_score"), "TEAM_B_WIN")
         .when(F.col("clean_team_a_score") == F.col("clean_team_b_score"), "DRAW")
         .otherwise("SCORE_MISSING_OR_INVALID")
    )
    
    # 1. Completed matches with missing/invalid scores (8 rows)
    df_matches_score_quarantine = df_match_outcome_check.filter(
        (F.col("match_status") == "completed") & (F.col("score_relationship") == "SCORE_MISSING_OR_INVALID")
    ).withColumn("match_quarantine_reason", F.lit("COMPLETED_MATCH_MISSING_SCORE"))
    
    # 2. Self-matches (Team A ID == Team B ID)
    df_matches_self_quarantine = df_match_outcome_check.filter(
        F.col("team_a_id") == F.col("team_b_id")
    ).withColumn("match_quarantine_reason", F.lit("SELF_MATCH_ANOMALY"))
    
    # Valid matches base
    df_matches_valid = df_match_outcome_check.filter(
        ~((F.col("match_status") == "completed") & (F.col("score_relationship") == "SCORE_MISSING_OR_INVALID")) &
        (F.col("team_a_id") != F.col("team_b_id"))
    )
    
    return df_matches_valid, df_matches_score_quarantine, df_matches_self_quarantine

def isolate_player_quarantines(df_players, df_teams):
    """
    Cross-file validation: players.team_id -> teams.team_id
    Quarantines orphaned players (e.g., PL006 from TM99).
    """
    df_players_quarantine = df_players.join(df_teams.select("team_id"), on="team_id", how="left_anti") \
        .withColumn("player_quarantine_reason", F.lit("ORPHANED_TEAM_REFERENCE"))
        
    df_players_valid = df_players.join(df_teams.select("team_id"), on="team_id", how="left_semi")
    
    return df_players_valid, df_players_quarantine

def isolate_stats_quarantines(df_player_stats_processed, df_players_valid, df_matches_valid, df_all_players):
    """
    Cross-file validations for player match stats:
    - Grain violations (duplicate player-match stat entries).
    - Ghost / unknown players (e.g., PL999).
    - Cancelled match stats.
    - Player-team side mismatches.
    """
    # 1. Grain violation check (duplicate combinations)
    df_dup_combos = df_player_stats_processed.groupBy("match_id", "player_id").count().filter(F.col("count") > 1)
    df_stats_grain_quarantine = df_player_stats_processed.join(df_dup_combos.select("match_id", "player_id"), on=["match_id", "player_id"], how="inner") \
        .withColumn("stat_quarantine_reason", F.lit("GRAIN_VIOLATION_DUPLICATE_STATS"))
        
    df_stats_valid_grain = df_player_stats_processed.join(df_dup_combos.select("match_id", "player_id"), on=["match_id", "player_id"], how="left_anti")
    
    # 2. Ghost/Unknown player check (e.g., PL999)
    df_ghost_quarantine = df_stats_valid_grain.join(df_all_players.select("player_id"), on="player_id", how="left_anti") \
        .withColumn("stat_quarantine_reason", F.lit("UNKNOWN_GHOST_PLAYER"))
        
    df_stats_valid_players = df_stats_valid_grain.join(df_all_players.select("player_id"), on="player_id", how="left_semi")

    # 3. Foreign key validation against valid matches
    df_stats_valid_matches = df_stats_valid_players.join(df_matches_valid.select("match_id"), on="match_id", how="left_semi")
    
    # 4. Match status filter (isolate cancelled match stats)
    df_stats_with_status = df_stats_valid_matches.join(df_matches_valid.select("match_id", "match_status"), on="match_id", how="inner")
    
    df_stats_cancelled_quarantine = df_stats_with_status.filter(F.col("match_status") == "cancelled") \
        .withColumn("stat_quarantine_reason", F.lit("CANCELLED_MATCH_STATS"))
        
    df_stats_completed_forfeit = df_stats_with_status.filter(F.col("match_status").isin("completed", "forfeit"))
    
    # 5. Team alignment check (Player team must match Team A or Team B in the match)
    df_stats_with_player_team = df_stats_completed_forfeit.join(
        df_players_valid.select("player_id", "team_id").withColumnRenamed("team_id", "player_team_id"),
        on="player_id", how="inner"
    )
    df_stats_team_alignment = df_stats_with_player_team.join(
        df_matches_valid.select("match_id", "team_a_id", "team_b_id"),
        on="match_id", how="inner"
    )
    
    df_stats_aligned = df_stats_team_alignment.withColumn(
        "side",
        F.when(F.col("player_team_id") == F.col("team_a_id"), "TEAM_A")
         .when(F.col("player_team_id") == F.col("team_b_id"), "TEAM_B")
         .otherwise("MISMATCH_OR_UNKNOWN_TEAM")
    )
    
    df_stats_mismatch_quarantine = df_stats_aligned.filter(F.col("side") == "MISMATCH_OR_UNKNOWN_TEAM") \
        .withColumn("stat_quarantine_reason", F.lit("PLAYER_TEAM_MISMATCH"))
        
    df_stats_final_valid = df_stats_aligned.filter(F.col("side").isin("TEAM_A", "TEAM_B"))
    
    return df_stats_final_valid, df_stats_grain_quarantine, df_ghost_quarantine, df_stats_cancelled_quarantine, df_stats_mismatch_quarantine

def isolate_event_quarantines(df_match_events_processed, df_matches_processed):
    """
    Cross-file validation for match events:
    - Quarantines orphaned events referencing missing match IDs (e.g., EV00006 on MT9999).
    - Quarantines events occurring outside match duration bounds (implausible timelines).
    """
    # 1. Foreign key match existence check
    df_events_valid_matches = df_match_events_processed.join(df_matches_processed.select("match_id"), on="match_id", how="semi")
    df_events_orphaned_quarantine = df_match_events_processed.join(df_matches_processed.select("match_id"), on="match_id", how="anti") \
        .withColumn("event_quarantine_reason", F.lit("ORPHANED_MATCH_EVENT"))
        
    # 2. Temporal window plausibility check (Event time must fall between match start and end)
    df_events_with_match_time = df_events_valid_matches.join(
        df_matches_processed.select("match_id", "parsed_match_time"), on="match_id", how="inner"
    )
    
    df_event_duration_bounds = df_events_with_match_time \
        .withColumn("clean_duration", F.expr("try_cast(duration_seconds AS INT)")) \
        .withColumn(
            "match_end_time", 
            F.when(F.col("clean_duration").isNotNull(), F.expr("from_unixtime(unix_timestamp(parsed_match_time) + clean_duration)"))
             .otherwise(None)
        ) \
        .withColumn("parsed_match_end_time", F.col("match_end_time").cast("timestamp"))
        
    df_event_window_check = df_event_duration_bounds.withColumn(
        "window_plausibility",
        F.when(F.col("parsed_match_end_time").isNull(), "DURATION_MISSING_OR_INVALID")
         .when((F.col("parsed_event_time") >= F.col("parsed_match_time")) & (F.col("parsed_event_time") <= F.col("parsed_match_end_time")), "WITHIN_MATCH_WINDOW")
         .otherwise("OUTSIDE_MATCH_WINDOW")
    )
    
    df_events_window_quarantine = df_event_window_check.filter(F.col("window_plausibility") != "WITHIN_MATCH_WINDOW") \
        .withColumn("event_quarantine_reason", F.lit("EVENT_OUTSIDE_MATCH_WINDOW_OR_INVALID_DURATION"))
        
    df_events_final_valid = df_event_window_check.filter(F.col("window_plausibility") == "WITHIN_MATCH_WINDOW")
    
    return df_events_final_valid, df_events_orphaned_quarantine, df_events_window_quarantine