 #Standardizes text fields, safely parses timestamps and metrics, 
 #and maps malformed placeholder values to nulls.


from pyspark.sql import functions as F

def clean_teams(df_teams):
    # standardizing team records
    return df_teams.dropDuplicates()

def clean_players(df_players):
    """Standardizes player roles and statuses to lowercase for consistent analysis."""
    return df_players \
        .dropDuplicates() \
        .withColumn("role", F.lower(F.col("role"))) \
        .withColumn("status", F.lower(F.col("status")))

def clean_matches(df_matches):
    """
    Cleans matches data:
    - Drops exact duplicate records.
    - Parses timestamps safely, flagging TBD or invalid formats.
    - Cleans scores using try_cast to handle placeholders like '??', '-', 'unknown'.
    """
    df_clean = df_matches.dropDuplicates()
    
    df_processed = df_clean \
        .withColumn("parsed_match_time", F.try_to_timestamp(F.col("match_time"))) \
        .withColumn(
            "dq_status",
            F.when(F.col("parsed_match_time").isNotNull(), "VALID")
             .when(F.col("match_time") == "TBD", "FLAGGED_TBD_TIMESTAMP")
             .otherwise("FLAGGED_INVALID_TIMESTAMP")
        ) \
        .withColumn("match_status", F.lower(F.col("match_status"))) \
        .withColumn(
            "clean_team_a_score",
            F.when(F.col("team_a_score").isin("0", "1", "2", "3"), F.col("team_a_score").cast("int"))
             .otherwise(None)
        ) \
        .withColumn(
            "team_a_score_flag",
            F.when(F.col("team_a_score").isin("0", "1", "2", "3"), "VALID")
             .otherwise("FLAGGED_INVALID_SCORE")
        ) \
        .withColumn(
            "clean_team_b_score",
            F.when(F.col("team_b_score").isin("0", "1", "2", "3"), F.col("team_b_score").cast("int"))
             .otherwise(None)
        ) \
        .withColumn(
            "team_b_score_flag",
            F.when(F.col("team_b_score").isin("0", "1", "2", "3"), "VALID")
             .otherwise("FLAGGED_INVALID_SCORE")
        )
        
    return df_processed

def clean_player_stats(stats_df):
    """
    Cleans player match stats:
    - Removes exact duplicate stat rows.
    - Safely parses kills and damage with range/negative-value validations.
    """
    stats_clean = stats_df.dropDuplicates()
    
    return stats_clean \
        .withColumn("parsed_kills", F.expr("try_cast(trim(kills) AS INT)")) \
        .withColumn(
            "clean_kills",
            F.when((F.col("parsed_kills").isNotNull()) & (F.col("parsed_kills") >= 0), F.col("parsed_kills"))
             .otherwise(None)
        ) \
        .withColumn(
            "kills_dq_status",
            F.when(F.col("parsed_kills").isNull(), "FLAGGED_UNPARSEABLE_KILLS")
             .when(F.col("parsed_kills") < 0, "FLAGGED_NEGATIVE_KILLS")
             .otherwise("VALID")
        ) \
        .withColumn("parsed_damage", F.expr("try_cast(trim(damage) AS DOUBLE)")) \
        .withColumn(
            "clean_damage",
            F.when((F.col("parsed_damage").isNotNull()) & (F.col("parsed_damage") >= 0), F.col("parsed_damage"))
             .otherwise(None)
        )



def clean_match_events(events_df):
    """
    Cleans match events:
    - Safely parses duration and timestamps.
    """
    return events_df \
        .dropDuplicates() \
        .withColumn("parsed_duration", F.expr("try_cast(trim(duration_seconds) AS INT)")) \
        .withColumn(
            "clean_duration_seconds",
            F.when((F.col("parsed_duration").isNotNull()) & (F.col("parsed_duration") >= 0), F.col("parsed_duration"))
             .otherwise(None)
        ) \
        .withColumn(
            "duration_dq_status",
            F.when(F.col("parsed_duration").isNull(), "FLAGGED_UNPARSEABLE_DURATION")
             .otherwise("VALID")
        ) \
        .withColumn("parsed_event_time", F.try_to_timestamp(F.trim(F.col("event_time")))) \
        .withColumn(
            "event_time_dq_status",
            F.when(F.col("parsed_event_time").isNull(), "FLAGGED_INVALID_EVENT_TIME")
             .otherwise("VALID")
        )