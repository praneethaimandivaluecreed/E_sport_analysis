"""
Module: main.py
Description: Orchestrates the entire Esports Tournament Analytics pipeline, 
             from raw ingestion and cleaning to validation, analytics, 
             SQL execution, and Parquet persistence.
"""

import os
import tempfile
from pyspark.sql import functions as F
from ingest import create_spark_session, load_raw_datasets
from clean import clean_teams, clean_players, clean_matches, clean_player_stats, clean_match_events
from validate import isolate_match_quarantines, isolate_player_quarantines, isolate_stats_quarantines, isolate_event_quarantines
from transform import compute_team_standings, compute_player_analytics_and_windows, run_spark_sql_analyses, demonstrate_execution_plans, save_analytical_outputs

def main():
    # Set a dummy Hadoop home directory to prevent Windows winutils permission errors on Parquet write
    os.environ['HADOOP_HOME'] = tempfile.gettempdir()

    # 1. Initialize Spark Session
    spark = create_spark_session("EsportsTournamentPipeline")
    
    # Define paths
    data_path = "./data"
    output_dir = "./outputs"
    
    print("=== STEP 1: Loading Raw Datasets ===")
    teams_df, players_df, matches_df, stats_df, events_df = load_raw_datasets(spark, data_path)
    
    print("=== STEP 2: Performing Intrinsic Cleaning & Type Parsing ===")
    teams_clean = clean_teams(teams_df)
    players_clean = clean_players(players_df)
    matches_processed = clean_matches(matches_df)
    stats_processed = clean_player_stats(stats_df)
    events_processed = clean_match_events(events_df)
    
    print("=== STEP 3: Executing Business Rule Validations & Quarantines ===")
    # Match quarantines (self-matches & missing score completed matches)
    matches_valid, matches_score_quarantine, matches_self_quarantine = isolate_match_quarantines(matches_processed)
    
    # Player referential integrity quarantines (orphaned team references)
    players_valid, players_quarantine = isolate_player_quarantines(players_clean, teams_clean)
    
    # Player match stats quarantines (grain, ghost players, cancelled matches, team mismatches)
    stats_valid_final, stats_grain_quarantine, ghost_quarantine, stats_cancelled_quarantine, stats_mismatch_quarantine = isolate_stats_quarantines(
        stats_processed, players_valid, matches_valid, players_clean
    )
    
    # Match events quarantines (orphaned matches and implausible temporal windows)
    events_valid, events_orphaned_quarantine, events_window_quarantine = isolate_event_quarantines(events_processed, matches_valid)
    
    print(f"Valid Matches Count: {matches_valid.count()}")
    print(f"Valid Players Count: {players_valid.count()}")
    print(f"Valid Player Stats Count: {stats_valid_final.count()}")
    print(f"Valid Match Events Count: {events_valid.count()}")
    
    print("=== STEP 4: Running Core Analytics & Window Functions ===")
    team_standings = compute_team_standings(matches_valid)
    ranked_players = compute_player_analytics_and_windows(stats_valid_final, players_valid)
    
    print("=== STEP 5: Executing Spark SQL Analyses ===")
    run_spark_sql_analyses(spark, matches_valid, stats_valid_final, players_valid, teams_clean)
    
    print("=== STEP 6: Inspecting Execution Plans ===")
    demonstrate_execution_plans(team_standings, ranked_players)
    
    print("=== STEP 7: Saving Analytical Outputs to Parquet ===")
    save_analytical_outputs(team_standings, ranked_players, f"{output_dir}/analytics")
    
    print("Pipeline execution completed successfully! 🚀")

if __name__ == "__main__":
    main()