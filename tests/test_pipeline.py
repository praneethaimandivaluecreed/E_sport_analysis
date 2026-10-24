import os
from pyspark.sql import SparkSession

def test_pipeline():
    # Initialize Spark Session (Lab PC environments usually have Hadoop/winutils configured natively)
    spark = SparkSession.builder \
        .appName("EsportsAnalytics-Test") \
        .master("local[*]") \
        .getOrCreate()
    
    print("=== SPARK SESSION INITIALIZED SUCCESSFULLY ===")
    
    data_path = "./data"
    output_dir = "./output/analytics"
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Quick Read Test
    print("Loading raw datasets for verification...")
    teams_df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{data_path}/teams.csv")
    players_df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{data_path}/players.csv")
    matches_df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{data_path}/matches.csv")
    stats_df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{data_path}/player_match_stats.csv")
    
    print(f"Teams Count: {teams_df.count()}")
    print(f"Players Count: {players_df.count()}")
    print(f"Matches Count: {matches_df.count()}")
    print(f"Player Stats Count: {stats_df.count()}")
    
    # 2. Basic Aggregation & Parquet Write Test
    print("Running sample aggregation & writing parquet output...")
    team_standings = teams_df.join(matches_df, teams_df.team_id == matches_df.team_a_id, "inner") \
        .groupBy("team_id", "team_name") \
        .count() \
        .withColumnRenamed("count", "matches_as_team_a")
    
    output_path = f"{output_dir}/test_team_standings.parquet"
    team_standings.write.mode("overwrite").parquet(output_path)
    
    print(f"SUCCESS: Parquet file written successfully to {output_path}")
    
    # 3. Read back verification
    verified_df = spark.read.parquet(output_path)
    print(f"Verified rows read from Parquet: {verified_df.count()}")
    verified_df.show(5, truncate=False)

if __name__ == "__main__":
    test_pipeline()