import os
from pyspark.sql import SparkSession

def create_spark_session(app_name="EsportsAnalytics"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
        .config("spark.hadoop.io.native.lib.available", "false") \
        .getOrCreate()
    
    # Force Hadoop configuration programmatically to bypass Windows winutils/permission checks
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    
    return spark

def load_raw_datasets(spark, data_path="./data"):
    teams_df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{data_path}/teams.csv")
    players_df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{data_path}/players.csv")
    matches_df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{data_path}/matches.csv")
    stats_df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{data_path}/player_match_stats.csv")
    events_df = spark.read.option("header", "true").option("inferSchema", "true").csv(f"{data_path}/match_events.csv")
    
    return teams_df, players_df, matches_df, stats_df, events_df