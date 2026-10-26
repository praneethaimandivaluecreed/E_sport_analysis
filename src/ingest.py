from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


# project root directory
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# source csv files directory
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"


# defining schema for teams data
TEAMS_SCHEMA = StructType([
    StructField("team_id", StringType(), True),
    StructField("team_name", StringType(), True),
    StructField("region", StringType(), True),
    StructField("coach", StringType(), True),
])


# initially keeping all columns as StringType
# because some columns may contain invalid values during type conversion
# these values will be validated and handled in later stages

# defining schema for players data
PLAYERS_SCHEMA = StructType([
    StructField("player_id", StringType(), True),
    StructField("player_name", StringType(), True),
    StructField("team_id", StringType(), True),
    StructField("role", StringType(), True),
    StructField("join_date", StringType(), True),
    StructField("status", StringType(), True),
])

# defining schema for matches data
MATCHES_SCHEMA = StructType([
    StructField("match_id", StringType(), True),
    StructField("match_time", StringType(), True),
    StructField("game", StringType(), True),
    StructField("team_a_id", StringType(), True),
    StructField("team_b_id", StringType(), True),
    StructField("team_a_score", StringType(), True),
    StructField("team_b_score", StringType(), True),
    StructField("match_status", StringType(), True),
])

# defining schema for player match statistics
PLAYER_MATCH_STATS_SCHEMA = StructType([
    StructField("stat_id", StringType(), True),
    StructField("match_id", StringType(), True),
    StructField("player_id", StringType(), True),
    StructField("kills", StringType(), True),
    StructField("deaths", StringType(), True),
    StructField("assists", StringType(), True),
    StructField("damage", StringType(), True),
    StructField("rounds_played", StringType(), True),
])

# defining schema for match events
MATCH_EVENTS_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("match_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("duration_seconds", StringType(), True),
])


# creating spark session
def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("EsportsTournamentAnalytics")
        .master("local[*]")
        .getOrCreate()
    )

    # setting log level to show warnings and errors
    spark.sparkContext.setLogLevel("WARN")

    # returning the spark session created
    return spark


# reading csv data using the provided schema
# This is a common fxn to read all the csv files
def read_csv(spark, file_path, schema, dataset_name):

    # checking whether the csv file exists
    if not file_path.is_file():
        # raising an exception if file doesnt exist
        raise FileNotFoundError(
            f"Cannot find {dataset_name} CSV file: {file_path}"
        )

    # printing dataset  being read
    print(f"Reading {dataset_name}: {file_path}")

    # reading csv with an explicit schema
    df = (
        spark.read
        .format("csv")
        .schema(schema)
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("inferSchema", "false")
        .load(str(file_path))
    )

    return df


# fxn for reading team details from teams.csv
def read_teams(spark, data_dir=DEFAULT_DATA_DIR):

    # calling the common read_csv fxn 
    return read_csv(
        spark=spark,
        file_path=data_dir / "teams.csv",
        schema=TEAMS_SCHEMA,
        dataset_name="teams",
    )


# reading player details from players.csv
def read_players(spark, data_dir=DEFAULT_DATA_DIR):

    return read_csv(
        spark=spark,
        file_path=data_dir / "players.csv",
        schema=PLAYERS_SCHEMA,
        dataset_name="players",
    )


# reading match details from matches.csv
def read_matches(spark, data_dir=DEFAULT_DATA_DIR):

    return read_csv(
        spark=spark,
        file_path=data_dir / "matches.csv",
        schema=MATCHES_SCHEMA,
        dataset_name="matches",
    )


# reading player statistics from player_match_stats.csv
def read_player_match_stats(spark, data_dir=DEFAULT_DATA_DIR):

    return read_csv(
        spark=spark,
        file_path=data_dir / "player_match_stats.csv",
        schema=PLAYER_MATCH_STATS_SCHEMA,
        dataset_name="player_match_stats",
    )


# reading match events from match_events.csv
def read_match_events(spark, data_dir=DEFAULT_DATA_DIR):

    return read_csv(
        spark=spark,
        file_path=data_dir / "match_events.csv",
        schema=MATCH_EVENTS_SCHEMA,
        dataset_name="match_events",
    )


# loading all five datasets into a dictionary
def load_all_datasets(spark, data_dir=DEFAULT_DATA_DIR):

    datasets = {
        "teams": read_teams(spark, data_dir),
        "players": read_players(spark, data_dir),
        "matches": read_matches(spark, data_dir),
        "player_match_stats": read_player_match_stats(spark, data_dir),
        "match_events": read_match_events(spark, data_dir),
    }

    # logging successful dataset loading
    print("\nAll five datasets have been loaded.")

    #returning the dictionary of all the dataframes
    return datasets