
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType


# ---------------------------------------------------------
# 1. PROJECT PATHS
# ---------------------------------------------------------

# This file is inside the src folder.
# parents[1] takes us to the project root: E_sport_analysis.
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# All five source CSV files are stored in the data folder.
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"


# ---------------------------------------------------------
# 2. EXPLICIT SOURCE SCHEMAS
# ---------------------------------------------------------

# We initially read every source column as a string.
#
# Why?
# Our data contains values such as "unknown", "N/A",
# "TBD", "??", and negative numbers where they may be invalid.
#
# Keeping raw values as strings lets later functions decide
# which values are valid and how they should be converted.
#
# These schemas also define the expected column names and order.

TEAMS_SCHEMA = StructType([
    StructField("team_id", StringType(), True),
    StructField("team_name", StringType(), True),
    StructField("region", StringType(), True),
    StructField("coach", StringType(), True),
])

PLAYERS_SCHEMA = StructType([
    StructField("player_id", StringType(), True),
    StructField("player_name", StringType(), True),
    StructField("team_id", StringType(), True),
    StructField("role", StringType(), True),
    StructField("join_date", StringType(), True),
    StructField("status", StringType(), True),
])

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

MATCH_EVENTS_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("match_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("duration_seconds", StringType(), True),
])


# ---------------------------------------------------------
# 3. CREATE THE SPARK SESSION
# ---------------------------------------------------------

def create_spark_session() -> SparkSession:
    """
    Create and return the Spark session used by this project.

    local[*] means Spark runs on this computer and can use
    the available logical CPU cores.

    getOrCreate() reuses an existing session when possible,
    instead of unnecessarily creating another one.
    """

    spark = (
        SparkSession.builder
        .appName("EsportsTournamentAnalytics")
        .master("local[*]")
        .getOrCreate()
    )

    # Show useful information while developing the pipeline.
    spark.sparkContext.setLogLevel("WARN")

    return spark


# ---------------------------------------------------------
# 4. REUSABLE CSV READER
# ---------------------------------------------------------

def read_csv(
    spark: SparkSession,
    file_path: Path,
    schema: StructType,
    dataset_name: str,
) -> DataFrame:
    """
    Read one CSV file using its explicitly defined schema.

    Parameters:
        spark:
            The active Spark session.

        file_path:
            Location of the CSV file.

        schema:
            Expected column names and data types.

        dataset_name:
            Friendly name used in messages.

    Returns:
        A Spark DataFrame containing the source records.

    Important:
        This function does not drop duplicates, replace
        placeholders, or remove invalid business records.
    """

    # Give a clear error before Spark tries to read a
    # file that does not exist.
    if not file_path.is_file():
        raise FileNotFoundError(
            f"Cannot find {dataset_name} CSV file: {file_path}"
        )

    print(f"Reading {dataset_name}: {file_path}")

    # Explicit schema prevents Spark from guessing column types.
    # PERMISSIVE mode allows Spark to attempt to read malformed
    # input instead of immediately failing the whole job.
    #
    # Placeholder strings are deliberately not configured as
    # null values, so values like "unknown" remain visible.
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


# ---------------------------------------------------------
# 5. INDIVIDUAL DATASET READERS
# ---------------------------------------------------------

def read_teams(spark: SparkSession, data_dir: Path = DEFAULT_DATA_DIR) -> DataFrame:
    """Read team details from teams.csv."""

    return read_csv(
        spark=spark,
        file_path=data_dir / "teams.csv",
        schema=TEAMS_SCHEMA,
        dataset_name="teams",
    )


def read_players(spark: SparkSession, data_dir: Path = DEFAULT_DATA_DIR) -> DataFrame:
    """Read player details from players.csv."""

    return read_csv(
        spark=spark,
        file_path=data_dir / "players.csv",
        schema=PLAYERS_SCHEMA,
        dataset_name="players",
    )


def read_matches(spark: SparkSession, data_dir: Path = DEFAULT_DATA_DIR) -> DataFrame:
    """Read match details and raw scores from matches.csv."""

    return read_csv(
        spark=spark,
        file_path=data_dir / "matches.csv",
        schema=MATCHES_SCHEMA,
        dataset_name="matches",
    )


def read_player_match_stats(
    spark: SparkSession,
    data_dir: Path = DEFAULT_DATA_DIR,
) -> DataFrame:
    """Read player statistics for individual matches."""

    return read_csv(
        spark=spark,
        file_path=data_dir / "player_match_stats.csv",
        schema=PLAYER_MATCH_STATS_SCHEMA,
        dataset_name="player_match_stats",
    )


def read_match_events(
    spark: SparkSession,
    data_dir: Path = DEFAULT_DATA_DIR,
) -> DataFrame:
    """Read events associated with tournament matches."""

    return read_csv(
        spark=spark,
        file_path=data_dir / "match_events.csv",
        schema=MATCH_EVENTS_SCHEMA,
        dataset_name="match_events",
    )


# ---------------------------------------------------------
# 6. LOAD ALL DATASETS
# ---------------------------------------------------------

def load_all_datasets(
    spark: SparkSession,
    data_dir: Path = DEFAULT_DATA_DIR,
) -> dict[str, DataFrame]:
    """
    Load all five source datasets and return them in a dictionary.

    The dictionary lets other modules access each DataFrame
    by a clear name without repeating the reading logic.
    """

    datasets = {
        "teams": read_teams(spark, data_dir),
        "players": read_players(spark, data_dir),
        "matches": read_matches(spark, data_dir),
        "player_match_stats": read_player_match_stats(spark, data_dir),
        "match_events": read_match_events(spark, data_dir),
    }

    print("\nAll five datasets have been loaded.")

    return datasets
