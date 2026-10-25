from pyspark.sql import SparkSession
from pathlib import Path

# Find the project folder and its outputs directory.
PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = PROJECT_ROOT / "outputs"

spark = (
    SparkSession.builder
    .appName("Esports-Read-Existing-Outputs")
    .master("local[*]")
    .getOrCreate()
)

# Read one Parquet dataset by passing its FOLDER path.
def read_parquet_folder(folder_path):
    df = spark.read.parquet(str(folder_path))

    print(f"\nDataset: {folder_path.name}")
    print(f"Rows: {df.count()}")
    print("Schema:")
    df.printSchema()

    df.show(10, truncate=False)

    return df


# First, let's inspect the main analytical datasets.
analytics_dir = OUTPUT_DIR / "analytics"

try:
    team_performance = read_parquet_folder(
        analytics_dir / "team_performance"
    )

    player_performance = read_parquet_folder(
        analytics_dir / "player_performance"
    )

    team_form = read_parquet_folder(
        analytics_dir / "team_form_over_time"
    )

finally:
    spark.stop()