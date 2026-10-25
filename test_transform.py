
from pyspark.sql import SparkSession

from src.ingest import create_spark_session, load_all_datasets
from src.transform import transform_all_datasets


def main():
    spark = create_spark_session()

    try:
        # Step 1: Read the raw CSV files.
        raw_datasets = load_all_datasets(spark)

        # Step 2: Apply the transformations.
        transformed_datasets = transform_all_datasets(raw_datasets)

        # Step 3: Inspect the new columns and sample records.
        for name, df in transformed_datasets.items():
            print(f"\n{'=' * 60}")
            print(f"TRANSFORMED DATASET: {name}")
            print(f"{'=' * 60}")

            df.printSchema()
            df.show(5, truncate=False)

        # Focused checks for the important parsed fields.
        print("\nInvalid match timestamps:")
        transformed_datasets["matches"].filter(
            "parsed_match_time IS NULL"
        ).select(
            "match_id", "match_time", "parsed_match_time"
        ).show(truncate=False)

        print("\nUnparseable kill values:")
        transformed_datasets["player_match_stats"].filter(
            "kills_int IS NULL"
        ).select(
            "stat_id", "kills", "kills_int"
        ).show(10, truncate=False)

        print("\nInvalid event timestamps:")
        transformed_datasets["match_events"].filter(
            "parsed_event_time IS NULL"
        ).select(
            "event_id", "event_time", "parsed_event_time"
        ).show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
