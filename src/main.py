
from pathlib import Path

from ingest import create_spark_session, load_all_datasets
from transform import transform_all_datasets
from validate import validate_all_datasets, show_issue_summary
from clean import clean_all_datasets

from analytics import (
    build_team_match_records,
    calculate_team_performance,
    calculate_player_performance,
    summarize_events_by_game_type,
    summarize_events_by_month,
    summarize_event_durations,
    summarize_event_exposure_by_team,
    calculate_team_form_over_time,
)

from sql_analytics import display_sql_analytics


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "outputs"


# ============================================================
# HELPER: SAVE DATAFRAME AS PARQUET
# ============================================================

def save_parquet(df, output_name: str) -> None:
    """Save a DataFrame to the outputs directory as Parquet."""

    output_path = OUTPUT_DIR / output_name

    (
        df.write
        .mode("overwrite")
        .parquet(str(output_path))
    )

    print(f"Saved Parquet output: {output_path}")


# ============================================================
# MAIN PIPELINE
# ============================================================

def main() -> None:
    spark = create_spark_session()

    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        # ----------------------------------------------------
        # STEP 1: INGEST RAW CSV DATASETS
        # ----------------------------------------------------
        print("\n" + "=" * 70)
        print("STEP 1: INGESTION")
        print("=" * 70)

        raw_data = load_all_datasets(spark)

        for name, df in raw_data.items():
            print(f"\nRaw dataset: {name}")
            print(f"Row count: {df.count()}")
            df.printSchema()

        # ----------------------------------------------------
        # STEP 2: TRANSFORM RAW DATA
        # ----------------------------------------------------
        print("\n" + "=" * 70)
        print("STEP 2: TRANSFORMATION")
        print("=" * 70)

        transformed_data = transform_all_datasets(raw_data)

        for name, df in transformed_data.items():
            print(f"{name}: {df.count()} transformed rows")

        # ----------------------------------------------------
        # STEP 3: DATA QUALITY VALIDATION
        # Validate before cleaning so source problems remain
        # visible in the quality report.
        # ----------------------------------------------------
        print("\n" + "=" * 70)
        print("STEP 3: DATA QUALITY VALIDATION")
        print("=" * 70)

        quality_issues = validate_all_datasets(transformed_data)

        show_issue_summary(quality_issues)

        # Save individual validation issue DataFrames.
        for issue_name, issue_df in quality_issues.items():
            save_parquet(
                issue_df,
                f"quality_issues/{issue_name}",
            )

        # ----------------------------------------------------
        # STEP 4: CLEANING AND QUARANTINE
        # clean_all_datasets returns nested dictionaries:
        #   cleaned_data["cleaned"]
        #   cleaned_data["quarantined"]
        # ----------------------------------------------------
        print("\n" + "=" * 70)
        print("STEP 4: CLEANING AND QUARANTINE")
        print("=" * 70)

        cleaning_result = clean_all_datasets(transformed_data)

        cleaned = cleaning_result["cleaned"]
        quarantined = cleaning_result["quarantined"]

        for name, df in cleaned.items():
            row_count = df.count()
            print(f"Cleaned {name}: {row_count} rows")

            save_parquet(
                df,
                f"cleaned/{name}",
            )

        for name, df in quarantined.items():
            row_count = df.count()
            print(f"Quarantined {name}: {row_count} rows")

            save_parquet(
                df,
                f"quarantined/{name}",
            )

        # ----------------------------------------------------
        # STEP 5: DATAFRAME ANALYTICS
        # ----------------------------------------------------
        print("\n" + "=" * 70)
        print("STEP 5: DATAFRAME ANALYTICS")
        print("=" * 70)

        team_match_records = build_team_match_records(
            cleaned["matches"]
        )

        team_performance = calculate_team_performance(
            team_match_records
        )

        player_performance = calculate_player_performance(
            cleaned["player_match_stats"],
            cleaned["matches"],
            cleaned["players"],
        )

        events_by_game_type = summarize_events_by_game_type(
            cleaned["match_events"],
            cleaned["matches"],
        )

        events_by_month = summarize_events_by_month(
            cleaned["match_events"],
            cleaned["matches"],
        )

        event_durations = summarize_event_durations(
            cleaned["match_events"],
            cleaned["matches"],
        )

        event_exposure_by_team = summarize_event_exposure_by_team(
            cleaned["match_events"],
            cleaned["matches"],
        )

        team_form = calculate_team_form_over_time(
            team_match_records
        )

        dataframe_results = {
            "team_performance": team_performance,
            "player_performance": player_performance,
            "events_by_game_type": events_by_game_type,
            "events_by_month": events_by_month,
            "event_durations": event_durations,
            "event_exposure_by_team": event_exposure_by_team,
            "team_form_over_time": team_form,
        }

        for name, df in dataframe_results.items():
            print(f"\n{'=' * 70}")
            print(name.replace("_", " ").upper())
            print("=" * 70)
            df.show(30, truncate=False)

            save_parquet(
                df,
                f"analytics/{name}",
            )

        # ----------------------------------------------------
        # STEP 6: SPARK SQL ANALYTICS
        # ----------------------------------------------------
        print("\n" + "=" * 70)
        print("STEP 6: SPARK SQL ANALYTICS")
        print("=" * 70)

        sql_results = display_sql_analytics(
            spark,
            cleaned,
        )

        for name, df in sql_results.items():
            save_parquet(
                df,
                f"sql_analytics/{name}",
            )

        # ----------------------------------------------------
        # STEP 7: READ BACK A PARQUET OUTPUT
        # ----------------------------------------------------
        print("\n" + "=" * 70)
        print("STEP 7: PARQUET READ-BACK CHECK")
        print("=" * 70)

        player_output_path = OUTPUT_DIR / "analytics/player_performance"

        player_parquet = spark.read.parquet(
            str(player_output_path)
        )

        print("Player performance Parquet schema:")
        player_parquet.printSchema()

        print(
            "Rows in saved player performance output:",
            player_parquet.count(),
        )

        player_parquet.show(10, truncate=False)

        print("\nPipeline completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
