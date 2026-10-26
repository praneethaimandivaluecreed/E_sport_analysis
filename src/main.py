
from pathlib import Path
from datetime import datetime
from contextlib import redirect_stdout
from io import StringIO
import logging

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
LOG_DIR = PROJECT_ROOT / "logs"

LOG_DIR.mkdir(parents=True, exist_ok=True)

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
LOG_FILE = LOG_DIR / f"pipeline_{timestamp}.log"


# ============================================================
# LOGGING CONFIGURATION
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
    force=True,
)

logger = logging.getLogger(__name__)

logger.info("Logging initialized")
logger.info("Log file: %s", LOG_FILE)


# ============================================================
# HELPER: LOG DATAFRAME SCHEMA
# ============================================================

def log_schema(df, label: str) -> None:
    """Write a readable DataFrame schema to the terminal and log file."""

    schema_text = df.schema.treeString()

    logger.info(
        "%s schema:\n%s",
        label,
        schema_text,
    )


# ============================================================
# HELPER: LOG DATAFRAME TABLE
# ============================================================

def log_dataframe(df, label: str, n: int = 20,
                  truncate: bool = False) -> None:
    """
    Capture df.show() output and write it through the logger.

    The table will appear in both the terminal and the log file.
    Only the first n rows are displayed.
    """

    buffer = StringIO()

    with redirect_stdout(buffer):
        df.show(n=n, truncate=truncate)

    table_output = buffer.getvalue().rstrip()

    logger.info(
        "%s | Displaying up to %s rows:\n%s",
        label,
        n,
        table_output if table_output else "(No table output)",
    )


# ============================================================
# HELPER: LOG VALIDATION SUMMARY
# ============================================================

def log_validation_summary(quality_issues: dict) -> None:
    """Log the number of rows flagged by each quality check."""

    logger.info("=" * 60)
    logger.info("DATA QUALITY ISSUE SUMMARY")
    logger.info("=" * 60)

    total_issue_rows = 0

    for issue_name, issue_df in quality_issues.items():
        issue_count = issue_df.count()
        total_issue_rows += issue_count

        if issue_count > 0:
            logger.warning(
                "Quality check '%s': %s flagged rows",
                issue_name,
                issue_count,
            )
        else:
            logger.info(
                "Quality check '%s': no flagged rows",
                issue_name,
            )

    logger.info(
        "Sum of flagged rows across quality checks: %s",
        total_issue_rows,
    )
    logger.info(
        "Note: this is a sum across checks; a record may be flagged "
        "by more than one check."
    )


# ============================================================
# HELPER: CAPTURE EXISTING VALIDATION SUMMARY
# ============================================================

def log_existing_issue_summary(quality_issues: dict) -> None:
    """
    Capture the existing show_issue_summary() output.

    This preserves the existing validation-summary function.
    """

    buffer = StringIO()

    with redirect_stdout(buffer):
        show_issue_summary(quality_issues)

    summary_output = buffer.getvalue().rstrip()

    if summary_output:
        logger.info(
            "Validation summary output:\n%s",
            summary_output,
        )
    else:
        logger.info(
            "The existing validation summary produced no stdout output."
        )


# ============================================================
# HELPER: SAVE DATAFRAME AS PARQUET
# ============================================================

def save_parquet(df, output_name: str) -> None:
    """Save a DataFrame to the outputs directory as Parquet."""

    output_path = OUTPUT_DIR / output_name

    logger.info("Saving Parquet output: %s", output_path)

    try:
        (
            df.write
            .mode("overwrite")
            .parquet(str(output_path))
        )

        logger.info(
            "Successfully saved Parquet output: %s",
            output_path,
        )

    except Exception:
        logger.exception(
            "Failed to save Parquet output: %s",
            output_path,
        )
        raise


# ============================================================
# MAIN PIPELINE
# ============================================================

def main() -> None:
    spark = None

    try:
        logger.info("=" * 70)
        logger.info("STARTING E-SPORT ANALYSIS PIPELINE")
        logger.info("=" * 70)

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        spark = create_spark_session()
        logger.info("Spark session created successfully")

        # ----------------------------------------------------
        # STEP 1: INGEST RAW CSV DATASETS
        # ----------------------------------------------------
        logger.info("STEP 1: INGESTION")

        raw_data = load_all_datasets(spark)

        for name, df in raw_data.items():
            row_count = df.count()

            logger.info(
                "Raw dataset: %s | Rows: %s",
                name,
                row_count,
            )

            log_schema(df, f"Raw dataset '{name}'")

        # ----------------------------------------------------
        # STEP 2: TRANSFORM RAW DATA
        # ----------------------------------------------------
        logger.info("STEP 2: TRANSFORMATION")

        transformed_data = transform_all_datasets(raw_data)

        for name, df in transformed_data.items():
            row_count = df.count()

            logger.info(
                "Transformed dataset: %s | Rows: %s",
                name,
                row_count,
            )

            log_schema(df, f"Transformed dataset '{name}'")

        # ----------------------------------------------------
        # STEP 3: DATA QUALITY VALIDATION
        # ----------------------------------------------------
        logger.info("STEP 3: DATA QUALITY VALIDATION")

        quality_issues = validate_all_datasets(transformed_data)

        # Capture the summary produced by your existing function.
        log_existing_issue_summary(quality_issues)

        # Log a structured count for every quality check.
        log_validation_summary(quality_issues)

        # Save each quality issue DataFrame.
        for issue_name, issue_df in quality_issues.items():
            save_parquet(
                issue_df,
                f"quality_issues/{issue_name}",
            )

        logger.info("Data quality validation completed")

        # ----------------------------------------------------
        # STEP 4: CLEANING AND QUARANTINE
        # ----------------------------------------------------
        logger.info("STEP 4: CLEANING AND QUARANTINE")

        cleaning_result = clean_all_datasets(transformed_data)

        cleaned = cleaning_result["cleaned"]
        quarantined = cleaning_result["quarantined"]

        for name, df in cleaned.items():
            row_count = df.count()

            logger.info(
                "Cleaned dataset: %s | Rows: %s",
                name,
                row_count,
            )

            save_parquet(
                df,
                f"cleaned/{name}",
            )

        for name, df in quarantined.items():
            row_count = df.count()

            if row_count > 0:
                logger.warning(
                    "Quarantined dataset: %s | Rows: %s",
                    name,
                    row_count,
                )
            else:
                logger.info(
                    "Quarantined dataset: %s | Rows: 0",
                    name,
                )

            save_parquet(
                df,
                f"quarantined/{name}",
            )

        logger.info("Cleaning and quarantine completed")

        # ----------------------------------------------------
        # STEP 5: DATAFRAME ANALYTICS
        # ----------------------------------------------------
        logger.info("STEP 5: DATAFRAME ANALYTICS")

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
            logger.info(
                "DATAFRAME ANALYTICS RESULT: %s",
                name,
            )

            log_schema(df, f"Analytics result '{name}'")
            log_dataframe(
                df,
                label=f"Analytics result '{name}'",
                n=20,
                truncate=False,
            )

            row_count = df.count()

            logger.info(
                "Analytics result '%s' | Total rows: %s",
                name,
                row_count,
            )

            save_parquet(
                df,
                f"analytics/{name}",
            )

        logger.info("DataFrame analytics completed")

        # ----------------------------------------------------
        # STEP 6: SPARK SQL ANALYTICS
        # ----------------------------------------------------
        logger.info("STEP 6: SPARK SQL ANALYTICS")

        sql_results = display_sql_analytics(
            spark,
            cleaned,
        )

        for name, df in sql_results.items():
            logger.info(
                "SPARK SQL ANALYTICS RESULT: %s",
                name,
            )

            log_schema(df, f"SQL analytics result '{name}'")
            log_dataframe(
                df,
                label=f"SQL analytics result '{name}'",
                n=20,
                truncate=False,
            )

            row_count = df.count()

            logger.info(
                "SQL analytics result '%s' | Total rows: %s",
                name,
                row_count,
            )

            save_parquet(
                df,
                f"sql_analytics/{name}",
            )

        logger.info("Spark SQL analytics completed")

        # ----------------------------------------------------
        # STEP 7: READ BACK A PARQUET OUTPUT
        # ----------------------------------------------------
        logger.info("STEP 7: PARQUET READ-BACK CHECK")

        player_output_path = (
            OUTPUT_DIR / "analytics/player_performance"
        )

        player_parquet = spark.read.parquet(
            str(player_output_path)
        )

        log_schema(
            player_parquet,
            "Player performance Parquet read-back",
        )

        row_count = player_parquet.count()

        logger.info(
            "Rows in saved player performance output: %s",
            row_count,
        )

        log_dataframe(
            player_parquet,
            label="Player performance Parquet read-back",
            n=10,
            truncate=False,
        )

        logger.info("=" * 70)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("Log file: %s", LOG_FILE)
        logger.info("=" * 70)

    except Exception:
        logger.exception("PIPELINE FAILED")
        raise

    finally:
        if spark is not None:
            logger.info("Stopping Spark session")

            try:
                spark.stop()
                logger.info("Spark session stopped")
            except Exception:
                logger.exception(
                    "Error while stopping Spark session"
                )


if __name__ == "__main__":
    main()
