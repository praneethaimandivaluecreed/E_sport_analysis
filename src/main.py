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



PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Define the directory where pipeline outputs will be stored.
# All quality reports, cleaned datasets, quarantined records,
# and analytics results will be written under this directory.
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



# fxn to save a dataframe as a parquet
def save_parquet(df, output_name):

    # Combine the main output directory with the requested output name.
    # For example, "analytics/team_performance" becomes a path
    # inside the project's outputs directory.
    output_path = OUTPUT_DIR / output_name

    (
        df.write
        .mode("overwrite")
        .parquet(str(output_path))
    )

    print(f"Saved Parquet output: {output_path}")


# START OF MAIN PIPELINE
def main():

def main() -> None:
    spark = create_spark_session()

    # Use try/finally to ensure Spark is stopped even if an error occurs
    # during ingestion, transformation, validation, cleaning, or analytics.
    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        spark = create_spark_session()
        logger.info("Spark session created successfully")

        # ----------------------------------------------------
        # STEP 1: INGEST RAW CSV DATASETS
        # ----------------------------------------------------
        print("\n" + "=" * 70)
        print("STEP 1: INGESTION")
        print("=" * 70)

        # Read all configured source datasets into Spark DataFrames.
        # The returned dictionary contains each dataset under its name.
        # These are the raw inputs before the transformation and cleaning steps.
        raw_data = load_all_datasets(spark)

        # Inspect every raw dataset before proceeding.
        # name is the dataset name, and df is its corresponding DataFrame.
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

        # Apply the transformations defined in transform.py.
        # The raw_data dictionary is passed into the transformation function.
        # The returned dictionary contains the transformed DataFrames.
        # The exact transformations are controlled by transform_all_datasets().
        transformed_data = transform_all_datasets(raw_data)

        # Report the number of rows in every transformed dataset.
        # This helps us inspect the output of the transformation stage.
        for name, df in transformed_data.items():
            print(f"{name}: {df.count()} transformed rows")


        # STEP 3: DATA QUALITY VALIDATION
        # Validate before cleaning so source problems remain
        # visible in the quality report.
        # ----------------------------------------------------
        print("\n" + "=" * 70)
        print("STEP 3: DATA QUALITY VALIDATION")
        print("=" * 70)

        # Run the configured data quality checks on the transformed datasets.
        # The function returns a dictionary containing issue DataFrames.
        # Each entry represents a particular validation issue or check.
        #
        # Validation happens before cleaning so that problems can be
        # identified and reported before cleaning changes the data.
        quality_issues = validate_all_datasets(transformed_data)

        show_issue_summary(quality_issues)

        # Save individual validation issue DataFrames.
        for issue_name, issue_df in quality_issues.items():

            # Save the issue DataFrame inside the quality_issues directory.
            # Each issue receives its own output path.
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

        # Apply the cleaning rules defined in clean.py.
        # The input is transformed_data, so cleaning operates on
        # the transformed datasets rather than the original raw inputs.
        #
        # The function returns a dictionary containing two groups:
        # cleaned datasets and quarantined records.
        # The exact cleaning and quarantine rules are defined in clean.py.
        cleaning_result = clean_all_datasets(transformed_data)

        # Extract the successfully cleaned DataFrames.
        # These are the datasets used by the downstream analytics.
        cleaned = cleaning_result["cleaned"]

        # Extract the quarantined DataFrames.
        # These contain records separated by the cleaning process.
        quarantined = cleaning_result["quarantined"]

        # Process every cleaned dataset.
        for name, df in cleaned.items():

            # Count the records remaining in the cleaned DataFrame.
            # This helps us inspect the size of each cleaned dataset.
            row_count = df.count()

            logger.info(
                "Cleaned dataset: %s | Rows: %s",
                name,
                row_count,
            )

            # Save each cleaned dataset to its own Parquet output directory.
            # For example, the matches DataFrame is saved under cleaned/matches.
            save_parquet(
                df,
                f"cleaned/{name}",
            )

        # Process every quarantined dataset.
        for name, df in quarantined.items():

            # Count the records separated into quarantine.
            # This helps us understand how many records were quarantined.
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

            # Save each quarantined DataFrame separately.
            # Keeping quarantine outputs separate preserves access to
            # records that were not retained in the cleaned datasets.
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

        # Build team-level match records from the cleaned matches DataFrame.
        # This function prepares a team-versus-opponent perspective
        # for the team performance and team form calculations.
        team_match_records = build_team_match_records(
            cleaned["matches"]
        )

        # Calculate team performance using the team-match records.
        # The returned DataFrame contains the performance summary
        # calculated by calculate_team_performance().
        team_performance = calculate_team_performance(
            team_match_records
        )

        # Calculate player performance using three cleaned datasets:
        # 1. player_match_stats contains player statistics for matches.
        # 2. matches provides match details and completion status.
        # 3. players provides player details such as names, teams, and roles.
        #
        # Passing these datasets allows the function to connect player
        # statistics with the relevant match and player information.
        player_performance = calculate_player_performance(
            cleaned["player_match_stats"],
            cleaned["matches"],
            cleaned["players"],
        )

        # Summarize event counts by game type and event type.
        # The event data provides the events, while matches supplies
        # the associated game information.
        events_by_game_type = summarize_events_by_game_type(
            cleaned["match_events"],
            cleaned["matches"],
        )

        # Summarize event counts by month.
        # The function uses event timestamps and match information
        # according to the logic defined in analytics.py.
        events_by_month = summarize_events_by_month(
            cleaned["match_events"],
            cleaned["matches"],
        )

        # Summarize event duration statistics.
        # The function calculates the duration measures defined in analytics.py.
        # Event duration is not the same as the total duration of a match.
        event_durations = summarize_event_durations(
            cleaned["match_events"],
            cleaned["matches"],
        )

        # Summarize event exposure by team.
        # This function associates events with teams involved in the matches
        # and produces the team-level event summary.
        event_exposure_by_team = summarize_event_exposure_by_team(
            cleaned["match_events"],
            cleaned["matches"],
        )

        # Calculate team form over time using the team-match records.
        # The function applies the chronological and rolling calculations
        # defined in analytics.py.
        team_form = calculate_team_form_over_time(
            team_match_records
        )

        # Collect all DataFrame analytics results in one dictionary.
        # Each key provides a descriptive name for the output.
        # Each value is the corresponding result DataFrame.
        
        # Keeping the results together makes it easier to display and
        # save all analytics using one loop instead of repeating code.
        dataframe_results = {
            "team_performance": team_performance,
            "player_performance": player_performance,
            "events_by_game_type": events_by_game_type,
            "events_by_month": events_by_month,
            "event_durations": event_durations,
            "event_exposure_by_team": event_exposure_by_team,
            "team_form_over_time": team_form,
        }

        # Loop through every analytics result.
        # name is the output name, and df is the result DataFrame.
        for name, df in dataframe_results.items():
            print(f"\n{'=' * 70}")
            print(name.replace("_", " ").upper())
            print("=" * 70)
            df.show(30, truncate=False)

            # Save this analytics result to its own Parquet output path.
            # For example, team_performance is saved under analytics/team_performance.
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

        # Run and display the SQL analyses using the Spark session
        # and the cleaned datasets.
        #
        # Inside display_sql_analytics(), the SQL views are registered,
        # the SQL queries are executed, their results are displayed,
        # and the match outcome counts are checked against DataFrame results.
        #
        # If the validation fails, the function raises an AssertionError.
        # Otherwise, it returns the SQL result DataFrames in a dictionary.
        sql_results = display_sql_analytics(
            spark,
            cleaned,
        )

        # Save every SQL analytics result to Parquet.
        # Each result is stored separately under sql_analytics.
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

        # Read the saved Parquet output back into a new Spark DataFrame.
        # This checks that Spark can access the saved output.
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

    # Always stop Spark when execution leaves the try block,
    # whether the pipeline finishes successfully or an exception occurs.
    # This releases Spark resources and shuts down the Spark session.
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
