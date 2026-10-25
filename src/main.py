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



PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Define the directory where pipeline outputs will be stored.
# All quality reports, cleaned datasets, quarantined records,
# and analytics results will be written under this directory.
OUTPUT_DIR = PROJECT_ROOT / "outputs"



# fxn to save a dataframe as a parquet
def save_parquet(df, output_name):

    # Combine the main output directory with the requested output name.
    # For example, "analytics/team_performance" becomes a path
    # inside the project's outputs directory.
    output_path = OUTPUT_DIR / output_name

    # Write the Spark DataFrame to Parquet format.
    # mode("overwrite") replaces existing output at this path.
    # parquet() writes the DataFrame using the Parquet file format.
    # str() converts the Path object to a string for Spark.
    (
        df.write
        .mode("overwrite")
        .parquet(str(output_path))
    )

    # Print the destination so we know where the DataFrame was saved.
    print(f"Saved Parquet output: {output_path}")


# START OF MAIN PIPELINE
def main():

    # Create the Spark session before performing any Spark operations.
    # The session provides the entry point for reading data,
    # transforming DataFrames, executing SQL, and writing outputs.
    spark = create_spark_session()

    # Use try/finally to ensure Spark is stopped even if an error occurs
    # during ingestion, transformation, validation, cleaning, or analytics.
    try:

        # Create the outputs directory if it does not already exist.
        # parents=True creates missing parent directories when necessary.
        # exist_ok=True prevents an error if the directory already exists.
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


        # INGEST RAW CSV DATASETS
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

            # Display which raw dataset is being inspected.
            print(f"\nRaw dataset: {name}")

            # Count the number of records in this dataset.
            # count() is an action, so Spark executes the work needed
            # to calculate the row count.
            print(f"Row count: {df.count()}")

            # Display the column names, data types, and nullability information.
            # This helps us inspect the schema Spark inferred or received.
            df.printSchema()

        
        # TRANSFORM RAW DATA

        # Print the heading for the transformation stage.
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

            # count() evaluates the DataFrame to determine its row count.
            print(f"{name}: {df.count()} transformed rows")


        # STEP 3: DATA QUALITY VALIDATION
        # Validate before cleaning so source problems remain
        # visible in the quality report.
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

        # Display a summary of the validation issues.
        # The implementation of this summary is defined in validate.py.
        show_issue_summary(quality_issues)

        # Save each validation issue DataFrame separately.
        # issue_name identifies the issue, and issue_df contains its records.
        for issue_name, issue_df in quality_issues.items():

            # Save the issue DataFrame inside the quality_issues directory.
            # Each issue receives its own output path.
            save_parquet(
                issue_df,
                f"quality_issues/{issue_name}",
            )

  
        # CLEANING AND QUARANTINE
        # clean_all_datasets returns nested dictionaries:
        #   cleaning_result["cleaned"]
        #   cleaning_result["quarantined"]

        # Print the heading for the cleaning and quarantine stage.
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
            print(f"Cleaned {name}: {row_count} rows")

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
            print(f"Quarantined {name}: {row_count} rows")

            # Save each quarantined DataFrame separately.
            # Keeping quarantine outputs separate preserves access to
            # records that were not retained in the cleaned datasets.
            save_parquet(
                df,
                f"quarantined/{name}",
            )


        #DATAFRAME ANALYTICS
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

            # Print a separator and a readable heading for the result.
            print(f"\n{'=' * 70}")
            print(name.replace("_", " ").upper())
            print("=" * 70)

            # Display the first 30 rows of the result.
            # truncate=False keeps long column values from being shortened.
            df.show(30, truncate=False)

            # Save this analytics result to its own Parquet output path.
            # For example, team_performance is saved under analytics/team_performance.
            save_parquet(
                df,
                f"analytics/{name}",
            )


        # STEP 6: SPARK SQL ANALYTICS
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

            # Use the result name to create its output path.
            save_parquet(
                df,
                f"sql_analytics/{name}",
            )

        #  READ BACK A PARQUET OUTPUT

        # Print the heading for the Parquet read-back check.
        print("\n" + "=" * 70)
        print("STEP 7: PARQUET READ-BACK CHECK")
        print("=" * 70)

        # Build the path to the saved player performance analytics output.
        # This is the same output location used in STEP 5 when saving
        # the player_performance DataFrame.
        player_output_path = OUTPUT_DIR / "analytics/player_performance"

        # Read the saved Parquet output back into a new Spark DataFrame.
        # This checks that Spark can access the saved output.
        player_parquet = spark.read.parquet(
            str(player_output_path)
        )

        # Display the schema of the DataFrame read from Parquet.
        # This allows us to inspect the saved column names and data types.
        print("Player performance Parquet schema:")
        player_parquet.printSchema()

        # Count the records in the saved Parquet output.
        # This lets us inspect how many rows were written and can be read back.
        print(
            "Rows in saved player performance output:",
            player_parquet.count(),
        )

        # Display the first 10 rows from the saved Parquet output.
        # This gives a quick visual check of the stored records.
        player_parquet.show(10, truncate=False)

        # Print the success message after all pipeline steps complete.
        print("\nPipeline completed successfully.")

    # Always stop Spark when execution leaves the try block,
    # whether the pipeline finishes successfully or an exception occurs.
    # This releases Spark resources and shuts down the Spark session.
    finally:
        spark.stop()


if __name__ == "__main__":
    main()