from pyspark.sql import DataFrame, SparkSession, functions as F


# fxn to register temporary views

def register_sql_views(cleaned):

    # Map each key in the cleaned dictionary to the SQL view name
    # that will be used to query the corresponding DataFrame.
    # The dictionary key identifies the DataFrame in Python.
    # The view name identifies the table-like object in Spark SQL.
    view_mapping = {
        "teams": "teams",
        "players": "players",
        "matches": "matches",
        "player_match_stats": "player_match_stats",
        "match_events": "match_events",
    }

    # Process every required DataFrame and register it as a SQL view.
    # dataframe_key is the key used to access the DataFrame.
    # view_name is the name that SQL queries will use.
    for dataframe_key, view_name in view_mapping.items():

        # Check that the expected DataFrame exists in the dictionary.
        # Raise an error immediately if a required dataset is missing,
        # instead of allowing a confusing error later in the SQL query.
        if dataframe_key not in cleaned:
            raise KeyError(
                f"Missing '{dataframe_key}' in the cleaned DataFrames."
            )

        # Register the DataFrame as a temporary SQL view.
        # SQL queries can now refer to it using its view_name.
        # If a view with this name already exists, replace it.
        # These views are temporary and belong to the current Spark session.
        cleaned[dataframe_key].createOrReplaceTempView(view_name)

    # Confirm that all required views have been registered successfully.
    print("Spark SQL temporary views registered successfully.")



# fxn to analyze match outcomes based on the game
def analyze_match_outcomes_by_game(spark):

    # Store the SQL query in a Python string.
    # spark.sql(query) will execute this query against the registered views.
    query = """
        SELECT
            game,

            -- Count every retained match for each game.
            -- This includes completed, cancelled, and forfeited matches.
            COUNT(*) AS total_matches,

            -- Count completed matches.
            -- CASE returns 1 for a completed match and 0 for every other status.
            -- SUM adds these values to produce the completed-match count.
            SUM(
                CASE WHEN status = 'completed'
                     THEN 1 ELSE 0 END
            ) AS completed_matches,

            -- Count cancelled matches using the same conditional-counting method.
            SUM(
                CASE WHEN status = 'cancelled'
                     THEN 1 ELSE 0 END
            ) AS cancelled_matches,

            -- Count forfeited matches.
            SUM(
                CASE WHEN status = 'forfeit'
                     THEN 1 ELSE 0 END
            ) AS forfeited_matches,

            -- Calculate the percentage of all matches that were completed.
            -- Multiply by 100 to convert the fraction into a percentage.
            -- ROUND(..., 2) keeps two digits after the decimal point.
            -- COUNT(*) is the denominator, so all retained matches are included.
            ROUND(
                100.0 * SUM(
                    CASE WHEN status = 'completed'
                         THEN 1 ELSE 0 END
                ) / COUNT(*),
                2
            ) AS completion_rate_pct,

            -- Calculate the percentage of matches that were cancelled.
            ROUND(
                100.0 * SUM(
                    CASE WHEN status = 'cancelled'
                         THEN 1 ELSE 0 END
                ) / COUNT(*),
                2
            ) AS cancellation_rate_pct,

            -- Calculate the percentage of matches that ended by forfeit.
            ROUND(
                100.0 * SUM(
                    CASE WHEN status = 'forfeit'
                         THEN 1 ELSE 0 END
                ) / COUNT(*),
                2
            ) AS forfeit_rate_pct

        -- Read the cleaned match records from the registered SQL view.
        FROM matches

        -- Produce one summary row for each game.
        GROUP BY game

        -- Show games with the highest cancellation percentage first.
        ORDER BY cancellation_rate_pct DESC
    """

    # Execute the SQL query and return its result as a Spark DataFrame.
    # Returning the DataFrame lets the caller display it, test it,
    # or write the result to storage later.
    return spark.sql(query)



# fxn to analyze events by game
def analyze_event_intensity_by_game(spark):


    query = """
        WITH event_counts AS (

            -- First, count the event records belonging to each match.
            SELECT
                match_id,
                COUNT(*) AS event_count

            -- Read event records from the registered match_events view.
            FROM match_events

            -- Ignore event rows without a match ID because they cannot
            -- be associated with a particular match.
            WHERE match_id IS NOT NULL

            -- Create one row per match_id with its total number of events.
            GROUP BY match_id
        ),

        match_event_counts AS (

            -- Start with the cleaned match list.
            -- This ensures matches without events are still included.
            SELECT
                m.match_id,
                m.game,
                m.status,

                -- If a match has no matching row in event_counts,
                -- the joined event_count will be NULL.
                -- COALESCE changes that missing count to zero because
                -- no matching event records were found for that match.
                COALESCE(e.event_count, 0) AS event_count

            FROM matches m

            -- Keep every match, even when it has no recorded events.
            -- A LEFT JOIN preserves all rows from matches.
            LEFT JOIN event_counts e
                ON m.match_id = e.match_id
        )

        -- Summarize match and event information for each game.
        SELECT
            game,

            -- Count all retained matches in the game.
            COUNT(*) AS total_matches,

            -- Add the event counts across all matches in the game.
            SUM(event_count) AS total_events,

            -- Calculate the average number of events per match.
            -- Matches with no recorded events contribute zero to this average.
            ROUND(
                AVG(event_count),
                2
            ) AS avg_events_per_match,

            -- Count matches whose event count is zero.
            SUM(
                CASE WHEN event_count = 0
                     THEN 1 ELSE 0 END
            ) AS matches_without_events,

            -- Calculate the percentage of matches with zero recorded events.
            -- The denominator includes every retained match for that game.
            ROUND(
                100.0 * SUM(
                    CASE WHEN event_count = 0
                         THEN 1 ELSE 0 END
                ) / COUNT(*),
                2
            ) AS pct_matches_without_events

        -- Use the intermediate result containing one row per match.
        FROM match_event_counts

        -- Produce one summary row for each game.
        GROUP BY game

        -- Display games with the highest average event count first.
        ORDER BY avg_events_per_match DESC
    """

    # Execute the query and return the resulting DataFrame.
    return spark.sql(query)



# fxn to analyze player performance by role
def analyze_role_performance_by_game(spark):

    # This query first prepares eligible player-match records.
    # It then groups those records by game and player role to calculate
    # performance metrics for each role within each game.
    query = """
        WITH eligible_stats AS (

            -- Select the columns required for the role analysis.
            SELECT
                s.match_id,
                s.player_id,
                p.role,
                m.game,

                -- Use the already-cleaned numeric metric columns.
                s.kills_int,
                s.assists_int,
                s.deaths_int,
                s.damage_int,
                s.rounds_played_int

            -- Start with the player statistics recorded for each match.
            FROM player_match_stats s

            -- Match each statistics record to its corresponding match.
            -- An INNER JOIN keeps only records with a matching match_id.
            INNER JOIN matches m
                ON s.match_id = m.match_id

            -- Attach each player's role using player_id.
            -- An INNER JOIN keeps only records with a matching player.
            INNER JOIN players p
                ON s.player_id = p.player_id

            -- Include only statistics from completed matches.
            WHERE m.status = 'completed'

              -- Exclude records where the player's role is missing.
              -- This allows the results to be grouped by a known role.
              AND p.role IS NOT NULL
        )

        -- Aggregate eligible player-match records by game and role.
        SELECT
            game,
            role,

            -- Count eligible rows, not necessarily unique players.
            -- A player can have multiple rows if they played multiple matches.
            COUNT(*) AS player_match_rows,

            -- Count distinct player IDs to determine how many unique
            -- players contributed records to this game-role combination.
            COUNT(DISTINCT player_id) AS unique_players,

            -- Count rows with non-negative kill values.
            -- NULL kill values do not satisfy the condition and are excluded.
            COUNT(
                CASE
                    WHEN kills_int >= 0 THEN 1
                END
            ) AS valid_kill_records,

            -- Calculate the average kills per round.
            -- Include a record only when kills are non-negative
            -- and the number of rounds played is greater than zero.
            -- A NULL result from CASE is ignored by AVG.
            ROUND(
                AVG(
                    CASE
                        WHEN kills_int >= 0
                         AND rounds_played_int > 0
                        THEN 1.0 * kills_int / rounds_played_int
                    END
                ),
                3
            ) AS avg_kills_per_round,

            -- Calculate average assists per round.
            -- Invalid assists and records without positive round counts
            -- are excluded from this metric's average.
            ROUND(
                AVG(
                    CASE
                        WHEN assists_int >= 0
                         AND rounds_played_int > 0
                        THEN 1.0 * assists_int / rounds_played_int
                    END
                ),
                3
            ) AS avg_assists_per_round,

            -- Calculate average deaths per round.
            -- Deaths are checked independently of kills and assists,
            -- so one invalid metric does not automatically invalidate another.
            ROUND(
                AVG(
                    CASE
                        WHEN deaths_int >= 0
                         AND rounds_played_int > 0
                        THEN 1.0 * deaths_int / rounds_played_int
                    END
                ),
                3
            ) AS avg_deaths_per_round,

            -- Calculate average damage per round.
            -- Only non-negative damage values with positive rounds played
            -- contribute to this average.
            -- Round the final result to two decimal places.
            ROUND(
                AVG(
                    CASE
                        WHEN damage_int >= 0
                         AND rounds_played_int > 0
                        THEN 1.0 * damage_int / rounds_played_int
                    END
                ),
                2
            ) AS avg_damage_per_round,

            -- Calculate an adjusted kills-per-death metric.
            -- Add 1 to deaths to avoid division by zero when deaths are zero.
            -- Include only rows where both kills and deaths are non-negative.
            -- This calculation does not require rounds_played_int.
            ROUND(
                AVG(
                    CASE
                        WHEN kills_int >= 0
                         AND deaths_int >= 0
                        THEN 1.0 * kills_int / (deaths_int + 1)
                    END
                ),
                3
            ) AS avg_kills_per_death_adjusted

        -- Use the eligible records created by the first CTE.
        FROM eligible_stats

        -- Create separate results for every game-role combination.
        GROUP BY game, role

        -- Sort by game, then put roles with higher average damage
        -- per round first within each game.
        ORDER BY game, avg_damage_per_round DESC
    """

    # Execute the query and return the role-performance DataFrame.
    return spark.sql(query)



# fxn to check consistency b/w sql and dataframes
def validate_match_outcomes_sql_vs_dataframe(cleaned, sql_result):

    # Get the cleaned matches DataFrame from the dictionary.
    # This DataFrame is used to independently recalculate the match counts
    # without relying on the SQL query's calculations.
    matches = cleaned["matches"]

    # Group the match records by game and calculate the same four counts
    # produced by the SQL match-outcomes analysis.
    dataframe_result = (
        matches
        .groupBy("game")
        .agg(

            # Count all match rows for each game.
            F.count("*").alias("total_matches"),

            # Count completed matches:
            # when the status matches, contribute 1; otherwise contribute 0.
            F.sum(
                F.when(F.col("status") == "completed", 1)
                .otherwise(0)
            ).alias("completed_matches"),

            # Count cancelled matches using the same conditional sum.
            F.sum(
                F.when(F.col("status") == "cancelled", 1)
                .otherwise(0)
            ).alias("cancelled_matches"),

            # Count forfeited matches using the same conditional sum.
            F.sum(
                F.when(F.col("status") == "forfeit", 1)
                .otherwise(0)
            ).alias("forfeited_matches"),
        )
    )

    # Select the columns that should be compared.
    # Percentage columns are intentionally not included in this validation.
    comparison_columns = [
        "game",
        "total_matches",
        "completed_matches",
        "cancelled_matches",
        "forfeited_matches",
    ]

    # Keep only the comparison columns from the SQL result.
    sql_counts = sql_result.select(*comparison_columns)

    # Keep the same comparison columns from the independent DataFrame result.
    dataframe_counts = dataframe_result.select(*comparison_columns)

    # exceptAll returns rows present in the first DataFrame
    # that do not have matching occurrences in the second DataFrame.
    # Unlike except(), exceptAll preserves duplicate-row counts.
    # If both calculations agree completely, neither direction has differences.

    # Find rows that appear in the SQL result but not in the DataFrame result.
    sql_only = sql_counts.exceptAll(dataframe_counts)

    # Find rows that appear in the DataFrame result but not in the SQL result.
    dataframe_only = dataframe_counts.exceptAll(sql_counts)

    # Count the differences in both directions.
    # If both counts are zero, the two sets of results agree.
    # The second count is evaluated only if the first count is zero,
    # because Python's 'and' operator short-circuits.
    passed = sql_only.count() == 0 and dataframe_only.count() == 0

    # Display a success message when no differences are found.
    if passed:
        print("PASS: SQL and DataFrame match outcome counts agree.")

    # Otherwise, display the records that differ so the issue can be investigated.
    else:
        print("FAIL: SQL and DataFrame match outcome counts differ.")

        # Show records present only in the SQL results.
        print("Rows found only in SQL:")
        sql_only.show(truncate=False)

        # Show records present only in the DataFrame results.
        print("Rows found only in DataFrame results:")
        dataframe_only.show(truncate=False)

    # Return True if the results agree, or False if differences exist.
    # The caller can use this Boolean value to decide whether to continue.
    return passed


# fxn to run all sql analysis
def run_sql_analytics(spark, cleaned):

    # Register the cleaned DataFrames as SQL views before executing queries.
    # This ensures that the SQL queries can find their required table names.
    register_sql_views(cleaned)

    # Run each SQL analysis and store its result in a dictionary.
    # Each dictionary key is a descriptive name for the analysis.
    # Each dictionary value is the DataFrame returned by its SQL query.
    results = {
        "match_outcomes_by_game":
            analyze_match_outcomes_by_game(spark),

        "event_intensity_by_game":
            analyze_event_intensity_by_game(spark),

        "role_performance_by_game":
            analyze_role_performance_by_game(spark),
    }

    # Return all result DataFrames together.
    # The caller can access each result by its dictionary key.
    return results

# fxn to display the results
def display_sql_analytics(spark, cleaned):

    # Run all SQL analyses and collect their result DataFrames in a dictionary.
    results = run_sql_analytics(spark, cleaned)

    # Loop through every analysis result.
    # analysis_name contains the dictionary key.
    # result_df contains the corresponding result DataFrame.
    for analysis_name, result_df in results.items():

        # Print a separator to make each result easier to identify.
        print("\n" + "=" * 70)

        # Convert the analysis name to uppercase and replace underscores
        # with spaces to produce a readable heading.
        print(analysis_name.upper().replace("_", " "))

        # Print another separator below the heading.
        print("=" * 70)

        # Display up to 100 rows without truncating long column values.
        # This displays the result but does not change the DataFrame itself.
        result_df.show(100, truncate=False)

    # Validate the SQL match-outcomes result against the independent
    # DataFrame calculation.
    # Use the SQL result stored under the match-outcomes dictionary key.
    parity_passed = validate_match_outcomes_sql_vs_dataframe(
        cleaned,
        results["match_outcomes_by_game"],
    )

    # If the validation fails, raise an error rather than silently
    # allowing the workflow to continue as though the results agreed.
    if not parity_passed:
        raise AssertionError(
            "SQL and DataFrame match outcome counts do not agree."
        )

    # Return the analysis results so that the caller can reuse them,
    # for example, to write the DataFrames to Parquet or perform more checks.
    return results