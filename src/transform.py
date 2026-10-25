from pyspark.sql import functions as F


# fxn for trimming spaces and converting blank strings to null
def trim_text_columns(df):
    for column_name in df.columns:
        df = df.withColumn(
            column_name,
            F.when(
                F.trim(F.col(column_name)) == "",
                F.lit(None).cast("string")
            ).otherwise(F.trim(F.col(column_name)))
        )

    return df


# fxn for standardizing category values to lowercase
def standardize_category(column):
    return F.lower(F.trim(F.col(column)))


# fxn for transforming teams data
def transform_teams(df):
    df = trim_text_columns(df)
    return df


# fxn transforming players data
def transform_players(df):

    # first trimming the text cols of players dataframe
    df = trim_text_columns(df)


    # standardizing role and status cols
    df = df.withColumn(
        "role",
        standardize_category("role")
    ).withColumn(
        "status",
        standardize_category("status")
    ).withColumn(
        # creating a new col which stores 
        # parsed join dates

        "parsed_join_date",
        F.to_date(
            F.try_to_timestamp(
                F.col("join_date"),
                F.lit("yyyy-MM-dd")
            )
        )
    )

    return df


# transforming matches data
def transform_matches(df):

    # trimming the cols
    df = trim_text_columns(df)

    # renaming the col
    df = df.withColumnRenamed("match_status", "status")

    # standardizing status and games col
    df = df.withColumn(
        "status",
        standardize_category("status")
    ).withColumn(
        "game",
        standardize_category("game")
    ).withColumn(

        # parsing match_time col and creating a new col 
        # to store those results

        "parsed_match_time",
        F.try_to_timestamp(
            F.col("match_time"),
            F.lit("yyyy-MM-dd HH:mm:ss")
        )
    ).withColumn(

        # casting team_a_score and team_b_score into
        # integer,If casting fails storing a NULL

        "team_a_score_int",
        F.expr("try_cast(`team_a_score` AS INT)")
    ).withColumn(
        "team_b_score_int",
        F.expr("try_cast(`team_b_score` AS INT)")
    )

    return df


# converting player match statistics into integer columns
def transform_player_match_stats(df):
    # trimming the cols
    df = trim_text_columns(df)

    # dictionary of numeric cols in the player_match_stats dataframe
    numeric_columns = {
        "kills": "kills_int",
        "deaths": "deaths_int",
        "assists": "assists_int",
        "damage": "damage_int",
        "rounds_played": "rounds_played_int"
    }

    # creating new col by casting it into integer
    # storing a NUll where casting fails

    for source_column, new_column in numeric_columns.items():
        df = df.withColumn(
            new_column,
            F.expr(f"try_cast(`{source_column}` AS INT)")
        )

    return df


# transforming match events data
def transform_match_events(df):
    # trimming the cols
    df = trim_text_columns(df)

    # standardizing event_type col
    df = df.withColumn(
        "event_type",
        standardize_category("event_type")
    ).withColumn(
        # creating a new col by parsing event_time into timestamp
        "parsed_event_time",
        F.try_to_timestamp(
            F.col("event_time"),
            F.lit("yyyy-MM-dd HH:mm:ss")
        )
    ).withColumn(
        # creating a new col by converting duration_seconds into integer

        "duration_seconds_int",
        F.expr("try_cast(`duration_seconds` AS INT)")
    )

    return df


# fxn for applying transformations to all datasets
def transform_all_datasets(datasets):
    transformed_datasets = {
        "teams": transform_teams(datasets["teams"]),
        "players": transform_players(datasets["players"]),
        "matches": transform_matches(datasets["matches"]),
        "player_match_stats": transform_player_match_stats(
            datasets["player_match_stats"]
        ),
        "match_events": transform_match_events(datasets["match_events"])
    }

    #returning a dictionary of all transformed datasets
    return transformed_datasets

# Also in all these dataframes, I made sure not to drop
# The original cols after creating the new cols for them
# To preserve the original data
