from pyspark.sql import functions as F


# COMMON HELPER FXNS


# fxn for adding a reason to records that are sent to quarantine
def add_quarantine_reason(df, reason):

    # adding a new col to identify why a record was rejected
    # F.lit(reason) assigns the same reason to every row in this dataframe
    # the original cols are preserved, and quarantine_reason is added
    # this helps us investigate rejected records instead of losing them
    return df.withColumn(
        "quarantine_reason",
        F.lit(reason)
    )


# fxn for separating exact duplicate rows from unique rows
def split_exact_duplicates(df, reason="EXACT_DUPLICATE_ROW"):

    # keeping one copy of each completely identical row
    # dropDuplicates() compares all cols because no specific cols are provided
    # the resulting dataframe contains only distinct rows
    cleaned_df = df.dropDuplicates()

    # finding the extra copies that were present in the original dataframe
    # exceptAll() performs a multiset difference and preserves duplicate counts
    # unlike subtract(), it allows us to identify the excess duplicate copies
    # for example, if the original has 3 identical rows and cleaned has 1,
    # this operation returns the 2 extra copies
    duplicate_rows = df.exceptAll(cleaned_df)

    # adding the reason so that we know why these extra copies were rejected
    duplicate_rows = add_quarantine_reason(
        duplicate_rows,
        reason
    )

    # returning the unique rows and the extra duplicate copies separately
    return cleaned_df, duplicate_rows


# fxn for combining multiple quarantine dataframes into one
def union_quarantines(dataframes, schema_df):

    # removing None values because they cannot be combined using unionByName()
    # this also allows us to pass optional quarantine branches
    valid_dataframes = [
        df for df in dataframes
        if df is not None
    ]

    # checking whether there are any quarantine dataframes to combine
    if not valid_dataframes:

        # if there are no quarantine records, return an empty dataframe
        # limit(0) keeps the schema but returns zero rows
        # adding quarantine_reason keeps the output structure consistent
        return add_quarantine_reason(
            schema_df.limit(0),
            "NO_ISSUE"
        )

    # using the first available dataframe as the starting point
    result = valid_dataframes[0]

    # combining the remaining quarantine dataframes one by one
    for df in valid_dataframes[1:]:

        # unionByName() matches columns using their names, not positions
        # allowMissingColumns=True allows dataframes to have different cols
        # if a column is missing in one dataframe, Spark fills it with NULL
        # this is useful because different validation branches may produce
        # different columns or different combinations of columns
        result = result.unionByName(
            df,
            allowMissingColumns=True
        )

    # returning all quarantine records together
    return result


# fxn for identifying records with missing identity fields
def missing_identity_condition(df, columns):

    # starting without any condition
    condition = None

    # checking each required identity column
    for column_name in columns:

        # a value is considered missing if it is NULL
        # or if it contains only spaces or an empty string
        # trim() removes spaces from the beginning and end before checking
        current_condition = (
            F.col(column_name).isNull()
            | (F.trim(F.col(column_name)) == "")
        )

        # for the first column, use its condition as the starting condition
        if condition is None:
            condition = current_condition

        else:

            # OR combines the conditions for all required identity columns
            # if even one required field is missing, the whole record
            # is classified as having incomplete identity
            condition = condition | current_condition

    # returning the Spark condition for use inside filter()
    return condition


# fxn for separating records with missing identity fields
def split_missing_identity(df, identity_columns, reason):

    # creating a condition to identify records with missing required fields
    condition = missing_identity_condition(
        df,
        identity_columns
    )

    # selecting records that match the condition
    # these records are rejected because at least one identity field is missing
    # adding a reason helps us understand why each record was rejected
    quarantined = add_quarantine_reason(
        df.filter(condition),
        reason
    )

    # keeping records that do not match the missing-identity condition
    # the ~ operator reverses the condition
    # only records with all required identity fields present remain
    cleaned = df.filter(~condition)

    # returning valid and rejected records separately
    return cleaned, quarantined


# fxn for validating references against a dataframe of known IDs
def split_unknown_references(
    df,
    reference_column,
    known_ids,
    known_id_column,
    reason
):

    # selecting only the ID column from the dataframe containing known IDs
    # alias() gives this lookup column a temporary name so it does not
    # conflict with the original reference column during the join
    reference_lookup = (
        known_ids
        .select(
            F.col(known_id_column).alias("_known_reference_id")
        )

        # NULL IDs cannot be used to validate a reference
        .filter(F.col("_known_reference_id").isNotNull())

        # keeping unique IDs in the lookup
        # this prevents repeated IDs in known_ids from multiplying rows
        # when the join is performed
        .distinct()
    )

    # performing a left join between the original records and known IDs
    # a left join preserves every input record, including unmatched records
    # if a reference has a match, _known_reference_id contains that ID
    # if there is no match, _known_reference_id becomes NULL
    joined = df.join(
        reference_lookup,
        df[reference_column] == F.col("_known_reference_id"),
        "left"
    )

    # identifying records whose reference is missing or has no matching ID
    # the first condition catches NULL references in the original dataframe
    # the second condition catches references that do not exist in known_ids
    unknown_rows = joined.filter(
        F.col(reference_column).isNull()
        | F.col("_known_reference_id").isNull()
    ).drop("_known_reference_id")

    # identifying records whose reference exists in the known-ID lookup
    # both the original reference and lookup match must be non-NULL
    valid_rows = joined.filter(
        F.col(reference_column).isNotNull()
        & F.col("_known_reference_id").isNotNull()
    ).drop("_known_reference_id")

    # returning valid records and quarantining unknown references
    return (
        valid_rows,
        add_quarantine_reason(unknown_rows, reason)
    )





# fxn for cleaning the teams dataframe
def clean_teams(teams):

    # Step 1: checking whether team_id or team_name is missing
    # both fields are required to identify a team reliably
    # records missing either field are moved to quarantine
    working_df, missing_identity = split_missing_identity(
        teams,
        ["team_id", "team_name"],
        "MISSING_TEAM_IDENTITY"
    )

    # Step 2: checking the remaining records for exact duplicate rows
    # one copy of each unique row is retained
    # excess copies are separated and marked as duplicate records
    cleaned, exact_duplicates = split_exact_duplicates(
        working_df,
        "EXACT_DUPLICATE_TEAM_ROW"
    )

    # Step 3: combining records rejected by the identity and duplicate checks
    # union_quarantines() combines both branches into one dataframe
    quarantined = union_quarantines(
        [missing_identity, exact_duplicates],
        teams
    )

    # returning the cleaned teams and quarantined teams separately
    return {
        "cleaned": cleaned,
        "quarantined": quarantined
    }




# fxn for cleaning the players dataframe and validating team references
def clean_players(players, teams):

    # Step 1: checking whether player_id or player_name is missing
    # these fields are required to identify a player
    # records missing either field are separated for investigation
    working_df, missing_identity = split_missing_identity(
        players,
        ["player_id", "player_name"],
        "MISSING_PLAYER_IDENTITY"
    )

    # Step 2: separating exact duplicate player rows
    # only one copy of an identical row is retained
    working_df, exact_duplicates = split_exact_duplicates(
        working_df,
        "EXACT_DUPLICATE_PLAYER_ROW"
    )

    # Step 3: validating each player's team_id
    # the teams dataframe contains only teams that passed team cleaning
    # therefore, a player is accepted only if their team_id exists there
    # players referencing missing or rejected teams are quarantined
    working_df, unknown_teams = split_unknown_references(
        working_df,
        "team_id",
        teams,
        "team_id",
        "UNKNOWN_TEAM_ID"
    )

    # Step 4: combining all rejected player records
    # each branch contains records rejected by a different validation rule
    quarantined = union_quarantines(
        [
            missing_identity,
            exact_duplicates,
            unknown_teams
        ],
        players
    )

    # returning players who passed all checks and the rejected records
    return {
        "cleaned": working_df,
        "quarantined": quarantined
    }





# fxn for cleaning matches and validating the participating teams
def clean_matches(matches, teams):

    # Step 1: checking whether match_id, team_a_id, or team_b_id is missing
    # these fields are required to identify the match and its participants
    # a match without one of these values cannot be validated properly
    working_df, missing_identity = split_missing_identity(
        matches,
        ["match_id", "team_a_id", "team_b_id"],
        "MISSING_MATCH_IDENTITY"
    )

    # Step 2: separating exact duplicate match rows
    # excess copies are quarantined, and one copy is retained
    working_df, exact_duplicates = split_exact_duplicates(
        working_df,
        "EXACT_DUPLICATE_MATCH_ROW"
    )

    # Step 3: checking whether team_a_id exists in the cleaned teams data
    # only teams that survived cleaning are accepted as valid references
    # records with an unknown team A are separated into quarantine
    working_df, unknown_team_a = split_unknown_references(
        working_df,
        "team_a_id",
        teams,
        "team_id",
        "UNKNOWN_TEAM_A_ID"
    )

    # Step 4: checking team_b_id independently
    # validating team A does not guarantee that team B is valid
    # both participating teams must exist in the cleaned teams dataframe
    working_df, unknown_team_b = split_unknown_references(
        working_df,
        "team_b_id",
        teams,
        "team_id",
        "UNKNOWN_TEAM_B_ID"
    )

    # Step 5: checking whether the same team is listed on both sides
    # a valid match is expected to have two different participating teams
    # this condition identifies records where team_a_id equals team_b_id
    same_team_condition = (
        F.col("team_a_id") == F.col("team_b_id")
    )

    # selecting matches where both team IDs are identical
    # adding a reason explains why these matches were rejected
    same_team_matches = add_quarantine_reason(
        working_df.filter(same_team_condition),
        "SAME_TEAM_ON_BOTH_SIDES"
    )

    # removing same-team matches from the cleaned output
    # ~ reverses the condition and keeps only rows where the IDs differ
    working_df = working_df.filter(
        ~same_team_condition
    )

    # Step 6: combining all rejected match records
    # each quarantine branch represents a separate validation failure
    quarantined = union_quarantines(
        [
            missing_identity,
            exact_duplicates,
            unknown_team_a,
            unknown_team_b,
            same_team_matches
        ],
        matches
    )

    # returning valid matches and quarantined matches separately
    return {
        "cleaned": working_df,
        "quarantined": quarantined
    }





# fxn for cleaning player-match statistics and validating relationships
def clean_player_match_stats(
    stats,
    raw_players,
    cleaned_players,
    raw_matches,
    cleaned_matches
):

    # Step 1: checking required identity fields
    # stat_id identifies the statistics record
    # match_id identifies the match in which the statistics were recorded
    # player_id identifies the player associated with those statistics
    # records missing any of these fields are quarantined
    working_df, missing_identity = split_missing_identity(
        stats,
        ["stat_id", "match_id", "player_id"],
        "MISSING_STAT_IDENTITY"
    )

    # Step 2: separating exact duplicate rows
    # this check happens before checking duplicate player-match combinations
    # otherwise, identical copies could be mistaken for separate statistics
    # records for the same player in the same match
    working_df, exact_duplicates = split_exact_duplicates(
        working_df,
        "EXACT_DUPLICATE_STAT_ROW"
    )

    # Step 3: identifying repeated (match_id, player_id) combinations
    # the expected grain is one row per player per match
    # groupBy() groups records sharing the same match and player IDs
    # count() calculates how many records exist in each group
    # count > 1 identifies combinations that occur more than once
    duplicate_combinations = (
        working_df
        .groupBy("match_id", "player_id")
        .count()
        .filter(F.col("count") > 1)
        .select("match_id", "player_id")
    )

    # selecting every statistics row belonging to a repeated combination
    # an inner join keeps rows whose match_id and player_id appear
    # in duplicate_combinations
    # the entire repeated group is quarantined instead of choosing one row
    # arbitrarily, because we cannot tell which record should be trusted
    duplicate_group_rows = working_df.join(
        duplicate_combinations,
        on=["match_id", "player_id"],
        how="inner"
    )

    # adding a reason to all records in repeated player-match groups
    duplicate_group_rows = add_quarantine_reason(
        duplicate_group_rows,
        "DUPLICATE_MATCH_PLAYER_COMBINATION"
    )

    # keeping only combinations that appear once
    # left_anti returns rows from the left dataframe that have no matching
    # keys in the right dataframe
    # this removes all records belonging to repeated player-match groups
    working_df = working_df.join(
        duplicate_combinations,
        on=["match_id", "player_id"],
        how="left_anti"
    )

   
    # Step 4: CLASSIFY PLAYER REFERENCES

    # raw_players contains players before cleaning
    # cleaned_players contains only players that survived cleaning
    #
    # comparing these two datasets helps distinguish:
    # 1. PLAYER_REFERENCE_QUARANTINED:
    #    the player existed in raw data but did not survive cleaning
    # 2. UNKNOWN_PLAYER_ID:
    #    the player ID did not exist in raw data at all
    # 3. valid reference:
    #    the player exists in the cleaned players dataframe

    # creating a lookup of unique player IDs from the original source
    # NULL IDs are excluded because they cannot identify a player
    raw_player_ids = (
        raw_players
        .select("player_id")
        .filter(F.col("player_id").isNotNull())
        .distinct()
    )

    # creating a lookup of unique player IDs that survived cleaning
    # this lets us check whether a source player passed the cleaning rules
    cleaned_player_ids = (
        cleaned_players
        .select("player_id")
        .filter(F.col("player_id").isNotNull())
        .distinct()
    )

    # joining the raw player lookup to the statistics dataframe
    # the marker is True when player_id exists in the original source
    # if no player is found, the marker will be NULL after the left join
    working_df = working_df.join(
        raw_player_ids.withColumn(
            "_player_exists_in_source",
            F.lit(True)
        ),
        on="player_id",
        how="left"
    )

    # joining the cleaned player lookup
    # the marker is True when the player survived cleaning
    # if no cleaned player matches, the marker will be NULL
    working_df = working_df.join(
        cleaned_player_ids.withColumn(
            "_player_is_clean",
            F.lit(True)
        ),
        on="player_id",
        how="left"
    )

    # identifying player IDs that existed in raw data but did not survive
    # cleaning
    # the first condition confirms the player existed in the source
    # the second confirms there is no corresponding cleaned player
    player_quarantined_condition = (
        F.col("_player_exists_in_source").isNotNull()
        & F.col("_player_is_clean").isNull()
    )

    # separating statistics that reference rejected players
    # temporary marker columns are dropped because they are only needed
    # for validation and are not part of the original statistics data
    player_quarantined_rows = add_quarantine_reason(
        working_df.filter(player_quarantined_condition)
        .drop("_player_exists_in_source", "_player_is_clean"),
        "PLAYER_REFERENCE_QUARANTINED"
    )

    # removing statistics that have already been classified as referencing
    # players rejected during cleaning
    working_df = working_df.filter(
        ~player_quarantined_condition
    )

    # identifying player IDs that were not found in the original source
    # a NULL source marker means no matching player_id was found in raw data
    unknown_player_condition = (
        F.col("_player_exists_in_source").isNull()
    )

    # separating statistics that reference completely unknown players
    # the temporary marker columns are removed from these rejected records
    unknown_player_rows = add_quarantine_reason(
        working_df.filter(unknown_player_condition)
        .drop("_player_exists_in_source", "_player_is_clean"),
        "UNKNOWN_PLAYER_ID"
    )

    # retaining only statistics that reference players present in raw data
    # and that survived cleaning
    # dropping the temporary columns keeps the cleaned output tidy
    working_df = working_df.filter(
        ~unknown_player_condition
    ).drop("_player_exists_in_source", "_player_is_clean")


    # Step 5: CLASSIFY MATCH REFERENCES

    # applying the same source-versus-cleaned validation to match_id
    #
    # this helps distinguish:
    # 1. MATCH_REFERENCE_QUARANTINED:
    #    the match existed in raw data but did not survive cleaning
    # 2. UNKNOWN_MATCH_ID:
    #    the match ID never existed in raw data
    # 3. valid reference:
    #    the match exists in the cleaned matches dataframe

    # creating a lookup of match IDs from the original matches dataframe
    # NULL match IDs cannot be used to identify a match
    raw_match_ids = (
        raw_matches
        .select("match_id")
        .filter(F.col("match_id").isNotNull())
        .distinct()
    )

    # creating a lookup of match IDs that survived cleaning
    cleaned_match_ids = (
        cleaned_matches
        .select("match_id")
        .filter(F.col("match_id").isNotNull())
        .distinct()
    )

    # adding a marker to show whether the match ID existed in raw data
    # the left join preserves every statistics row even when there is no match
    working_df = working_df.join(
        raw_match_ids.withColumn(
            "_match_exists_in_source",
            F.lit(True)
        ),
        on="match_id",
        how="left"
    )

    # adding a marker to show whether the match survived cleaning
    working_df = working_df.join(
        cleaned_match_ids.withColumn(
            "_match_is_clean",
            F.lit(True)
        ),
        on="match_id",
        how="left"
    )

    # identifying matches that existed in raw data but were rejected
    # a non-NULL source marker confirms the match existed originally
    # a NULL cleaned marker means the match did not survive cleaning
    match_quarantined_condition = (
        F.col("_match_exists_in_source").isNotNull()
        & F.col("_match_is_clean").isNull()
    )

    # separating statistics that reference rejected matches
    # removing temporary markers because they are only used for validation
    match_quarantined_rows = add_quarantine_reason(
        working_df.filter(match_quarantined_condition)
        .drop("_match_exists_in_source", "_match_is_clean"),
        "MATCH_REFERENCE_QUARANTINED"
    )

    # removing rows already classified as referencing rejected matches
    working_df = working_df.filter(
        ~match_quarantined_condition
    )

    # identifying match IDs that were not found in the original source
    # a NULL source marker means that match_id was not present in raw_matches
    unknown_match_condition = (
        F.col("_match_exists_in_source").isNull()
    )

    # separating statistics that reference completely unknown match IDs
    unknown_match_rows = add_quarantine_reason(
        working_df.filter(unknown_match_condition)
        .drop("_match_exists_in_source", "_match_is_clean"),
        "UNKNOWN_MATCH_ID"
    )

    # keeping statistics that reference matches that survived cleaning
    # removing temporary marker columns before returning the dataframe
    working_df = working_df.filter(
        ~unknown_match_condition
    ).drop("_match_exists_in_source", "_match_is_clean")

    # Step 6: combining every quarantine branch into one dataframe
    # each branch represents a separate reason for rejecting statistics
    quarantined = union_quarantines(
        [
            missing_identity,
            exact_duplicates,
            duplicate_group_rows,
            player_quarantined_rows,
            unknown_player_rows,
            match_quarantined_rows,
            unknown_match_rows
        ],
        stats
    )

    # returning statistics that passed all checks and all rejected records
    return {
        "cleaned": working_df,
        "quarantined": quarantined
    }





# fxn for cleaning match events and validating their match references
def clean_match_events(events, raw_matches, cleaned_matches):

    # Step 1: checking required identity fields
    # event_id identifies the event
    # match_id identifies the match to which the event belongs
    # event_type describes the type of event
    # records missing any of these fields are quarantined
    working_df, missing_identity = split_missing_identity(
        events,
        ["event_id", "match_id", "event_type"],
        "MISSING_EVENT_IDENTITY"
    )

    # Step 2: separating exact duplicate event rows
    # only one copy of each identical event row is retained
    # excess copies are sent to quarantine
    working_df, exact_duplicates = split_exact_duplicates(
        working_df,
        "EXACT_DUPLICATE_EVENT_ROW"
    )

    # Step 3: creating a lookup of match IDs from the original source
    # we need the original dataset to identify whether a match ever existed
    # unique non-NULL IDs are sufficient for this existence check
    raw_match_ids = (
        raw_matches
        .select("match_id")
        .filter(F.col("match_id").isNotNull())
        .distinct()
    )

    # Step 4: creating a lookup of match IDs that survived cleaning
    # a match appearing here is considered a valid match reference
    cleaned_match_ids = (
        cleaned_matches
        .select("match_id")
        .filter(F.col("match_id").isNotNull())
        .distinct()
    )

    # Step 5: adding a marker that shows whether the match ID existed
    # in the original matches dataframe
    # a left join preserves all events, including those without a match
    working_df = working_df.join(
        raw_match_ids.withColumn(
            "_match_exists_in_source",
            F.lit(True)
        ),
        on="match_id",
        how="left"
    )

    # adding a second marker to show whether the match survived cleaning
    working_df = working_df.join(
        cleaned_match_ids.withColumn(
            "_match_is_clean",
            F.lit(True)
        ),
        on="match_id",
        how="left"
    )

    # identifying events whose match existed in raw data but was rejected
    # the source marker is present, but the cleaned marker is NULL
    quarantined_match_condition = (
        F.col("_match_exists_in_source").isNotNull()
        & F.col("_match_is_clean").isNull()
    )

    # separating events that reference matches rejected during cleaning
    # dropping the temporary marker columns because they are not original
    # event fields
    quarantined_match_events = add_quarantine_reason(
        working_df.filter(quarantined_match_condition)
        .drop("_match_exists_in_source", "_match_is_clean"),
        "MATCH_REFERENCE_QUARANTINED"
    )

    # removing events that have already been classified as referencing
    # rejected matches so they are not classified again below
    working_df = working_df.filter(
        ~quarantined_match_condition
    )

    # identifying events whose match ID does not exist in the raw source
    # a NULL source marker means the lookup could not find the match ID
    unknown_match_condition = (
        F.col("_match_exists_in_source").isNull()
    )

    # separating events that reference completely unknown match IDs
    unknown_match_events = add_quarantine_reason(
        working_df.filter(unknown_match_condition)
        .drop("_match_exists_in_source", "_match_is_clean"),
        "UNKNOWN_MATCH_ID"
    )

    # keeping events that reference matches that survived cleaning
    # dropping temporary marker columns before returning the results
    working_df = working_df.filter(
        ~unknown_match_condition
    ).drop("_match_exists_in_source", "_match_is_clean")

    # Step 6: combining all rejected event records
    quarantined = union_quarantines(
        [
            missing_identity,
            exact_duplicates,
            quarantined_match_events,
            unknown_match_events
        ],
        events
    )

    # returning valid events and quarantined events separately
    return {
        "cleaned": working_df,
        "quarantined": quarantined
    }



# fxn for running all cleaning functions in dependency order
def clean_all_datasets(datasets):

    # preserving the original transformed datasets before cleaning
    # these are needed later to distinguish unknown IDs from IDs belonging
    # to records that existed in the source but were rejected during cleaning
    raw_teams = datasets["teams"]
    raw_players = datasets["players"]
    raw_matches = datasets["matches"]

    # Step 1: cleaning teams first
    # players and matches both reference teams, so the teams dataset must
    # be cleaned before those relationships can be validated
    teams_result = clean_teams(raw_teams)

    # Step 2: cleaning players using only teams that passed cleaning
    # this prevents players from referencing teams that were rejected
    players_result = clean_players(
        raw_players,
        teams_result["cleaned"]
    )

    # Step 3: cleaning matches using the cleaned teams dataframe
    # both team_a_id and team_b_id must refer to accepted teams
    matches_result = clean_matches(
        raw_matches,
        teams_result["cleaned"]
    )

    # Step 4: cleaning player-match statistics
    # raw_players and raw_matches help identify whether a reference
    # originally existed
    # the cleaned versions help determine whether those records passed
    # their own cleaning rules
    stats_result = clean_player_match_stats(
        datasets["player_match_stats"],
        raw_players,
        players_result["cleaned"],
        raw_matches,
        matches_result["cleaned"]
    )

    # Step 5: cleaning match events
    # event match IDs are checked against both the original matches
    # and the matches that survived cleaning
    events_result = clean_match_events(
        datasets["match_events"],
        raw_matches,
        matches_result["cleaned"]
    )

    # returning two groups of results:
    # cleaned contains the records that passed the cleaning rules
    # quarantined contains records rejected by one of the cleaning rules
    #
    # keeping these outputs separate allows the calling pipeline to
    # inspect the results, run quality checks, or write them to storage
    return {
        "cleaned": {
            "teams": teams_result["cleaned"],
            "players": players_result["cleaned"],
            "matches": matches_result["cleaned"],
            "player_match_stats": stats_result["cleaned"],
            "match_events": events_result["cleaned"]
        },
        "quarantined": {
            "teams": teams_result["quarantined"],
            "players": players_result["quarantined"],
            "matches": matches_result["quarantined"],
            "player_match_stats": stats_result["quarantined"],
            "match_events": events_result["quarantined"]
        }
    }