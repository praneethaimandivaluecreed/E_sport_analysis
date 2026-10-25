
"""
Pytest suite for the E_sport_analysis project.

Expected structure:
    E_sport_analysis/
    ├── src/
    │   ├── ingest.py
    │   ├── transform.py
    │   ├── validate.py
    │   ├── clean.py
    │   ├── analytics.py
    │   └── sql_analytics.py
    └── tests/
        └── test_esport_project.py

Run from the project root:
    python -m pytest -v
"""

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


# --------------------------------------------------
# PROJECT PATH
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


# --------------------------------------------------
# SPARK FIXTURE
# --------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """Create one local Spark session for the test suite."""
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("E-sport-analysis-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


# --------------------------------------------------
# SAMPLE DATA FIXTURES
# --------------------------------------------------

@pytest.fixture
def sample_teams(spark):
    """Create sample team data for tests."""
    return spark.createDataFrame(
        [
            ("T01", " Alpha ", " Valorant ", " APAC ", " Coach A "),
            ("T02", "Bravo", "CS2", "EU", "Coach B"),
        ],
        [
            "team_id",
            "team_name",
            "game",
            "region",
            "coach",
        ],
    )


@pytest.fixture
def sample_matches(spark):
    """Create sample match data for tests."""
    from datetime import datetime

    return spark.createDataFrame(
        [
            (
                "M01",
                "Valorant",
                datetime(2026, 1, 10, 12, 0, 0),
                "T01",
                "T02",
                13,
                8,
                "completed",
            ),
            (
                "M02",
                "Valorant",
                datetime(2026, 1, 11, 12, 0, 0),
                "T02",
                "T01",
                None,
                None,
                "cancelled",
            ),
        ],
        [
            "match_id",
            "game",
            "parsed_match_time",
            "team_a_id",
            "team_b_id",
            "team_a_score_int",
            "team_b_score_int",
            "status",
        ],
    )


# --------------------------------------------------
# 1. INGESTION TESTS
# --------------------------------------------------

def test_ingest_schema_constants_exist():
    """Verify the explicit schemas are defined."""
    import ingest

    expected_schemas = [
        "TEAMS_SCHEMA",
        "PLAYERS_SCHEMA",
        "MATCHES_SCHEMA",
    ]

    for schema_name in expected_schemas:
        assert hasattr(ingest, schema_name), (
            f"Missing schema constant: {schema_name}"
        )


# --------------------------------------------------
# 2. TRANSFORMATION TESTS
# --------------------------------------------------

def test_transform_functions_exist():
    """Verify transformation functions exist for all datasets."""
    import transform

    expected_functions = [
        "transform_teams",
        "transform_players",
        "transform_matches",
        "transform_player_match_stats",
        "transform_match_events",
    ]

    for function_name in expected_functions:
        assert callable(getattr(transform, function_name, None)), (
            f"Missing transformation function: {function_name}"
        )


# --------------------------------------------------
# 3. CLEANING TESTS
# --------------------------------------------------

def test_clean_teams_returns_expected_structure(sample_teams):
    """Verify the cleaner returns cleaned and quarantined DataFrames."""
    import clean

    result = clean.clean_teams(sample_teams)

    assert isinstance(result, dict)
    assert "cleaned" in result
    assert "quarantined" in result

    assert result["cleaned"] is not None
    assert result["quarantined"] is not None

    # Both results should be Spark DataFrames.
    assert hasattr(result["cleaned"], "columns")
    assert hasattr(result["quarantined"], "columns")


# --------------------------------------------------
# 4. VALIDATION TESTS
# --------------------------------------------------

def test_validation_entry_point_exists():
    """Verify the validation orchestration function exists."""
    import validate

    assert callable(
        getattr(validate, "validate_all_datasets", None)
    )


# --------------------------------------------------
# 5. DATAFRAME ANALYTICS TESTS
# --------------------------------------------------

def test_analytics_functions_exist():
    """Verify the main team analytics functions exist."""
    import analytics

    assert callable(
        getattr(analytics, "build_team_match_records", None)
    )

    assert callable(
        getattr(analytics, "calculate_team_performance", None)
    )


# --------------------------------------------------
# 6. SPARK SQL ANALYTICS TESTS
# --------------------------------------------------

def test_sql_analytics_functions_exist():
    """Verify the expected Spark SQL analytics functions exist."""
    import sql_analytics

    expected_functions = [
        "register_sql_views",
        "analyze_match_outcomes_by_game",
        "analyze_event_intensity_by_game",
        "analyze_role_performance_by_game",
        "validate_match_outcomes_sql_vs_dataframe",
        "run_sql_analytics",
    ]

    for function_name in expected_functions:
        assert callable(getattr(sql_analytics, function_name, None)), (
            f"Missing SQL analytics function: {function_name}"
        )
