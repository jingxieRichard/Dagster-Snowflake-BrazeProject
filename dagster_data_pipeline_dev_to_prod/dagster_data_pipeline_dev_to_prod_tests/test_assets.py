
import pytest 
from unittest.mock import MagicMock, patch
from dagster import build_op_context 
from dagster_data_pipeline_dev_to_prod.assets.braze_data.processor import aggregate_braze_user_events
from dagster_data_pipeline_dev_to_prod.assets.braze_data.braze_user_data import BrazeDataProcessor

# Define some constants for tests
EXPECTED_DB_NAME = "mock_database"

# Mock the Snowflake Snowpark Session and BrazeDataProcessor
@pytest.fixture
def mock_sf_snowpark_session():
    # Create a mock session 
    mock_session = MagicMock()

    # Mock the "sql" method of the Snowpark Session to return a specific value when it is called. 
    mock_session.sql.return_value.collect.return_value = [[EXPECTED_DB_NAME]]

    return mock_session


@pytest.fixture
def mock_braze_data_processor():
    # Use patch to mock the BrazeDataProcessor Class: the patch function temporarily replaces BrazeDataProcessor with a mock object. 
    with patch("dagster_data_pipeline_dev_to_prod.assets.braze_data.braze_user_data.BrazeDataProcessor") as MockBrazeDataProcessor:
        mock_processor = MagicMock()
        MockBrazeDataProcessor.return_value = mock_processor
        yield mock_processor 

# Test recevies two fixtures as arguments
def test_aggregate_braze_user_events(mock_sf_snowpark_session, mock_braze_data_processor):
    # Create a fake context object with the mocked session: simulate the environment that the asset would run in during execution 
    context = build_op_context(resources={"snowflake_snowpark_session": mock_sf_snowpark_session})

    # Call the asset: passing the fake context to it. 
    aggregate_braze_user_events(context)

    # Asset the session.sql method was called to get the current database 
    mock_sf_snowpark_session.sql.asset_called_once_with("SELECT CURRENT_DATABASE()")

    # Asset the BrazeDataProcessor's 'run' method was called once
    mock_braze_data_processor.run.asset_called_once()

    # Optionally, check if the correct database name was passed to the BrazeDataProcessor
    db_name = mock_sf_snowpark_session.sql.return_value.collect.return_value[0][0]
    print("db_name is: ", db_name)
    assert db_name == EXPECTED_DB_NAME, f"Expected db_bame to be '{EXPECTED_DB_NAME}', but got '{db_name}' "

    mock_braze_data_processor.run.asset_called_with()     # You can add more checks if necessary
