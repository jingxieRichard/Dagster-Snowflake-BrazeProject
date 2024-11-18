import pytest 
from unittest.mock import MagicMock
from datetime import datetime

from snowflake.snowpark import Session, DataFrame 
import snowflake.snowpark.types as T 

from dagster_data_pipeline_dev_to_prod.assets.braze_data.braze_user_data import BrazeDataProcessor, BrazeDataset
from .utils import create_mock_dataframe

@pytest.fixture
def mock_sf_snowpark_session():
    # Create a mock Snowpark session 
    mock_session = MagicMock(spec=Session)
    mock_session.table.return_value = MagicMock(spec=DataFrame)
    return mock_session


@pytest.fixture
def braze_data_processor(mock_sf_snowpark_session): 
    # Instantiate the BrazeDataProcessor with the mocked session 
    return BrazeDataProcessor(
        session=mock_sf_snowpark_session,
        src_database="src_db",
        src_schema="src_schema",
        dst_database="dst_db",
        dst_schema="dst_schema"
    )



# Parametrize different test data for "user_sends", "user_impressions", etc. 
@pytest.mark.parametrize("user_sends_data, user_impressions_data, user_clicks_data, user_purchases_data, expected_output", [
    (
        [{"USER_ID": "user1", "TIME": datetime(2024, 11, 18, 10, 30), "CONTENT_CARD_ID": f"card{n}", "CAMPAIGN_ID": f"campaign{n}"} for n in range(1, 11)],
        [{"USER_ID": f"user{n}", "TIME": datetime(2024, 11, 18, 10, 31 + n), "CONTENT_CARD_ID": f"card{n}", "CAMPAIGN_ID": f"campaign{n}"} for n in range(1, 11)],
        [{"USER_ID": f"user{n}", "TIME": datetime(2024, 11, 18, 10, 32 + n), "CONTENT_CARD_ID": f"card{n}", "CAMPAIGN_ID": f"campaign{n}"} for n in range(1, 11)],
        [{"USER_ID": f"user{n}", "TIME": datetime(2024, 11, 18, 12, 30 + n), "PRODUCT_ID": f"product{n}", "PROPERTIES": f'{{"price": {n * 10}, "discount": 0.{n}}}'} for n in range(1, 11)],
        [{"USER_ID": f"user{n}", "Number_sends": 1, "Number_impressions": 1, "Number_clicks": 1, "Number_purchases": 1} for n in range(1, 11)],
    ),
])
def test_transform_with_parametrized_data(braze_data_processor, 
                                          user_sends_data, 
                                          user_impressions_data, 
                                          user_clicks_data, 
                                          user_purchases_data, 
                                          expected_output):
    # Mock the `table` method to return different mock data based on parameters
    mock_schema =  T.StructType([
    T.StructField("USER_ID", T.StringType(), nullable=True),
    T.StructField("TIME", T.LongType(), nullable=True),
    T.StructField("CONTENT_CARD_ID", T.StringType(), nullable=True),
    T.StructField("CAMPAIGN_ID", T.StringType(), nullable=True),])

    COLUMNS = ["USER_ID", "TIME", "CONTENT_CARD_ID", "CAMPAIGN_ID"]


    braze_data_processor.data.user_sends = create_mock_dataframe(user_sends_data, mock_schema, COLUMNS)
    braze_data_processor.data.user_impressions = create_mock_dataframe(user_impressions_data, mock_schema, COLUMNS)
    braze_data_processor.data.user_clicks = create_mock_dataframe(user_clicks_data, mock_schema, COLUMNS)

    mock_purchase_schema =  T.StructType([
    T.StructField("USER_ID", T.StringType(), nullable=True),
    T.StructField("TIME", T.LongType(), nullable=True),
    T.StructField("PRODUCT_ID", T.StringType(), nullable=True),
    T.StructField("PROPERTIES", T.StringType(), nullable=True),])
    braze_data_processor.data.user_purchases = create_mock_dataframe(user_purchases_data, mock_purchase_schema, columns=["USER_ID", "TIME", "PRODUCT_ID", "PROPERTIES"])
   
    braze_data_processor.transform()
    result = braze_data_processor.data.agg_user_events
    
    expected_output_schema =  T.StructType([
    T.StructField("USER_ID", T.StringType(), nullable=True),
    T.StructField("NUMBER_SENDS", T.LongType(), nullable=True),
    T.StructField("NUMBER_IMPRESSIONS", T.LongType(), nullable=True),
    T.StructField("NUMBER_CLICKS", T.LongType(), nullable=True),
    T.StructField("NUMBER_CLICKS", T.LongType(), nullable=True),
    ])
    expected_output = create_mock_dataframe(expected_output, expected_output_schema, columns=["USER_ID", "NUMBER_SENDS", "NUMBER_IMPRESSIONS",
                                                          "NUMBER_CLICKS", "NUMBER_PURCHASES"])
    print("expected_output is: ", expected_output.collect())
    assert result.equals(expected_output), f"Expected result to be '{expected_output}', but got '{result}' "


