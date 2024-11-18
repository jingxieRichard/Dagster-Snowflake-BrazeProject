from unittest.mock import MagicMock
import snowflake.snowpark.types as T
from snowflake.snowpark import DataFrame 

def create_mock_dataframe(mock_data: dict, data_schema: T.StructType, columns: list) -> MagicMock:
    mock_data_df = MagicMock(spec=DataFrame)

    # Setup mock properties
    mock_data_df.collect.return_value = mock_data    # Simulated data 
    mock_data_df.schema = data_schema                # Simulated schema 
    mock_data_df.columns = columns             

    return mock_data_df