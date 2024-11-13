from .braze_user_data import BrazeDataProcessor
from .popularity import CampaignPopularityProcessor

from dagster import MaterializeResult, asset, AssetCheckExecutionContext
from dagster_data_pipeline_dev_to_prod.constants import * 



@asset(
        required_resource_keys={"snowflake_resource"},
        key_prefix=["braze"],
)
def load_data(context) -> MaterializeResult: 
    query = """
        SELECT 
            COUNT (*) AS row_count, 
            COUNT(DISTINCT USER_ID) AS unique_user_count,
            COUNT(DISTINCT DEVICE_ID) AS unique_device_id
        FROM BRAZE_USER_EVENT_DEMO_DATASET.PUBLIC.USERS_MESSAGES_CONTENTCARD_SEND_VIEW
    """
    with context.resources.snowflake_resource.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()


    if result:
        row_count, unique_user_count, unique_device_id = result
        return MaterializeResult(
            metadata={
            "row_count": row_count,
            "unique_user_count": unique_user_count,
            "unique_device_id": unique_device_id,
            }
        )
    else:
        return MaterializeResult(
            metadata={
            "row_count": 0,
            "unique_user_count": 0,
            "unique_device_id": 0,
            }
        )

# A data pipeline to aggregate Braze demo user events 
@asset(
        required_resource_keys={"snowflake_snowpark_session"},
        key_prefix=["braze"],
)
def aggregate_braze_user_events(context) -> None:
    session = context.resources.snowflake_snowpark_session
    db_name = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
    bz_data_processor = BrazeDataProcessor(session=session, 
                                           src_database=db_name,
                                           src_schema=RAW_SCHEMA,
                                           dst_database=db_name,
                                           dst_schema=PROCESSED_SCHEMA)
    bz_data_processor.run()
    print("aggregattion of Braze user events has been done successfully!")


# Collect some statistics of the aggregated_user_events 
@asset(
        deps={"aggregate_braze_user_events"},
        required_resource_keys={"snowflake_resource"},
        key_prefix=["braze"],
)
def sf_table_statistics(context) -> MaterializeResult:
    
    # Execute the query to fetch the database name 
    with context.resources.snowflake_resource.get_connection() as conn:
        cursor = conn.cursor()
   
        query_db = "SELECT CURRENT_DATABASE()"

        cursor.execute(query_db)
        db_name = cursor.fetchone()[0]
        
        role = cursor.execute("SELECT CURRENT_ROLE()").fetchone()[0]
        print("role is : ", role)
        query = f"""
            SELECT 
                COUNT (*) AS row_count, 
                COUNT(DISTINCT USER_ID) AS unique_user_count,
                MAX(NUMBER_CLICKS) AS max_clicks,
                MIN(NUMBER_CLICKS) AS min_clicks
            FROM {db_name}.{PROCESSED_SCHEMA}.AGG_USER_EVENTS
        
        """ 
    
        cursor.execute(query)
        result = cursor.fetchone()

        row_count, unique_user_count, max_clicks, min_clicks = result 
   

    return MaterializeResult(
        metadata={
            "row_count": row_count,
            "unique_user_count": unique_user_count,
            "user_max_clicks": max_clicks,
            "user_min_clicks": min_clicks}
    )


# A data pipeline to aggregate Braze demo user events 
@asset(
        deps={"aggregate_braze_user_events"},
        required_resource_keys={"snowflake_snowpark_session"},
        key_prefix=["braze"],
)
def compute_campaign_popularity(context) -> None:
    session = context.resources.snowflake_snowpark_session
    db_name = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
    campaign_popularity_processor = CampaignPopularityProcessor(session=session, 
                                           src_database=db_name,
                                           src_schema=RAW_SCHEMA,
                                           dst_database=db_name,
                                           dst_schema=PROCESSED_SCHEMA)
    campaign_popularity_processor.run()
    print("Compute campaign popularity has been done successfully!")

