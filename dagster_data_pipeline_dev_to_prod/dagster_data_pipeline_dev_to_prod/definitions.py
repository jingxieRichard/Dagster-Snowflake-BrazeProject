import os
from dotenv import load_dotenv

from .resources import build_snowflake_session

from dagster import (Definitions, 
                     EnvVar, 
                     ScheduleDefinition,
                     define_asset_job, 
                     )

from dagster_snowflake import SnowflakeResource 

from dagster_data_pipeline_dev_to_prod.assets.braze_data.processor import load_data, aggregate_braze_user_events, sf_table_statistics, compute_campaign_popularity
from .constants import *



load_dotenv()

# Get deployment name from environment variable: local or production
deployment_name = os.getenv("DAGSTER_DEPLOYMENT", "local")

SHARED_SF_CONFIG = {
    "account": EnvVar("SNOWFLAKE_ACCOUNT").get_value(),
    "user": EnvVar("SNOWFLAKE_USER").get_value(),
    "password": EnvVar("SNOWFLAKE_PASSWORD").get_value()
}
LOCAL_SF_CONFIG = {**SHARED_SF_CONFIG, 
                   **{"warehouse": SNOWFLAKE_DEV_WAREHOUSE,
                    "role": SNOWFLAKE_DEV_ROLE, 
                    "database": SNOWFLAKE_DEV_DATABASE, 
                    "schema": RAW_SCHEMA}}

PROD_SF_CONFIG = {**SHARED_SF_CONFIG, 
                   **{"warehouse": SNOWFLAKE_PROD_WAREHOUSE,
                    "role": SNOWFLAKE_PROD_ROLE, 
                    "database": SNOWFLAKE_PROD_DATABASE, 
                    "schema": RAW_SCHEMA}}

resource_defs = {
    "local": {
        "snowflake_snowpark_session": build_snowflake_session(LOCAL_SF_CONFIG),
       
        "snowflake_resource": SnowflakeResource(
            account=LOCAL_SF_CONFIG["account"],
            user=LOCAL_SF_CONFIG["user"],
            password=LOCAL_SF_CONFIG["password"],
            warehouse=LOCAL_SF_CONFIG["warehouse"],
            role=LOCAL_SF_CONFIG["role"],
            database=LOCAL_SF_CONFIG["database"],
            schema=LOCAL_SF_CONFIG["schema"],
        ),
    },
   
    "production": {
        "snowflake_snowpark_session": build_snowflake_session(PROD_SF_CONFIG),
       
        "snowflake_resource": SnowflakeResource(
           account=PROD_SF_CONFIG["account"],
            user=PROD_SF_CONFIG["user"],
            password=PROD_SF_CONFIG["password"],
            warehouse=PROD_SF_CONFIG["warehouse"],
            role=PROD_SF_CONFIG["role"],
            database=PROD_SF_CONFIG["database"],
            schema=PROD_SF_CONFIG["schema"],
        )
    },
}

# Define the Braze data job 
braze_data_job = define_asset_job(
    name="braze_data_pipeline",
    assets=[load_data, aggregate_braze_user_events, sf_table_statistics, compute_campaign_popularity]
)
# Add hourly refresh schedule 
hourly_refresh_schedule = ScheduleDefinition(
    job=braze_data_job,
    cron_schedule="*/10 * * * *"  # Cron schedule to run the job every 10 minutes
)



defs = Definitions(
    assets=[load_data, aggregate_braze_user_events, sf_table_statistics, compute_campaign_popularity],
    resources=resource_defs[deployment_name],
    schedules=[hourly_refresh_schedule]
)