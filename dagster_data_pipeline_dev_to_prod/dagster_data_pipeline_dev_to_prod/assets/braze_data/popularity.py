from dataclasses import dataclass

from dagster_data_pipeline_dev_to_prod.constants import *

from snowflake.snowpark import DataFrame, Session  
import snowflake.snowpark.functions as F 

@dataclass
class PopularityDataset:
    user_clicks: DataFrame 
    user_impressions: DataFrame
    campaign_popularity: DataFrame


class CampaignPopularityProcessor:
    def __init__(self,
                 session: Session,
                 src_database: str, 
                 src_schema: str, 
                 dst_database: str, 
                 dst_schema) -> None:
        self.session = session
        self.src_database = src_database
        self.src_schema = src_schema
        self.dst_database = dst_database
        self.dst_schema = dst_schema 
        self.data = PopularityDataset(
            user_clicks=None, 
            user_impressions=None,
            campaign_popularity=None,
        )
    
    def run(self):
        self.extract()
        self.transform()
        self.load()

    def extract(self): 
        """Load the Braze user events from Snowflake source database """
        self.data.user_clicks = self.session.table(f"{self.src_database}.{self.src_schema}.USER_CLICKS")
        self.data.user_impressions = self.session.table(f"{self.src_database}.{self.src_schema}.USER_IMPRESSIONS")

    def transform(self):
        # Clean the table 
        valid_user_clicks = self.data.user_clicks.where(F.col("CAMPAIGN_ID").is_not_null())
        valid_user_impressions = self.data.user_impressions.where(F.col("CAMPAIGN_ID").is_not_null())

        # Aggregate clicks and impressions by campaign_id
        agg_clicks_per_campaign = (valid_user_clicks
                                   .group_by("CAMPAIGN_ID")
                                   .agg(F.count_distinct("USER_ID").alias("num_clicks")))
        agg_impressions_per_campaign = (valid_user_impressions
                                        .group_by("CAMPAIGN_ID")
                                        .agg(F.count_distinct("USER_ID").alias("num_impressions")))
        
        agg_events_per_campaign = agg_clicks_per_campaign.join(agg_impressions_per_campaign, "CAMPAIGN_ID", "outer")
        campaign_popularity = (agg_events_per_campaign
                      .with_column("popularity", F.col("NUM_CLICKS")* F.lit(2) + F.col("NUM_IMPRESSIONS")*F.lit(1)))

        min_val = campaign_popularity.select(F.min('popularity')).collect()[0][0]
        max_val = campaign_popularity.select(F.max('popularity')).collect()[0][0]

        # Apply min-max scaling to the column
        self.data.campaign_popularity = (campaign_popularity
                             .with_column('scaled_popularity', 
                                          F.lit(1) + (F.col('popularity') - min_val) * (99 / (max_val - min_val))))
        

    def load(self) -> None: 
        (self.data.campaign_popularity
         .write
         .mode("overwrite")
         .save_as_table(f"{self.dst_database}.{self.dst_schema}.campaign_popularity"))
