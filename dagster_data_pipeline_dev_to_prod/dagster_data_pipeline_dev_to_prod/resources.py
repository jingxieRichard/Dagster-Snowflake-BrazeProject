from snowflake.snowpark.session import Session 

def build_snowflake_session(config_params: dict) -> Session:
    return Session.builder.configs(config_params).create()

