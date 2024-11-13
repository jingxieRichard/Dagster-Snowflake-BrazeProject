from setuptools import find_packages, setup

setup(
    name="dagster_data_pipeline_dev_to_prod",
    packages=find_packages(exclude=["dagster_data_pipeline_dev_to_prod_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "requests",
        "dagster-snowflake",
        "snowflake-connector-python[pandas]",
        "dagster-snowflake-pandas",
        "matplotlib",
        "python-dotenv",
        "snowflake-snowpark-python",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"], "tests": ["mypy", "pylint", "pytest"]},
)
