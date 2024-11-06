# Dagster-Snowflake-BrazeProject

- Step 1: Create a conda env
  * conda activate dagster-sf-env
  * activate dagster-sf-env
- Step 2: Install dagster and install project dependencies 
  * pip install dagster
  * dagster project scaffold --name sf-braze-project
  * cd sf-braze-project
  * pip install -e ".[dev]"
- Step 3: Start the Dagster UI (local)
  * dagster dev 
