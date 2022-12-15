import os

from dagster_snowflake import snowflake_resource
from dagster_snowflake_pandas import snowflake_pandas_io_manager

from dagster import Definitions

from .assets import comments, items, stories
from .clone_and_drop_db import clone_prod

snowflake_config = {
    "account": {"env": "SNOWFLAKE_ACCOUNT"},
    "user": {"env": "SNOWFLAKE_USER"},
    "password": {"env": "SNOWFLAKE_PASSWORD"},
    "schema": "HACKER_NEWS",
}

# start_resources
resources = {
    "branch": {
        "snowflake_io_manager": snowflake_pandas_io_manager.configured(
            {
                **snowflake_config,
                "database": f"PRODUCTION_CLONE_{os.getenv('DAGSTER_CLOUD_PULL_REQUEST_ID')}",
            }
        ),
        "snowflake": snowflake_resource.configured(
            {
                **snowflake_config,
                "database": f"PRODUCTION_CLONE_{os.getenv('DAGSTER_CLOUD_PULL_REQUEST_ID')}",
            }
        ),
    },
    "production": {
        "snowflake_io_manager": snowflake_pandas_io_manager.configured(
            {
                **snowflake_config,
                "database": "PRODUCTION",
            }
        ),
        "snowflake": snowflake_resource.configured({**snowflake_config, "database": "PRODUCTION"}),
    },
}
# end_resources


def is_branch_depl():
    return bool(os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT"))


def get_current_env():
    return "branch" if is_branch_depl() else "prod"


# start_repository
defs = Definitions(
    assets=[items, comments, stories],
    resources=resources[get_current_env()],
    jobs=[clone_prod] if is_branch_depl() else [],
)

# end_repository
