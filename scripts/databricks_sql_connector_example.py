"""Sample script for running queries using Databricks SQL API"""

import os

from databricks.sdk.core import Config, oauth_service_principal
from databricks.sql import connect


def credential_provider():
    config = Config(
        host=os.getenv("DATABRICKS_HOST"),
        client_id=os.getenv("DATABRICKS_CLIENT_ID"),
        client_secret=os.getenv("DATABRICKS_CLIENT_SECRET"),
    )
    return oauth_service_principal(config)


with connect(  # noqa: SIM117
    server_hostname=os.getenv("DATABRICKS_HOST"),
    credentials_provider=credential_provider,
    http_path=os.getenv("DATABRICKS_HTTP_PATH"),
) as connection:
    with connection.cursor() as cursor:
        cursor.execute("USE CATALOG project_dev_db")
        result = cursor.fetchall()
        print(result)
