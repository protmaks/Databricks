# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Access Requests: Export Configured Approvers (Destinations) with the API
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC _author_: **Maksim Pachkouski** - [Databricks MVP | Senior Data Engineer]
# MAGIC - [file source](https://github.com/protmaks/Databricks/blob/main/API%20%26%20SDK/Databricks%20Access%C2%A0Requests/Databricks%20Access%20Requests%20-%20Export%20Configured%20Approvers.py)
# MAGIC - [description](https://medium.com/@protmaks/databricks-access-requests-export-configured-approvers-c57f16772c60)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC [![Medium](https://img.shields.io/badge/Medium-000000?style=for-the-badge&logo=medium&logoColor=white)](https://medium.com/@protmaks) &nbsp;
# MAGIC [![LinkedIn](https://img.shields.io/badge/LinkedIn-3572A5?style=for-the-badge&logo=linkedin&logoColor=white)](https://linkedin.com/in/protmaks) &nbsp;
# MAGIC [![GitHub](https://img.shields.io/github/followers/protmaks?label=Follow&style=social)](https://github.com/protmaks) &nbsp;
# MAGIC

# COMMAND ----------

# DBTITLE 1,update SDK
# MAGIC %pip install -U databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,imports
import pytz, requests, pyarrow as pa, json
from datetime import datetime, timezone
from pprint import pprint
from collections import defaultdict
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (AccessRequestDestinations, Securable, NotificationDestination, SecurableType, DestinationType)

wc = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC #Load ALL tables

# COMMAND ----------

# DBTITLE 1,all tables
df_all_tables = spark.sql("""
    SELECT
        CONCAT_WS('.', table_catalog, table_schema, table_name) AS table,
        table_owner,
        table_type   
    FROM system.information_schema.tables
    WHERE
        table_catalog NOT IN ('samples', 'system')
        AND table_schema <> 'information_schema'
    ORDER BY table
""")

df_all_tables.createOrReplaceTempView("ALL_TABLES")
#display(df_all_tables)

# COMMAND ----------

# MAGIC %md
# MAGIC # checking

# COMMAND ----------

# DBTITLE 1,ACCESS_DESTINATIONS
EXCLUDE_CATALOGS = {"samples", "system"}
EXCLUDE_SCHEMAS = {"information_schema"}
rows = []

def get_rfa(securable_type: str, full_name: str) -> dict:
    return wc.rfa.get_access_request_destinations(securable_type, full_name).as_dict()

def norm_destinations(rfa: dict):
    ds = (rfa or {}).get("destinations") or []
    return sorted({(d.get("destination_type"), d.get("destination_id")) for d in ds})

def pick_level(dt, ds, dc):
    if not dc and not ds and not dt:
        return None
    if dt and dt != ds:
        return "table"
    if ds and ds != dc:
        return "schema"
    if dc:
        return "catalog"
    return None

for cat in wc.catalogs.list():
    if cat.name in EXCLUDE_CATALOGS:
        continue

    rfa_catalog = get_rfa("catalog", cat.name)
    dc = norm_destinations(rfa_catalog)

    for sch in wc.schemas.list(catalog_name=cat.name):
        if sch.name in EXCLUDE_SCHEMAS:
            continue

        schema_full = sch.full_name
        rfa_schema = get_rfa("schema", schema_full)
        ds = norm_destinations(rfa_schema)

        for tbl in wc.tables.list(catalog_name=cat.name, schema_name=sch.name):
            table_full = tbl.full_name
            rfa_table = get_rfa("table", table_full)
            dt = norm_destinations(rfa_table)

            effective_level = pick_level(dt, ds, dc)

            if effective_level == "table":
                effective = rfa_table
            elif effective_level == "schema":
                effective = rfa_schema
            elif effective_level == "catalog":
                effective = rfa_catalog
            else:
                effective = None

            rows.append(Row(
                catalog=cat.name,
                schema=schema_full,
                table=table_full,
                effective_level=effective_level,
                effective_access_request_destinations_json=(json.dumps(effective, ensure_ascii=False) if effective is not None else None)
            ))

df_rfa = spark.createDataFrame(rows)
df_rfa.createOrReplaceTempView("TABLE_ACCESS_DESTINATIONS")
#display(df_rfa)


# COMMAND ----------

# DBTITLE 1,ACCESS_DESTINATIONS_EMAILS
df_dest_emails = spark.sql("""
    SELECT
        catalog,
        schema,
        table,
        effective_level,
        get(emails, 0) AS EMAIL_1,
        get(emails, 1) AS EMAIL_2,
        get(emails, 2) AS EMAIL_3,
        get(emails, 3) AS EMAIL_4,
        get(emails, 4) AS EMAIL_5
    FROM (
        SELECT
            *,
            TRANSFORM(
                FILTER(
                    FROM_JSON(
                        effective_access_request_destinations_json,
                        'STRUCT<destinations:ARRAY<STRUCT<destination_id:STRING,destination_type:STRING>>>'
                    ).destinations,
                    d -> d.destination_type = 'EMAIL'
                ),
                d -> d.destination_id
            ) AS emails
        FROM TABLE_ACCESS_DESTINATIONS
    )
""")

df_dest_emails.createOrReplaceTempView("TABLE_ACCESS_DESTINATIONS_EMAILS")
#df_dest_emails.display()


# COMMAND ----------

# DBTITLE 1,ALL_TABLES
df_all_tables = spark.sql("""
    SELECT
        CONCAT_WS('.', table_catalog, table_schema, table_name) AS table,
        table_owner,
        table_type   
    FROM system.information_schema.tables
    WHERE
        table_catalog NOT IN ('samples', 'system')
        AND table_schema <> 'information_schema'
    ORDER BY table
""")

df_all_tables.createOrReplaceTempView("ALL_TABLES")

# COMMAND ----------

# DBTITLE 1,SELECT result
# MAGIC %sql
# MAGIC SELECT
# MAGIC   AT.table as TABLE,
# MAGIC   TAD.effective_level as LEVEL,
# MAGIC   TAD.EMAIL_1,
# MAGIC   TAD.EMAIL_2,
# MAGIC   TAD.EMAIL_3,
# MAGIC   TAD.EMAIL_4,
# MAGIC   TAD.EMAIL_5
# MAGIC FROM ALL_TABLES as AT
# MAGIC LEFT JOIN TABLE_ACCESS_DESTINATIONS_EMAILS as TAD
# MAGIC   ON AT.table = TAD.table
# MAGIC ORDER BY table