# Databricks notebook source
# MAGIC %md
# MAGIC # Set Access Request Approvers in Databricks from  Excel via API
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC _author_: **Maksim Pachkouski** - [Databricks MVP | Senior Data Engineer]
# MAGIC - [file source](https://github.com/protmaks/Databricks/)
# MAGIC - [description](https://medium.com/@protmaks)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC [![Medium](https://img.shields.io/badge/Medium-000000?style=for-the-badge&logo=medium&logoColor=white)](https://medium.com/@protmaks) &nbsp;
# MAGIC [![LinkedIn](https://img.shields.io/badge/LinkedIn-3572A5?style=for-the-badge&logo=linkedin&logoColor=white)](https://linkedin.com/in/protmaks) &nbsp;
# MAGIC [![GitHub](https://img.shields.io/github/followers/protmaks?label=Follow&style=social)](https://github.com/protmaks) &nbsp;

# COMMAND ----------

# DBTITLE 1,Update SDK
# MAGIC %pip install -U databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,imports
import pytz, requests, pyarrow as pa, json
from databricks.sdk import WorkspaceClient
from datetime import datetime, timezone
from pprint import pprint
from pyspark.sql import Row
from collections import defaultdict
from pyspark.sql.functions import *
from pyspark.sql.types import *
from databricks.sdk.service.catalog import (AccessRequestDestinations, Securable, NotificationDestination, SecurableType, DestinationType)

wc = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC #Manual set

# COMMAND ----------

# DBTITLE 1,set for the table
table_full_name = "qa.new_schema.new4"

payload = AccessRequestDestinations(
    securable=Securable(type=SecurableType.TABLE, full_name=table_full_name),
    destinations=[
        NotificationDestination(
            destination_type=DestinationType.EMAIL,
            destination_id="data-approvers@company.com",
        )
    ],
)

wc.rfa.update_access_request_destinations(payload, update_mask="destinations")

# COMMAND ----------

# DBTITLE 1,set many emails for the table
securable_full_name = "qa.new_schema.new4"

payload = AccessRequestDestinations(
    securable=Securable(type=SecurableType.TABLE, full_name=securable_full_name),
    destinations=[
        NotificationDestination(destination_type=DestinationType.EMAIL, destination_id="approver1@company.com"),
        NotificationDestination(destination_type=DestinationType.EMAIL, destination_id="approver2@company.com"),
        NotificationDestination(destination_type=DestinationType.EMAIL, destination_id="approver3@company.com"),

        NotificationDestination(destination_type=DestinationType.SLACK, destination_id="11111111-1111-1111-1111-111111111111"),
        NotificationDestination(destination_type=DestinationType.TEAMS, destination_id="22222222-2222-2222-2222-222222222222"),
        NotificationDestination(destination_type=DestinationType.WEBHOOK, destination_id="33333333-3333-3333-3333-333333333333"),
    ],
)

wc.rfa.update_access_request_destinations(payload, update_mask="destinations")

# COMMAND ----------

# MAGIC %md
# MAGIC #Auto SET

# COMMAND ----------

# DBTITLE 1,Parameters
path = '/Volumes/dev/edl_test/volume_test/data_approvers_tst.xlsx' # REPLACE WITH YOUR FILE

# COMMAND ----------

# DBTITLE 1,schemas from Excel
df_excel_schemes = spark.read.option("headerRows", 1).option("dataAddress", "schemes").excel(path)
df_excel_schemes.createOrReplaceTempView("EXCEL_SCHEMES")
df_excel_schemes.display()

# COMMAND ----------

# DBTITLE 1,tables from Excel
df_excel_tables = spark.read.option("headerRows", 1).option("dataAddress", "tables").excel(path)
df_excel_tables.createOrReplaceTempView("EXCEL_TABLES")
df_excel_tables.display()

# COMMAND ----------

# DBTITLE 1,def infer_securable_type_by_dots
def infer_securable_type_by_dots(full_name: str) -> SecurableType:
    dots = full_name.count(".")

    if dots == 2:
        return SecurableType.TABLE
    if dots == 1:
        return SecurableType.SCHEMA
    if dots == 0:
        return SecurableType.CATALOG

    raise ValueError(f"Error '{full_name}' (dots: {dots})")

# COMMAND ----------

# DBTITLE 1,def set_approvers_from_row
error_rows = []
success_counts = {
    "schema": 0,
    "table": 0
}

def set_approvers_from_row(row):
    securable_type = infer_securable_type_by_dots(row.TABLE)
    print(f"Processing: {row.TABLE}, inferred type: {securable_type}")

    if securable_type == SecurableType.SCHEMA:
        level = "schema"
    elif securable_type == SecurableType.TABLE:
        level = "table"
    else:
        level = "catalog"

    emails = [
        row.EMAIL_1,
        row.EMAIL_2,
        row.EMAIL_3,
        row.EMAIL_4,
        row.EMAIL_5,
    ]
    emails = [e.strip() for e in emails if e and e.strip()]

    if not emails:
        error_rows.append(Row(FULL_NAME=row.TABLE, LEVEL=level, ERROR="No EMAILs provided"))
        return

    payload = AccessRequestDestinations(
        securable=Securable(type=securable_type, full_name=row.TABLE),
        destinations=[NotificationDestination(destination_type=DestinationType.EMAIL, destination_id=e) for e in emails],
    )

    try:
        wc.rfa.update_access_request_destinations(payload, update_mask="destinations")
        success_counts[level] += 1

    except Exception as e:
        error_rows.append(Row(FULL_NAME=row.TABLE, LEVEL=level, ERROR=str(e)))

# COMMAND ----------

# DBTITLE 1,df to dicts
schemas = df_excel_schemes.select(
    trim("TABLE").alias("TABLE"),
    trim("EMAIL_1").alias("EMAIL_1"),
    trim("EMAIL_2").alias("EMAIL_2"),
    trim("EMAIL_3").alias("EMAIL_3"),
    trim("EMAIL_4").alias("EMAIL_4"),
    trim("EMAIL_5").alias("EMAIL_5"),
).collect()

tables = df_excel_tables.select(
    trim("TABLE").alias("TABLE"),
    trim("EMAIL_1").alias("EMAIL_1"),
    trim("EMAIL_2").alias("EMAIL_2"),
    trim("EMAIL_3").alias("EMAIL_3"),
    trim("EMAIL_4").alias("EMAIL_4"),
    trim("EMAIL_5").alias("EMAIL_5"),
).collect()

# COMMAND ----------

# DBTITLE 1,set approvers
for row in schemas:
    set_approvers_from_row(row)

for row in tables:
    set_approvers_from_row(row)

# COMMAND ----------

# DBTITLE 1,show errors
print("===== SUMMARY =====")
print(f"Schemas successfully updated : {success_counts['schema']}")
print(f"Tables successfully updated  : {success_counts['table']}")
print(f"Total failures               : {df_errors.count()}\n")
print("===== ERROR DETAILS:")

df_errors = spark.createDataFrame(error_rows)
df_errors.display()