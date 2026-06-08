# Databricks notebook source
# MAGIC %md
# MAGIC # Automate Start/Stop Databricks Apps
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC _author_: **Maksim Pachkouski** - [Databricks MVP | Senior Data Engineer]
# MAGIC - [file source](https://github.com/protmaks/Databricks/blob/main/)
# MAGIC - [description](https://medium.com/@protmaks/)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC [![Medium](https://img.shields.io/badge/Medium-000000?style=for-the-badge&logo=medium&logoColor=white)](https://medium.com/@protmaks) &nbsp;
# MAGIC [![LinkedIn](https://img.shields.io/badge/LinkedIn-3572A5?style=for-the-badge&logo=linkedin&logoColor=white)](https://linkedin.com/in/protmaks) &nbsp;
# MAGIC [![GitHub](https://img.shields.io/github/followers/protmaks?label=Follow&style=social)](https://github.com/protmaks) &nbsp;

# COMMAND ----------

# DBTITLE 1,imports
from databricks.sdk import WorkspaceClient
import json
from pyspark.sql.functions import schema_of_json, lit, from_json, col

w = WorkspaceClient()
apps = w.api_client.do("GET", "/api/2.0/apps")

# COMMAND ----------

# DBTITLE 1,get status
def get_apps_status(apps_response):
    json_data = [json.dumps(a) for a in apps_response["apps"]]
    raw_df = spark.createDataFrame([(j,) for j in json_data], ["value"])
    sample_schema = schema_of_json(lit(json_data[0]))
    apps_df = raw_df.select(from_json(col("value"), sample_schema).alias("data")).select("data.*")
    apps_df = apps_df.select("name", "id", "compute_status.state", "update_time", "compute_size", "service_principal_id", "service_principal_name")
    return apps_df

apps = w.api_client.do("GET", "/api/2.0/apps")
apps_df = get_apps_status(apps)
display(apps_df)

# COMMAND ----------

# DBTITLE 1,wigets
app_names = ["all"] + [a["name"] for a in apps["apps"]]
dbutils.widgets.dropdown("app_name", "all", app_names)
app_name = dbutils.widgets.get("app_name")

dbutils.widgets.dropdown("app_command", "stop", ["start", "stop"])
app_command = dbutils.widgets.get("app_command")

print(f"app_name: {app_name}")
print(f"app_command: {app_command}")

# COMMAND ----------

# DBTITLE 1,start/stop
def start_stop_app(name, command):
    app_info = w.api_client.do("GET", f"/api/2.0/apps/{name}")
    current_state = app_info.get("compute_status", {}).get("state", "UNKNOWN")

    if command == "start" and current_state in ("STARTING", "ACTIVE"):
        print(f"  '{name}' is already {current_state} — skipped.")
    elif command == "stop" and current_state in ("STOPPING", "STOPPED"):
        print(f"  '{name}' is already {current_state} — skipped.")
    else:
        w.api_client.do("POST", f"/api/2.0/apps/{name}/{command}")
        print(f"  '{name}' — {command}ed successfully.")


if app_name.lower() == "all":
    apps = w.api_client.do("GET", "/api/2.0/apps")
    app_names = [a["name"] for a in apps["apps"]]
    print(f"{app_command.upper()} all apps ({len(app_names)}):")
    for name in app_names:
        start_stop_app(name, app_command)
else:
    start_stop_app(app_name, app_command)


apps = w.api_client.do("GET", "/api/2.0/apps")
apps_df2 = get_apps_status(apps)
display(apps_df2)