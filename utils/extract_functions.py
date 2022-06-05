# Databricks notebook source
import os 
import wget

# COMMAND ----------

def download_file(repository,file,destination):
    destination = f"./{destination.split('/')[-2]}/"
    source = f"{repository}{file}"
    file_has_folder = len(file.split("/")) > 1
    file = file if not file_has_folder else file.split("/")[1]
    if not os.path.isfile(f'{destination}{file}'):
        wget.download(source,destination)

# COMMAND ----------

def download_repo_files(repository,files,destination):
    dbutils.fs.mkdirs(destination)
    repo_downloads = [download_file(repository,file,destination) for file in files]
    return dbutils.fs.ls(destination)

# COMMAND ----------

def mv_from_local_to_dbfs(destination_local, destination_dbfs_archived, destination_dbfs_last):
    dbutils.fs.rm(destination_dbfs_archived, True)
    dbutils.fs.rm(destination_dbfs_last, True)
    dbutils.fs.cp(destination_local, destination_dbfs_archived, True)
    dbutils.fs.mv(destination_local, destination_dbfs_last, True)

# COMMAND ----------

def get_raws_path(files,mount_point_raw):
    return list(map(lambda file: {'filename': file, 'rawpath': f"{mount_point_raw}{file}"} if len(file.split('/')) == 1 else {'filename':file.split("/")[-1], 'rawpath': f"{mount_point_raw}{file.split('/')[-1]}"} ,files))

# COMMAND ----------

def mount_datalake(tenant_id, application_id, key_value, container_name, storage_account_name, mount_point):
    try:
        dbutils.fs.ls(f"dbfs:{mount_point}")
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            directory = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
            configs = {
            "fs.azure.account.auth.type" : "OAuth",
            "fs.azure.account.oauth.provider.type":"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": application_id,
            "fs.azure.account.oauth2.client.secret": key_value,
            "fs.azure.account.oauth2.client.endpoint": directory 
            }
            if not any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
                dbutils.fs.mount(
                source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
                mount_point = mount_point,
                extra_configs = configs)
            return False
    return True

# COMMAND ----------

def set_config_spark(storage_account_name, application_id, service_principal_secret,tenant_id,dl_primary_key):
    spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY") 
    spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", application_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", service_principal_secret)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)
    spark.conf.set(
  f"fs.azure.account.key.{_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",dl_primary_key)