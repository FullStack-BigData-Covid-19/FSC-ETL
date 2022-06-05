# Databricks notebook source
# MAGIC %run ./utils/parameters_databricks

# COMMAND ----------

# MAGIC %run ./utils/extract_functions

# COMMAND ----------

# MAGIC %run ./utils/transform_functions

# COMMAND ----------

# MAGIC %run ./utils/load_functions

# COMMAND ----------

import sys
from pyspark.sql.functions import col,regexp_replace,round,countDistinct,current_timestamp,lit,current_date,date_sub,date_format,year as yearpy,month as monthpy, to_date
from pyspark.sql.types import StringType,BooleanType,DateType,DoubleType,IntegerType
from datetime import datetime
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,INFORMATION TO CONNECT AZURE DATALAKE
#key_value = "Z457Q~zHetjQxTJ4huPdA03RZ3RfAwZPWd3H-"
_FULLMODE = get_parameter("FULLMODE")
_TENANT_ID = get_secret_parameter("fsc-secret-scope", 'tenant-id')
_APPLICATIONID  = get_secret_parameter("fsc-secret-scope", 'sp-app-id')
_SERVICEPRINCIPALSECRET = get_secret_parameter("fsc-secret-scope", 'sp-secret')  #_SERVICEPRINCIPALSECRET is collected from the key vault
_DL_PRIMARY_KEY = get_secret_parameter("fsc-secret-scope", 'dl-primary-key')
_SYNAPSE_SQL_PASS = get_secret_parameter("fsc-secret-scope", 'synapse-pass')
_CONTAINER_NAME_RAW = "raw"
_CONTAINER_NAME_CURATED = "curated"
_CONTAINER_NAME_ENTERPRISE = "enterprise"
_MOUNT_POINT_RAW = f"/mnt/etl/{_CONTAINER_NAME_RAW}/"
_MOUNT_POINT_CURATED = f"/mnt/etl/{_CONTAINER_NAME_CURATED}/"
_MOUNT_POINT_ENTERPRISE = f"/mnt/etl/{_CONTAINER_NAME_ENTERPRISE}/"
_STORAGE_ACCOUNT_NAME = "fscdatalakecovid19"

# COMMAND ----------

# DBTITLE 1,MOUNT DATALAKE IN DATABRICKS AND SET SPARK CONFIG.
os.chdir("/databricks/driver")
_WRITEMODE = "overwrite" if _FULLMODE == 'Y' else "append"
was_mounted_raw = mount_datalake(_TENANT_ID, _APPLICATIONID, _SERVICEPRINCIPALSECRET,_CONTAINER_NAME_RAW, _STORAGE_ACCOUNT_NAME, _MOUNT_POINT_RAW)
was_mounted_curated = mount_datalake(_TENANT_ID, _APPLICATIONID, _SERVICEPRINCIPALSECRET,_CONTAINER_NAME_CURATED, _STORAGE_ACCOUNT_NAME, _MOUNT_POINT_CURATED)
was_mounted_enterprise = mount_datalake(_TENANT_ID, _APPLICATIONID, _SERVICEPRINCIPALSECRET,_CONTAINER_NAME_ENTERPRISE, _STORAGE_ACCOUNT_NAME, _MOUNT_POINT_ENTERPRISE)
set_config_spark(_STORAGE_ACCOUNT_NAME,_APPLICATIONID, _SERVICEPRINCIPALSECRET,_TENANT_ID,_DL_PRIMARY_KEY)

# COMMAND ----------

# DBTITLE 1,DOWNLOAD FILES FROM COVID19 REPOSITORY (EXTRACT)
now = datetime.now() # current date and time
year = now.strftime("%Y")
month = now.strftime("%m")
day = now.strftime("%d")

repository_large = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/"
files = ["owid-covid-data.csv", "vaccinations/vaccinations.csv", "hospitalizations/covid-hospitalizations.csv", "excess_mortality/excess_mortality.csv", "jhu/full_data.csv"]
destination_local = f"file:/databricks/driver/covid_files/"
destination_dbfs_archived = f"dbfs:{_MOUNT_POINT_RAW}/archived/{year}/{month}/{day}/"
destination_dbfs_last = f"dbfs:{_MOUNT_POINT_RAW}/last/"
sys.stdout.fileno = lambda: False
download_repo_files(repository_large,files,destination_local)
mv_from_local_to_dbfs(destination_local, destination_dbfs_archived, destination_dbfs_last)
raw_paths_last = get_raws_path(files,destination_dbfs_last) #list of dictionaries with the filenames and raw path
raw_paths_yesterday = get_raws_path(files,f"dbfs:{_MOUNT_POINT_RAW}/archived/{year}/{month}/{str(int(day)-1)}/")

# COMMAND ----------

# DBTITLE 1,PYSPARK FILES TREATMENTS (TRANSFORM)
#CREATE DATAFRAMES WITH THE LATESTS ROWS AND UPDATES IF FULL MODE IS 'N' ELSE TAKE ALL ROWS FROM LAST FILES.
df_owid_covid_data = spark.read.csv(list(filter(lambda raw_path: raw_path['filename'] == 'owid-covid-data.csv',raw_paths_last))[0]['rawpath'], header = True).select("location","iso_code","date","stringency_index", "population", "aged_65_older", "aged_70_older","new_tests","total_tests")
df_vaccinations = spark.read.csv(list(filter(lambda raw_path: raw_path['filename'] == 'vaccinations.csv',raw_paths_last))[0]['rawpath'], header = True).select(col("iso_code").alias("iso_code_vaccs"),col("date").alias("date_vaccs") ,"total_vaccinations","daily_vaccinations","total_boosters")
df_hospitalizations = spark.read.csv(list(filter(lambda raw_path: raw_path['filename'] == 'covid-hospitalizations.csv',raw_paths_last))[0]['rawpath'], header = True).select(col("iso_code").alias("iso_code_hosp"), col("date").alias("date_hosp"),"indicator", "value")
df_excess_mortality = spark.read.csv(list(filter(lambda raw_path: raw_path['filename'] == 'excess_mortality.csv',raw_paths_last))[0]['rawpath'], header = True).select(col("location").alias("location_excess_mort"),col("date").alias("date_excess_mort"),"excess_proj_all_ages")
df_full_data = spark.read.csv(list(filter(lambda raw_path: raw_path['filename'] == 'full_data.csv',raw_paths_last))[0]['rawpath'], header = True).select(col("location").alias("location_full_data"),col("date").alias("date_full_data"),"new_cases","new_deaths","total_cases","total_deaths","weekly_cases","weekly_deaths")


if _FULLMODE != 'Y':
    df_owid_covid_data_yes = spark.read.csv(list(filter(lambda raw_path: raw_path['filename'] == 'owid-covid-data.csv',raw_paths_yesterday))[0]['rawpath'], header = True).select("location","iso_code","date","stringency_index", "population", "aged_65_older", "aged_70_older","new_tests","total_tests")
    df_vaccinations_yes = spark.read.csv(list(filter(lambda raw_path: raw_path['filename'] == 'vaccinations.csv',raw_paths_yesterday))[0]['rawpath'], header = True).select(col("iso_code").alias("iso_code_vaccs"),col("date").alias("date_vaccs") ,"total_vaccinations","daily_vaccinations","total_boosters")
    df_hospitalizations_yes = spark.read.csv(list(filter(lambda raw_path: raw_path['filename'] == 'covid-hospitalizations.csv',raw_paths_yesterday))[0]['rawpath'], header = True).select(col("iso_code").alias("iso_code_hosp"), col("date").alias("date_hosp"),"indicator", "value")
    df_excess_mortality_yes = spark.read.csv(list(filter(lambda raw_path: raw_path['filename'] == 'excess_mortality.csv',raw_paths_yesterday))[0]['rawpath'], header = True).select(col("location").alias("location_excess_mort"),col("date").alias("date_excess_mort"),"excess_proj_all_ages")
    df_full_data_yes = spark.read.csv(list(filter(lambda raw_path: raw_path['filename'] == 'full_data.csv',raw_paths_yesterday))[0]['rawpath'], header = True).select(col("location").alias("location_full_data"),col("date").alias("date_full_data"),"new_cases","new_deaths","total_cases","total_deaths","weekly_cases","weekly_deaths")
    
    df_owid_covid_data = df_owid_covid_data.subtract(df_owid_covid_data_yes) 
    df_vaccinations = df_vaccinations.subtract(df_vaccinations_yes)
    df_hospitalizations = df_hospitalizations.subtract(df_hospitalizations_yes)
    df_excess_mortality = df_excess_mortality.subtract(df_excess_mortality_yes)
    df_full_data = df_full_data_yes.subtract(df_full_data_yes)

# COMMAND ----------

#MAPPING LOCATIONS WITH ISO_CODE
renamed_cols_countries = list(zip(['location', 'iso_code'], ['location_map', 'iso_code_map']))
df_countries_mapping = rename_multiple_cols(df_owid_covid_data.select("location","iso_code").distinct(), renamed_cols_countries)

################# MAP ISO_CODE WITH TABLE THAT ONLY HAS LOCATIONS #################
df_excess_mortality = df_excess_mortality.join(df_countries_mapping,df_excess_mortality.location_excess_mort ==  df_countries_mapping.location_map,"inner").withColumnRenamed("iso_code_map","iso_code_excess_mort").drop('location_map')
df_full_data = df_full_data.join(df_countries_mapping,df_full_data.location_full_data ==  df_countries_mapping.location_map,"inner").withColumnRenamed("iso_code_map","iso_code_full_data").drop('location_map')

################# FILTER HOSPITALIZATIONS METRICS  #################
renamed_cols_daily_hosp = list(zip(['iso_code_hosp', 'date_hosp','value'], ['iso_code_daily_hosp', 'date_daily_hosp',"Daily_hospital_occupancy"]))
renamed_cols_daily_icu = list(zip(['iso_code_hosp', 'date_hosp','value'], ['iso_code_daily_icu', 'date_daily_icu','Daily_icu_occupancy']))
renamed_cols_weekly_hosp = list(zip(['iso_code_hosp', 'date_hosp','value'], ['iso_code_weekly_hosp', 'date_weekly_hosp','Weekly_new_hospital_admissions']))
renamed_cols_weekly_icu = list(zip(['iso_code_hosp', 'date_hosp','value'], ['iso_code_weekly_icu', 'date_weekly_icu','Weekly_new_icu_admissions']))

df_daily_hosp = rename_multiple_cols(df_hospitalizations.filter(df_hospitalizations.indicator == 'Daily hospital occupancy').drop('indicator'),renamed_cols_daily_hosp)
df_daily_icu = rename_multiple_cols(df_hospitalizations.filter(df_hospitalizations.indicator == 'Daily ICU occupancy').drop('indicator'),renamed_cols_daily_icu)
df_weekly_hosp = rename_multiple_cols(df_hospitalizations.filter(df_hospitalizations.indicator == 'Weekly new hospital admissions').drop('indicator'), renamed_cols_weekly_hosp)
df_weekly_icu = rename_multiple_cols(df_hospitalizations.filter(df_hospitalizations.indicator == 'Weekly new ICU admissions').drop('indicator'),renamed_cols_weekly_icu)

# COMMAND ----------

##SEPARATE UPDATES AND INSERTS IF NOT FULLMODE 
datatypes_castings_metrics_fact = [{'type': 'Decimal2', 'fields': ['Daily_hospital_occupancy','Daily_icu_occupancy','Weekly_new_hospital_admissions','Weekly_new_icu_admissions','excess_proj_all_ages']}, #Decimal2 means with only two number after the comma
                      {'type': 'Decimal1', 'fields': ['stringency_index']}, #Decimal1 means with only one number after the comma
                      {'type': 'Integer', 'fields': ['new_cases','new_deaths','total_cases','total_deaths','weekly_cases','weekly_deaths','total_vaccinations','daily_vaccinations','total_boosters','new_tests','total_tests','population','aged_65_older','aged_70_older']},
                      {'type': 'Date', 'fields': ['date']}
                     ]
metrics_fact_curated_location = f'dbfs://{_MOUNT_POINT_CURATED}/metrics_fact/'
delta_metrics_fact_curated = DeltaTable.forPath(spark, metrics_fact_curated_location ) if DeltaTable.isDeltaTable(spark, metrics_fact_curated_location) else None
if _FULLMODE != 'Y':
    update_dfs = [
                  {'file': 'owid_covid_data', 'df': cast_types(df_owid_covid_data.filter(col('date') != date_sub(current_date(),1)).withColumn("_TF_LAST_UPDATE", lit(current_timestamp())),datatypes_castings_metrics_fact).na.fill(value=0).withColumn('Is_updated', lit('Y'))}, 
                  {'file': 'vaccinations', 'df': cast_types(df_vaccinations.filter(col('date_vaccs') != date_sub(current_date(),1)).withColumn("_TF_LAST_UPDATE", lit(current_timestamp())),datatypes_castings_metrics_fact).na.fill(value=0).withColumn('Is_updated', lit('Y'))},
                  {'file': 'excess_mortality', 'df': cast_types(df_excess_mortality.filter(col('date_excess_mort') != date_sub(current_date(),1)).withColumn("_TF_LAST_UPDATE", lit(current_timestamp())),datatypes_castings_metrics_fact).na.fill(value=0).withColumn('Is_updated', lit('Y'))},
                  {'file': 'full_data', 'df': cast_types(df_full_data.filter(col('date_full_data') != date_sub(current_date(),1)).withColumn("_TF_LAST_UPDATE", lit(current_timestamp())),datatypes_castings_metrics_fact).na.fill(value=0).withColumn('Is_updated', lit('Y'))},
                  {'file': 'daily_hospital_occ', 'df': cast_types(df_daily_hosp.filter(col('date_daily_hosp') != date_sub(current_date(),1)).withColumn("_TF_LAST_UPDATE", lit(current_timestamp())),datatypes_castings_metrics_fact).na.fill(value=0).withColumn('Is_updated', lit('Y'))},
                  {'file': 'daily_icu_occ', 'df': cast_types(df_daily_icu.filter(col('date_daily_icu') != date_sub(current_date(),1)).withColumn("_TF_LAST_UPDATE", lit(current_timestamp())),datatypes_castings_metrics_fact).na.fill(value=0).withColumn('Is_updated', lit('Y'))},
                  {'file': 'weekly_hosp_adm', 'df': cast_types(df_weekly_hosp.filter(col('date_weekly_hosp') != date_sub(current_date(),1)).withColumn("_TF_LAST_UPDATE", lit(current_timestamp())),datatypes_castings_metrics_fact).na.fill(value=0).withColumn('Is_updated', lit('Y'))},
                  {'file': 'weekly_icu_adm', 'df': cast_types(df_weekly_icu.filter(col('date_weekly_icu') != date_sub(current_date(),1)).withColumn("_TF_LAST_UPDATE", lit(current_timestamp())),datatypes_castings_metrics_fact).na.fill(value=0).withColumn('Is_updated', lit('Y'))}
                  ]
    
    mappings_updates = [{'file':'owid_covid_data', 'mappings': {'Stringency_index':'updates.stringency_index', 
                                                                'Population': 'updates.population', 
                                                                'Aged_65_older_perc': 'updates.aged_65_older',
                                                                'Aged_70_older_perc': 'updates.aged_70_older',
                                                                'New_tests': 'updates.new_tests',
                                                                'Total_tests': 'updates.total_tests',
                                                                '_TF_LAST_UPDATE':'updates._TF_LAST_UPDATE',
                                                                'Is_updated':'updates.Is_updated'
                                                               }, 'match_condition':'metrics_fact.CodeISO = updates.iso_code and metrics_fact.Date = updates.date'},
                        
                        {'file': 'vaccinations', 'mappings' : { 'Total_vaccinations':'updates.total_vaccinations',
                                                                'Daily_vaccinations': 'updates.daily_vaccinations',
                                                                'Total_boosters_vaccinations': 'updates.total_boosters',
                                                                '_TF_LAST_UPDATE':'updates._TF_LAST_UPDATE',
                                                               'Is_updated': 'updates.Is_updated'
                                                              }, 'match_condition': 'metrics_fact.CodeISO = updates.iso_code_vaccs and metrics_fact.Date = updates.date_vaccs'},
                        
                        {'file': 'excess_mortality', 'mappings' : {'Projection_excess_death':'updates.excess_proj_all_ages',
                                                                  '_TF_LAST_UPDATE':'updates._TF_LAST_UPDATE',
                                                                   'Is_updated' : 'updates.Is_updated'
                                                                  },'match_condition': 'metrics_fact.CodeISO = updates.iso_code_excess_mort and metrics_fact.Date = updates.date_excess_mort'},
                        
                        {'file': 'full_data', 'mappings' : {'New_cases': 'updates.new_cases',
                                                            'New_deaths': 'updates.new_deaths',
                                                            'Total_cases': 'updates.total_cases',
                                                            'Total_deaths': 'updates.total_deaths',
                                                            'Weekly_cases': 'updates.weekly_cases',
                                                            'Weekly_deaths': 'updates.weekly_deaths',
                                                            '_TF_LAST_UPDATE':'updates._TF_LAST_UPDATE',
                                                            'Is_updated' : 'updates.Is_updated'
                                                           }, 'match_condition': 'metrics_fact.CodeISO = updates.iso_code_full_data and metrics_fact.Date = updates.date_full_data'},
                        
                        {'file': 'daily_hospital_occ', 'mappings' : {'Daily_hospital_occupancy':'updates.Daily_hospital_occupancy',
                                                                     '_TF_LAST_UPDATE':'updates._TF_LAST_UPDATE',
                                                                     'Is_updated' : 'updates.Is_updated'
                                                                    } , 'match_condition': 'metrics_fact.CodeISO = updates.iso_code_daily_hosp and metrics_fact.Date = updates.date_daily_hosp'},
                        
                        {'file': 'daily_icu_occ', 'mappings' : {'Daily_icu_occupancy':'updates.Daily_icu_occupancy',
                                                                '_TF_LAST_UPDATE':'updates._TF_LAST_UPDATE',
                                                                'Is_updated' : 'updates.Is_updated'
                                                               } , 'match_condition': 'metrics_fact.CodeISO = updates.iso_code_daily_icu and metrics_fact.Date = updates.date_daily_icu'},
                        
                        {'file': 'weekly_hosp_adm', 'mappings' : {'Weekly_new_hospital_admissions':'updates.Weekly_new_hospital_admissions',
                                                                  '_TF_LAST_UPDATE':'updates._TF_LAST_UPDATE',
                                                                  'Is_updated' : 'updates.Is_updated'
                                                                 } , 'match_condition': 'metrics_fact.CodeISO = updates.iso_code_weekly_hosp and metrics_fact.Date = updates.date_weekly_hosp'},
                        
                        {'file': 'weekly_icu_adm', 'mappings' : {'Weekly_new_icu_admissions':'updates.Weekly_new_icu_admissions',
                                                                 '_TF_LAST_UPDATE':'updates._TF_LAST_UPDATE',
                                                                 'Is_updated' : 'updates.Is_updated'
                                                                } , 'match_condition': 'metrics_fact.CodeISO = updates.iso_code_weekly_icu and metrics_fact.Date = updates.date_weekly_icu'}
                       ]
    
    for update_df in update_dfs: 
        actual_mapping_update = list(filter(lambda mapping: update_df['file'] == mapping['file'] , mappings_updates))[0]
        delta_metrics_fact_curated.alias('metrics_fact') \
        .merge(
            update_df['df'].alias('updates'),
            actual_mapping_update['match_condition']
          ) \
        .whenMatchedUpdate(set = actual_mapping_update['mappings']) \
        .execute()
        
    df_owid_covid_data = df_owid_covid_data.filter(col('date') == date_sub(current_date(),1))
    df_vaccinations = df_vaccinations.filter(col('date_vaccs') == date_sub(current_date(),1))
    df_excess_mortality = df_excess_mortality.filter(col('date_excess_mort') != date_sub(current_date(),1))
    df_full_data = df_full_data.filter(col('date_full_data') == date_sub(current_date(),1))
    df_daily_hosp = df_daily_hosp.filter(col('date_daily_hosp') == date_sub(current_date(),1))
    df_daily_icu = df_daily_icu.filter(col('date_daily_icu') == date_sub(current_date(),1))
    df_weekly_hosp = df_weekly_hosp.filter(col('date_weekly_hosp') == date_sub(current_date(),1))
    df_weekly_icu = df_weekly_icu.filter(col('date_weekly_icu') == date_sub(current_date(),1)) 

# COMMAND ----------

################# FEEDING METRICS_FACT #################
df_metrics_fact = df_owid_covid_data.join(df_excess_mortality, (df_owid_covid_data.iso_code ==  df_excess_mortality.iso_code_excess_mort) & (df_owid_covid_data.date ==  df_excess_mortality.date_excess_mort), "left")
df_metrics_fact = df_metrics_fact.join(df_full_data, (df_metrics_fact.iso_code == df_full_data.iso_code_full_data) & (df_metrics_fact.date == df_full_data.date_full_data), "left")
df_metrics_fact = df_metrics_fact.join(df_vaccinations, (df_metrics_fact.iso_code == df_vaccinations.iso_code_vaccs) & (df_metrics_fact.date == df_vaccinations.date_vaccs), "left")

df_metrics_fact = df_metrics_fact.join(df_daily_hosp, (df_metrics_fact.iso_code == df_daily_hosp.iso_code_daily_hosp) & (df_metrics_fact.date == df_daily_hosp.date_daily_hosp), "left")
df_metrics_fact = df_metrics_fact.join(df_daily_icu, (df_metrics_fact.iso_code == df_daily_icu.iso_code_daily_icu) & (df_metrics_fact.date == df_daily_icu.date_daily_icu), "left")
df_metrics_fact = df_metrics_fact.join(df_weekly_hosp, (df_metrics_fact.iso_code == df_weekly_hosp.iso_code_weekly_hosp) & (df_metrics_fact.date == df_weekly_hosp.date_weekly_hosp), "left")
df_metrics_fact = df_metrics_fact.join(df_weekly_icu, (df_metrics_fact.iso_code == df_weekly_icu.iso_code_weekly_icu) & (df_metrics_fact.date == df_weekly_icu.date_weekly_icu), "left")
df_metrics_fact = df_metrics_fact.withColumn("_TF_LAST_UPDATE", lit(current_timestamp()))
################# REMOVING COLS NOT NEEDED #################
cols_to_drop = ("location_excess_mort","location_full_data","iso_code_excess_mort","iso_code_full_data","iso_code_daily_hosp","iso_code_daily_icu","iso_code_weekly_hosp","iso_code_weekly_icu","iso_code_vaccs","date_excess_mort","date_full_data","date_daily_hosp","date_daily_icu","date_weekly_hosp","date_weekly_icu","date_vaccs")
df_metrics_fact = df_metrics_fact.drop(*cols_to_drop)
################# CASTING DATATYPES AND ADDING COLUMN FORMAT #################
df_metrics_fact = cast_types(df_metrics_fact, datatypes_castings_metrics_fact).na.fill(value=0)
renamed_final_fields = list(zip(['iso_code','aged_65_older','aged_70_older','total_boosters','excess_proj_all_ages'], ['CodeISO','Aged_65_older_perc','Aged_70_older_perc','Total_boosters_vaccinations','Projection_excess_death']))
df_metrics_fact = uppercase_first_letter_cols(rename_multiple_cols(df_metrics_fact, renamed_final_fields), ['*'])
df_metrics_fact = df_metrics_fact.withColumn("Year", yearpy(df_metrics_fact.Date)).withColumn("Month", date_format(df_metrics_fact.Date,'MM')).withColumn('Is_updated', lit('N'))
################# INSERT NEW ROWS IN CURATED #################
if _FULLMODE == 'Y' and DeltaTable.isDeltaTable(spark, metrics_fact_curated_location):
        deltaTable = DeltaTable.forPath(spark, metrics_fact_curated_location)
        deltaTable.delete()
        deltaTable.vacuum(0)
df_metrics_fact.coalesce(1).write.partitionBy("Year","Month").format("delta").mode("append").save(metrics_fact_curated_location) #With this line we decrease the number of partitions in memory to 1 and we partition the data by Year and Month

# COMMAND ----------

# DBTITLE 1,INSERT OR UPDATE DATA IN SYNAPSE DATAWAREHOUSE AND ENRICHED (LOAD)
#dw_jbdc_extra_options = "encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;"
sql_dw_url = f"jdbc:sqlserver://fsc-synapse-workspace.sql.azuresynapse.net:1433;database=fsc_dwh_sql;user=smitexx@fsc-synapse-workspace;password={_SYNAPSE_SQL_PASS};encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;"
sql_table = "MetricsCovid_Fact" 
sql_table_staging = "MetricsCovid_Fact_tmp"
lookup_cols = "Date|CodeISO"
update_col = "_TF_LAST_UPDATE"
hash_col = "CodeISO"
temp_dir = f"abfss://{_CONTAINER_NAME_CURATED}@{_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/tempDir"

# COMMAND ----------

################# GET MAX _SK_METRICS_FACT FROM SYNAPSE #################
df_max_key = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", sql_dw_url) \
  .option("tempDir", temp_dir) \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("query", "select max(_SK_METRICS_FACT) as value from MetricsCovid_Fact") \
  .load()
################# GET ALL ROWS FROM CURATED #################
df_metrics_fact_curated = spark \
                      .read \
                      .format("delta") \
                      .load(metrics_fact_curated_location) 
################ GET SKs FROM SYNAPSE #################
df_synapse = spark.read \
  .format("com.databricks.spark.sqldw") \
  .option("url", sql_dw_url) \
  .option("tempDir", f"abfss://{_CONTAINER_NAME_CURATED}@{_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/tempDir") \
  .option("forwardSparkAzureStorageCredentials", "true") \
  .option("query", "select _SK_METRICS_FACT, Date as Date_syn, CodeISO as CodeISO_syn from MetricsCovid_Fact") \
  .load()

# COMMAND ----------

metrics_fact_enterprise_location = f"dbfs://{_MOUNT_POINT_ENTERPRISE}/MetricsCovid_Fact/" 
delta_metrics_fact_enterprise = DeltaTable.forPath(spark, metrics_fact_enterprise_location) if DeltaTable.isDeltaTable(spark, metrics_fact_enterprise_location) else None
df_mf_curated_update = df_metrics_fact_curated.filter(col('Is_updated') == 'Y').join(df_synapse, (df_metrics_fact_curated.Date == df_synapse.Date_syn) & (df_metrics_fact_curated.CodeISO == df_synapse.CodeISO_syn), "inner").drop("Year","Month","Date_syn","CodeISO_Syn","Is_updated").withColumn("_TF_LAST_UPDATE", lit(current_timestamp()))
df_mf_curated_insert = df_metrics_fact_curated.filter(col("Date") == date_sub(current_date(),1)).drop("Year","Month","Is_updated").withColumn("_TF_LAST_UPDATE", lit(current_timestamp())) if _FULLMODE != 'Y' else df_metrics_fact_curated.drop("Year","Month","Is_updated").withColumn("_TF_LAST_UPDATE", lit(current_timestamp()))
#UPSERT RECORDS
if not df_mf_curated_insert.rdd.isEmpty():
    print(f"Hay {df_mf_curated_insert.count()} filas que insertar.")     
    #print(f"Hay {df_mf_curated_update.count()} filas que actualizar.")
    max_key_number = 0 if (df_max_key.first()['value'] == None or _FULLMODE == 'Y') else df_max_key.first()['value']
    df_metrics_fact_schema = df_mf_curated_insert.withColumn("_SK_METRICS_FACT", lit(1))
    rdd_sk = df_mf_curated_insert.rdd.zipWithIndex().map(lambda rowtuple: list(rowtuple[0]) + [rowtuple[1]+1+max_key_number]) #rowtuple[0] is the row and rowtuple[1] is the incremental id.
    df_mf_curated_insert = sqlContext.createDataFrame(rdd_sk, schema=df_metrics_fact_schema.schema)
    df_metrics_fact_upsert = df_mf_curated_insert.union(df_mf_curated_update).select("_SK_METRICS_FACT",'_TF_LAST_UPDATE','Location','CodeISO','Date','New_cases','New_deaths','Total_cases','Total_deaths','Weekly_cases','Weekly_deaths','Daily_hospital_occupancy','Daily_icu_occupancy','Weekly_new_hospital_admissions','Weekly_new_icu_admissions','Total_vaccinations','Daily_vaccinations','Total_boosters_vaccinations','New_tests','Total_tests','Projection_excess_death','Stringency_index','Population','Aged_65_older_perc','Aged_70_older_perc')
    #UPSERT SYNAPSE
    upsertDWH(df_metrics_fact_upsert,sql_table_staging,sql_table,lookup_cols,update_col,hash_col,sql_dw_url,temp_dir)
    #UPSERT DATALAKE
    upsertDL(delta_metrics_fact_enterprise,df_metrics_fact_upsert,metrics_fact_enterprise_location)
#RESET IS_UPDATED FIELD
if not df_mf_curated_update.rdd.isEmpty():
    #UPDATE IS_UPDATED = 'N' IN CURATED
    df_mf_curated_update = df_mf_curated_update.withColumn('Is_updated', lit('N'))
    delta_metrics_fact_curated.alias('metrics_fact') \
    .merge(
        df_mf_curated_update.alias('updates'),
        'metrics_fact.Date = updates.Date and metrics_fact.CodeISO = updates.CodeISO' #Match Condition
      ) \
    .whenMatchedUpdate(set = {
       'Is_updated' : 'updates.Is_updated'
    }) \
    .execute()   