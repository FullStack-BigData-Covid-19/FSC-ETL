# Databricks notebook source
def upsertDWH(df,dwhStagingTable,dwhTargetTable,lookupColumns,deltaName,dwhStagingDistributionColumn,sql_dw_url, temp_dir):
     
    #STEP1: Derive dynamic delete statement to delete existing record from TARGET if the source record is newer
    lookupCols =lookupColumns.split("|")
    whereClause=""
    for col in lookupCols:
      whereClause= whereClause + dwhStagingTable  +"."+ col  + "="+ dwhTargetTable +"." + col + " and "
 
    if deltaName is not None and  len(deltaName) >0:
      #Check if the last updated is greater than existing record
      whereClause= whereClause + dwhStagingTable  +"."+ deltaName  + ">="+ dwhTargetTable +"." + deltaName
    else:
      #remove last "and"
      remove="and"
      reverse_remove=remove[::-1]
      whereClause = whereClause[::-1].replace(reverse_remove,"",1)[::-1]
 
    deleteSQL = "delete from " + dwhTargetTable + " where exists (select 1 from " + dwhStagingTable + " where " +whereClause +");"
 
    #STEP2: Delete existing records but outdated records from SOURCE
    whereClause=""
    for col in lookupCols:
      whereClause= whereClause + dwhTargetTable  +"."+ col  + "="+ dwhStagingTable +"." + col + " and "
 
    if deltaName is not None and  len(deltaName) >0:
      #Check if the last updated is lesser than existing record
      whereClause= whereClause + dwhTargetTable  +"."+ deltaName  + "> "+ dwhStagingTable +"." + deltaName
    else:
      #remove last "and"
      remove="and"
      reverse_remove=remove[::-1]
      whereClause = whereClause[::-1].replace(reverse_remove,"",1)[::-1]
 
    deleteOutdatedSQL = "delete from " + dwhStagingTable + " where exists (select 1 from " + dwhTargetTable + " where " + whereClause + " );"
    #print("deleteOutdatedSQL={}".format(deleteOutdatedSQL))
 
    #STEP3: Insert SQL
    insertSQL ="Insert Into " + dwhTargetTable + " select * from " + dwhStagingTable +";"
    #print("insertSQL={}".format(insertSQL))
 
    #consolidate post actions SQL
    postActionsSQL = deleteSQL + deleteOutdatedSQL + insertSQL
    print("postActionsSQL={}".format(postActionsSQL))
 
    #Use Hash Distribution on STG table where possible
    if dwhStagingDistributionColumn is not None and len(dwhStagingDistributionColumn) > 0:
      stgTableOptions ="CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = HASH (" +  dwhStagingDistributionColumn + ")"
    else:
      stgTableOptions ="CLUSTERED COLUMNSTORE INDEX, DISTRIBUTION = ROUND_ROBIN"
     
    #Upsert/Merge to Target using STG postActions
    df.write.format("com.databricks.spark.sqldw")\
      .option("url", sql_dw_url).option("forwardSparkAzureStorageCredentials", "true")\
      .option("dbTable",dwhStagingTable)\
      .option("tableOptions",stgTableOptions)\
      .option("tempDir",temp_dir)\
      .option("maxStrLength",4000)\
      .option("postActions",postActionsSQL)\
      .mode("overwrite").save()

# COMMAND ----------

def upsertDL(df_delta_table,df_upsert,metrics_fact_enterprise_location):
    if df_delta_table:
        df_delta_table.alias('MetricsCovid_Fact') \
        .merge(
            df_upsert.alias('updates'),
            'MetricsCovid_Fact.Date = updates.Date and MetricsCovid_Fact.CodeISO = updates.CodeISO' #Match Condition
          ) \
        .whenMatchedUpdate(set = {
            'New_cases' : 'updates.New_cases',
            'New_deaths' : 'updates.New_deaths',
            'Total_cases' : 'updates.Total_cases',
            'Total_deaths' : 'updates.Total_deaths',
            'Weekly_cases' : 'updates.Weekly_cases',
            'Weekly_deaths' : 'updates.Weekly_deaths',
            'Daily_hospital_occupancy' : 'updates.Daily_hospital_occupancy',
            'Daily_icu_occupancy' : 'updates.Daily_icu_occupancy',
            'Weekly_new_hospital_admissions' : 'updates.Weekly_new_hospital_admissions',
            'Weekly_new_icu_admissions' : 'updates.Weekly_new_icu_admissions',
            'Total_vaccinations' : 'updates.Total_vaccinations',
            'Daily_vaccinations' : 'updates.Daily_vaccinations',
            'Total_boosters_vaccinations' : 'updates.Total_boosters_vaccinations',
            'New_tests' : 'updates.New_tests',
            'Total_tests' : 'updates.Total_tests',
            'Projection_excess_death' : 'updates.Projection_excess_death',
            'Stringency_index' : 'updates.Stringency_index',
            'Population' : 'updates.Population',
            'Aged_65_older_perc' : 'updates.Aged_65_older_perc',
            'Aged_70_older_perc' : 'updates.Aged_70_older_perc'
        }) \
        .whenNotMatchedInsert(values =
        {
            '_SK_METRICS_FACT' : 'updates._SK_METRICS_FACT',
            '_TF_LAST_UPDATE' : 'updates._TF_LAST_UPDATE',
            'Location' : 'updates.Location',
            'CodeISO' : 'updates.CodeISO',
            'Date' : 'updates.Date',
            'New_cases' : 'updates.New_cases',
            'New_deaths' : 'updates.New_deaths',
            'Total_cases' : 'updates.Total_cases',
            'Total_deaths' : 'updates.Total_deaths',
            'Weekly_cases' : 'updates.Weekly_cases',
            'Weekly_deaths' : 'updates.Weekly_deaths',
            'Daily_hospital_occupancy' : 'updates.Daily_hospital_occupancy',
            'Daily_icu_occupancy' : 'updates.Daily_icu_occupancy',
            'Weekly_new_hospital_admissions' : 'updates.Weekly_new_hospital_admissions',
            'Weekly_new_icu_admissions' : 'updates.Weekly_new_icu_admissions',
            'Total_vaccinations' : 'updates.Total_vaccinations',
            'Daily_vaccinations' : 'updates.Daily_vaccinations',
            'Total_boosters_vaccinations' : 'updates.Total_boosters_vaccinations',
            'New_tests' : 'updates.New_tests',
            'Total_tests' : 'updates.Total_tests',
            'Projection_excess_death' : 'updates.Projection_excess_death',
            'Stringency_index' : 'updates.Stringency_index',
            'Population' : 'updates.Population',
            'Aged_65_older_perc' : 'updates.Aged_65_older_perc',
            'Aged_70_older_perc' : 'updates.Aged_70_older_perc'
        }
      ) \
        .execute()
    else:
        df_upsert.coalesce(1).write.format("delta").mode("append").save(metrics_fact_enterprise_location)