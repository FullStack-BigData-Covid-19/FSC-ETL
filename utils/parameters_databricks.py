# Databricks notebook source
def get_parameter(parameter_name):
  dbutils.widgets.text(parameter_name,"","")
  return dbutils.widgets.get(parameter_name) 

# COMMAND ----------

def get_secret_parameter(scope_name, key_name):
  return dbutils.secrets.get(scope = scope_name, key = key_name)