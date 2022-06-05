# Databricks notebook source
def rename_multiple_cols(df, list_mappings):
    for col_mapped in list_mappings:
        old_col, new_col = col_mapped
        df = df.withColumnRenamed(old_col,new_col)
    return df

def uppercase_first_letter_cols(df, fields):
    if fields[0] == '*':
        for col in df.columns:
            df = df.withColumnRenamed(col, col[0].upper()+col[1:])
    else:
        for field in fields:
            df = df.withColumnRenamed(field, field[0].upper() + field[1:]) 
    return df

def lowercase_first_letter_cols(df, fields):
    if fields[0] == '*':
        for col in df.columns:
            df = df.withColumnRenamed(col, col.lower()) 
    else:
        for field in fields:
            df = df.withColumnRenamed(field, field.lower()) 
    return df
    

# COMMAND ----------

def cast_types(df, datatypes_casting):
    for dict_type in datatypes_casting:
        fields = dict_type['fields']
        if dict_type['type'] == 'Decimal2':
            for field in fields:
                if field in df.schema.fieldNames():
                    df = df.withColumn(field,round(getattr(df, field).cast(DoubleType()),2)) 
        elif dict_type['type'] == 'Decimal1':
            for field in fields:
                if field in df.schema.fieldNames():
                    df = df.withColumn(field,round(getattr(df, field).cast(DoubleType()),1))
        elif dict_type['type'] == 'Integer':
            for field in fields:
                if field in df.schema.fieldNames():
                    df = df.withColumn(field,getattr(df, field).cast(IntegerType()))
        elif dict_type['type'] == 'Date':
            for field in fields:
                if field in df.schema.fieldNames():
                    df = df.withColumn(field,getattr(df, field).cast(DateType()))  
    return df