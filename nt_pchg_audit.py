# Databricks notebook source
# MAGIC %run "../General/nt_user_defined_methods"

# COMMAND ----------

#Declare variables and import libraries
import os 
import datetime
import json
import glob
import csv
import time
import gzip
import pandas as pd
from azure.storage.fileshare import ShareFileClient, ShareServiceClient
from zipfile import ZipFile
from os.path import basename
from pyspark.sql.functions import substring
from pyspark.sql import functions as f
from pyspark.sql.functions import lit
from pyspark.sql.types import DecimalType
from decimal import Decimal
from pyspark.sql.types import IntegerType, StringType,DateType,LongType,DoubleType

try:
    
    jsonContext = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
    context = json.loads(jsonContext)
    run_id_obj = context.get('currentRunId', {})
    varRunId   = run_id_obj.get('id', None) if run_id_obj else -1
    varJobId   = context.get('tags', {}).get('jobId', None)
    if varJobId == None:
        varJobId = -1
 
except Exception as e:   
    raise (e)

# COMMAND ----------


varCollection = "priceChangeAudit"

# COMMAND ----------


#list out all the files in the  fileshare
try:
    # set root directory
    varMntpoint = mountPoint
    varRootMnt = "/dbfs" + mountPoint
    os.environ['varRootMnt'] = varRootMnt
    from azure.storage.fileshare import ShareFileClient ,ShareServiceClient
    from azure.storage.fileshare import ShareDirectoryClient
    parent_dir_des = ShareDirectoryClient.from_connection_string(conn_str=conn_str,
    share_name=share_name, directory_path=directory_path)
  
except Exception as e:   
    raise (e)

# COMMAND ----------

try:
    
    latest_audit_df=spark.sql("select uploaded_time from "+delta_schema+".stg_file_check where file_name='pchg_audit'")
    latest_audit_pd= latest_audit_df.toPandas()
    latest_audit_time=latest_audit_pd.loc[0,'uploaded_time']
    spark.sql("delete from "+delta_schema+".stg_pchg_audit")
    
except Exception as e:   
    raise (e) 

# COMMAND ----------

 
try:
    latest_pchg_audt = 0 
    pchg_audit_new_file = 'N'   
    for i in parent_dir_des.list_directories_and_files():
        filetime_pchg_audt = i.values()[0][14:28]
        if filetime_pchg_audt[0].isdigit():
            if int(filetime_pchg_audt) > int(latest_audit_time):
                latest_pchg_audt = int(filetime_pchg_audt)
                latest_pchg_audt_file = i.values()[0]
                from io import BytesIO
                
                file_path=directory_path+latest_pchg_audt_file
                file_client = ShareFileClient.from_connection_string(conn_str=conn_str,
                share_name=share_name, file_path=file_path)
                download_stream = file_client.download_file()
                file_content = download_stream.readall()
                with gzip.GzipFile(fileobj=BytesIO(file_content)) as gz:
                        df = pd.read_csv(gz, delimiter='|')
                        if not df.empty:
                            df = spark.createDataFrame(df)
                            df = df.withColumn("USER_EMAIL_ID", df["USER_EMAIL_ID"].cast(StringType())) \
                                .withColumn("USER_ID", df["USER_ID"].cast(StringType())) \
                                .withColumn("ROG", df["ROG"].cast(StringType())) \
                                .withColumn("PRICE_AREA", df["PRICE_AREA"].cast(IntegerType())) \
                                .withColumn("PRICE_AREA_SL", df["PRICE_AREA_SL"].cast(StringType())) \
                                .withColumn("PRICE_GROUP_ID", df["PRICE_GROUP_ID"].cast(StringType())) \
                                .withColumn("NEW_PRICE", df["NEW_PRICE"].cast(DoubleType())) \
                                .withColumn("NEW_PRICE_EFFECTIVE_DATE", df["NEW_PRICE_EFFECTIVE_DATE"].cast(StringType())) \
                                .withColumn("CURRENT_PRICE", df["CURRENT_PRICE"].cast(DoubleType())) \
                                .withColumn("CALCULATED_PRICE", df["CALCULATED_PRICE"].cast(IntegerType())) \
                                .withColumn("CURRENT_COST", df["CURRENT_COST"].cast(DoubleType())) \
                                .withColumn("CURRENT_COST_EFFECTIVE_DATE", df["CURRENT_COST_EFFECTIVE_DATE"].cast(StringType())) \
                                .withColumn("FUTURE_COST", df["FUTURE_COST"].cast(DoubleType())) \
                                .withColumn("FUTURE_COST_EFFECTIVE_DATE", df["FUTURE_COST_EFFECTIVE_DATE"].cast(StringType())) \
                                .withColumn("MULTI_FACTOR_PRICE", df["MULTI_FACTOR_PRICE"].cast(DoubleType())) \
                                .withColumn("MULTI_FACTOR_QUANTITY", df["MULTI_FACTOR_QUANTITY"].cast(DoubleType())) \
                                .withColumn("WORKFLOW_STATE", df["WORKFLOW_STATE"].cast(StringType())) \
                                .withColumn("WORKFLOW_TIME_STAMP", df["WORKFLOW_TIME_STAMP"].cast(DateType())) \
                                .withColumn("EXPORT_TIME_STAMP", df["EXPORT_TIME_STAMP"].cast(DateType())) \
                                .withColumn("WORKFLOW_TRANSITION_COMMENTS", df["WORKFLOW_TRANSITION_COMMENTS"].cast(StringType())) \
                                .withColumn("MSGSEQNBR", df["MSGSEQNBR"].cast(IntegerType())) \
                                .withColumn("TOTALMSGCOUNT", df["TOTALMSGCOUNT"].cast(IntegerType()))
                            
                            addDeltaTable("stg_pchg_audit",df,"append")
                        
                        pchg_audit_new_file = 'Y'
                        new_file_path = pchg_audit_me01r_fileshare_archive
                        file_path =directory_path+latest_pchg_audt_file
                        file_name = os.path.basename(file_path)
                        new_file_path_with_name = os.path.join(new_file_path, file_name)
                        file_client = ShareFileClient.from_connection_string(conn_str, share_name, file_path)
                        new_file_client = ShareFileClient.from_connection_string(conn_str, share_name, new_file_path_with_name)
                        new_file_client.upload_file(file_content)
                        file_client.delete_file()
                        
                         
except Exception as e:   
    raise e

# COMMAND ----------

#Determine if records are present in price_group_new table
try:
    df_pchg_audtt="""select *  from  {0}.stg_pchg_audit"""
    df_pchg_audtt=getDeltaTable(None,df_pchg_audtt.format(delta_schema))
    df_pchg_audt_count=df_pchg_audtt.count()
    if df_pchg_audt_count==0:
        dbutils.notebook.exit("success")
except Exception as e:   
     raise (e)

# COMMAND ----------

from pyspark.sql.functions import concat_ws
from pyspark.sql.functions import current_timestamp
selected_columns = df_pchg_audtt[[]
    'ROG', 'PRICE_AREA', 'PRICE_GROUP_ID', 'NEW_PRICE', 'NEW_PRICE_EFFECTIVE_DATE',
    'NEXT_PRICE', 'NEXT_PRICE_EFFECTIVE_DATE',
    'CURRENT_PRICE', 'CALCULATED_PRICE', 'CURRENT_COST', 'CURRENT_COST_EFFECTIVE_DATE',
    'FUTURE_COST', 'FUTURE_COST_EFFECTIVE_DATE',
    'MULTI_FACTOR_PRICE', 'MULTI_FACTOR_QUANTITY',
    'WORKFLOW_STATE', 'WORKFLOW_TIME_STAMP', 'EXPORT_TIME_STAMP', 'WORKFLOW_TRANSITION_COMMENTS'
]]


spark_df = selected_columns.withColumn("priceAreaId", concat_ws("-", selected_columns.ROG, selected_columns.PRICE_AREA))
spark_df = spark_df.withColumn("createdTimestamp", current_timestamp())


# COMMAND ----------

# #function for creating json
def generateauditEventsJson(df_data):
    try:
        # Creating _id field with rog and priceGroupId
        df_id_object = (df_data.withColumn('_id',
                                           F.struct(F.col('priceAreaId').alias('priceAreaId'),
                                                    F.col('PRICE_GROUP_ID').alias('priceGroupId'),
                                                    F.col('EXPORT_TIME_STAMP').alias('exportTimeStamp'),

                                                    )))
        
        df_auditEvents = (df_data
            .withColumn('auditEvents',
                F.struct(
                    F.col('NEW_PRICE').alias('newPrice'),
                    F.col('NEW_PRICE_EFFECTIVE_DATE').alias('newPriceEffectiveDate'),
                    F.col('MULTI_FACTOR_PRICE').alias('multiFactorPrice'),
                    F.col('MULTI_FACTOR_QUANTITY').alias('multiFactorQuantity'),
                    F.col('WORKFLOW_TIME_STAMP').alias('workflowTs'),
                    F.col('WORKFLOW_STATE').alias('workflowState'),
                    F.col('WORKFLOW_TRANSITION_COMMENTS').alias('workflowTransitionComments')
                )
            )
        )
        # Grouping audit details by priceAreaId , PRICE_GROUP_ID and EXPORT_TIME_STAMP
        df_auditEvents_item_grouped = (df_auditEvents.groupBy('priceAreaId', 'PRICE_GROUP_ID','EXPORT_TIME_STAMP')
                                 .agg(F.collect_list('auditEvents').alias('auditEvents')))
        # Joining all parts together
        df_result = (df_id_object
            .join(df_auditEvents_item_grouped, ['priceAreaId', 'PRICE_GROUP_ID', 'EXPORT_TIME_STAMP'])
            .select(
                '_id',
                F.col('CURRENT_PRICE').alias('currentPrice'),
                F.col('CALCULATED_PRICE').alias('calculatedPrice'),
                F.col('CURRENT_COST').alias('currentCost'),
                F.col('CURRENT_COST_EFFECTIVE_DATE').alias('currentCostEffectiveDate'),
                F.col('FUTURE_COST').alias('futureCost'),
                F.col('FUTURE_COST_EFFECTIVE_DATE').alias('futureCostEffectiveDate'),
                F.col('MULTI_FACTOR_PRICE').alias('multiFactorPrice'),
                F.col('MULTI_FACTOR_QUANTITY').alias('multiFactorQuantity'),
                'auditEvents',
                F.col('createdTimestamp').alias('createdTimestamp')
            )
        )
        return df_result
    except Exception as e:
        raise e

# COMMAND ----------

df_audit_data_json = generateauditEventsJson(spark_df)
            # Write to MongoDB collection
mongodb_Write_with_df_append(varCollection, df_audit_data_json)

# COMMAND ----------

query="""update {0}.stg_file_check set uploaded_time={1} where file_name='pchg_audit'"""
getDeltaTable(None,query.format(delta_schema,latest_pchg_audt))

# COMMAND ----------

from azure.storage.fileshare import ShareDirectoryClient
import os
import shutil
from datetime import datetime

def keep_latest_files(dir_client, num_files=30):
    # Get list of files in the directory
    files = [file.name for file in dir_client.list_directories_and_files() if not file.is_directory]
    
    # Sort files by modification time
    files.sort(key=lambda x: dir_client.get_file_client(x).get_file_properties().last_modified, reverse=True)
    
    # Keep only the latest num_files
    files_to_keep = files[:num_files]
    files_to_delete = files[num_files:]
    
    # Delete files that are not in the keep list
    for file in files_to_delete:
        dir_client.delete_file(file)
        

dir_path=pchg_audit_me01r_fileshare_archive
dir_client = ShareDirectoryClient.from_connection_string(conn_str, share_name, dir_path)
keep_latest_files(dir_client)

# COMMAND ----------



# AI Suggestion for bug: Two columns ‘NEXT_PRICE’ and 'NEXT_PRICE_EFFECTIVE_DATE' were missing in the extract for price change audit. Update extract to include those
# (No AI suggestion available)
