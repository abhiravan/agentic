# Databricks notebook source
# MAGIC  %run "../General/nt_user_defined_methods"

# COMMAND ----------

# MAGIC  %run "./nt_msp_priceArea_query"

# COMMAND ----------

import json

# COMMAND ----------

varCollectionName = "priceArea"
varNtName = "nt_msp_priceArea_load"

# COMMAND ----------

# Get the RunId and JobID of the scheduled run.
try:
    jsonContext = (
        dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
    )
    context = json.loads(jsonContext)
    run_id_obj = context.get("currentRunId", {})
    varRunId = run_id_obj.get("id", None) if run_id_obj else -1
    varJobId = context.get("tags", {}).get("jobId", None)
    if varJobId == None:
        varJobId = -1
except Exception as e:
    raise (e)

# COMMAND ----------

def generatePriceAreaJson(df_priceAreaData):
    try:
        # Creating _id field
        df_id_object = (df_priceAreaData.withColumn('_id',
                                                    F.struct(F.col('ROG_ID').alias('rog'),
                                                             F.col('aciFacilityId'),
                                                             F.col('priceAreaId'))))
    
        # select required attribute 
        df_result = (df_id_object.groupBy('_id',
                                         F.col('BANNER_NM').alias('aciBannerName'),
                                         'priceAreaName',
                                         F.col('FACILITY_NM').alias('facilityName'),
                                         F.col('PRICE_AREA_CD').alias('priceArea'),
                                         F.col('DIVISION_ID').alias('division'),
                                         F.col('ADDRESS_LINE1_TXT').alias('addressLine1'),
                                         F.col('ADDRESS_LINE2_TXT').alias('addressLine2'),
                                         F.col('CITY_NM').alias('city'),
                                         F.col('STATE_NM').alias('state'),
                                         F.col('COUNTRY_NM').alias('country'),
                                         F.col('LONGITUDE_DGR').alias('geoLongitude'),
                                         F.col('LATITUDE_DGR').alias('geoLatitude'),
                                         F.col('POSTAL_ZONE_CD').alias('postalCode'),
                                         F.col('CREATEDTIMESTAMP').alias('createdTimestamp'),
                                         F.col('CREATEDBY').alias('createdBy'),
                                         F.col('UPDATEDTIMESTAMP').alias('updatedTimestamp'),
                                         F.col('UPDATEDBY').alias('updatedBy'))
                     .agg(F.collect_list(F.col('SECTION_CD').alias('retailSection')).alias('retailSection'))
                     .drop_duplicates())
 
        return df_result
    except Exception as e:
        raise e

# COMMAND ----------

try:
    #extract data from gcp
    source_df = getBqTable(extract_priceArea_sql.format(bq_project_id))
    df_priceArea_json = generatePriceAreaJson(source_df)
    mongodb_Write_with_df(varCollectionName, df_priceArea_json)                                  
except Exception as e:
    udfInsertLogDetails("E", str(e))
    raise e
