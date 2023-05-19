# Databricks notebook source
## Importing required libraries
import pyspark
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, concat_ws, when
from datetime import datetime
import boto3

# COMMAND ----------

## Creating a spark session
spark = SparkSession.builder.config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.7").getOrCreate()

# COMMAND ----------

## Loading aws access keys in a dataframe
aws_keys_df = spark.read.format('csv').option('header','true').option('inferSchema','true').load('/FileStore/tables/TejasviMedi_accessKeys.csv')

ACCESS_KEY = aws_keys_df.select('Access key ID').take(1)[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').take(1)[0]['Secret access key']

# COMMAND ----------

## Setting up configuration to make connection with S3
bucket_name='aws-patient-health-analytics'
file_path = "s3a://{}/input/exercise-data.xlsx".format(bucket_name)

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", ACCESS_KEY)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", SECRET_KEY)

# COMMAND ----------

# import urllib
# ENCODED_SECRET_KEY = urllib.parse.quote(string = SECRET_KEY,safe="")
# AWS_S3_BUCKET = 'aws-patient-health-analytics'
# MOUNT_NAME = '/mnt/mount_s3'
# SOURCE_URL = f's3a://{ACCESS_KEY}:{ENCODED_SECRET_KEY}@{AWS_S3_BUCKET}'

# dbutils.fs.mount(SOURCE_URL,MOUNT_NAME)
# %fs ls '/mnt/mount_s3'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract Data

# COMMAND ----------

## Since data is in an excel sheet, used crealytics library to load the file
## Looped through the file names and created all the required Dataframes
sheet_names = ['encounter_e1','lab_e1','medications_e1','patient_e1']

for sheet_name in sheet_names:
    locals()[sheet_name] = spark.read.format("com.crealytics.spark.excel")\
    .option("dataAddress", sheet_name+"!").option("header", "true")\
    .option("inferSchema", "true").load(file_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up the tables
# MAGIC ##### Removing Duplicate rows
# MAGIC ##### stripping columns we do not need

# COMMAND ----------

medications_e1.count()

# COMMAND ----------

medications_e1 = medications_e1.dropDuplicates()
medications = medications_e1.select(['encounterid','patientid','medication_simple_generic_name','minimum_dose','dose_unit'])

# COMMAND ----------

medications_e1.count()

# COMMAND ----------

medications = medications.na.drop(subset = ['minimum_dose'])
medications.count()

# COMMAND ----------

encounter = encounter_e1.select(['patientid','encounterid','admit_diagnosis'])
patient = patient_e1.select(['patientid','Sex','Age','primary_care_provider'])

# COMMAND ----------

patient = patient.withColumn("Age", when(col("Age") > 100, None).otherwise(col("Age")))

# COMMAND ----------

average_age = patient.select(avg("Age")).first()[0]
patient = patient.fillna({'Age':average_age})

# COMMAND ----------

# patient = patient.withColumn("Age", patient["Age"].cast("int"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join the Medications and Encounter tables

# COMMAND ----------

med_enc = medications.join(encounter, on=['encounterid','patientid'],how='inner')

med_enc.printSchema()

# COMMAND ----------

med_enc.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Perform the grouping operation

# COMMAND ----------

med_enc_grouped = med_enc.groupBy('patientid','medication_simple_generic_name','dose_unit','admit_diagnosis').agg(avg("minimum_dose").alias("avg_minimum_dose"))

# COMMAND ----------

patient_meds = patient.join(med_enc_grouped, on='patientid', how="inner")
patient_meds.printSchema()
patient_meds.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Converting all the columns to String

# COMMAND ----------

## Converting patientid and primary_care_provider to Decimal so it doesn't change to scientific numbers.
patient_meds = patient_meds.withColumn("patientid", col('patientid').cast("Decimal(14,0)")).withColumn("primary_care_provider", col('primary_care_provider').cast("Decimal(14,0)"))

## casting all columns to string
for col_name, col_type in patient_meds.dtypes:
    if col_type != "string":
        patient_meds = patient_meds.withColumn(col_name, col(col_name).cast("string"))
        
patient_meds.printSchema()
patient_meds.show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Storing Dataframe into a text file on S3

# COMMAND ----------

date_str = datetime.now().strftime("%Y%m%d")
output_file = f"tmp/target_1"
# local_output = "/dbfs/FileStore/tables/result"
final_output = f"output/target_1_{date_str}.txt"

# COMMAND ----------

# file_key = 'path/to/your/target_1_YYYYMMDD.txt'
field_delimiter = '|'
text_qualifier = '"'

patient_meds.coalesce(1).write.mode("overwrite").option("header", True)\
  .option("delimiter", field_delimiter)\
  .option("quote", text_qualifier)\
  .option("encoding", "UTF-8").option("wholetext","true").csv(f"s3a://{bucket_name}/{output_file}")

# COMMAND ----------

s3 = boto3.client('s3',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY)

response = s3.list_objects_v2(Bucket=bucket_name,Prefix='tmp/')

objects = [object['Key'] for object in response['Contents'] if object['Key'].endswith('.csv')]

# COMMAND ----------

objects[0]

# COMMAND ----------

result = s3.copy_object(Bucket=bucket_name,Key=final_output,CopySource={'Bucket':bucket_name,'Key':objects[0]})

# COMMAND ----------

objects = [s3.delete_object(Bucket=bucket_name,Key=object['Key']) for object in response['Contents']]

# COMMAND ----------

# delimiter = "|"
# text_qualifier = '"'
# header = text_qualifier + delimiter.join(patient_meds.columns) + text_qualifier
# header

# COMMAND ----------

# df_string = patient_meds.select(concat_ws(delimiter, *[col(c).cast("string") for c in patient_meds.columns]).alias(header))
# df_string.coalesce(1).write.text(output_file)

# file_path = [file.path for file in dbutils.fs.ls(output_file+"/") if file.name.endswith(".csv")][0]

# dbutils.fs.cp(file_path, final_output)
# dbutils.fs.rm(output_file, recurse=True)

# COMMAND ----------

spark.stop()
