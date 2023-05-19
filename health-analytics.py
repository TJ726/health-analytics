# Databricks notebook source
## Importing required libraries
import pyspark
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, concat_ws
from datetime import datetime
import boto3


## Creating a spark session
spark = SparkSession.builder.config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.7").getOrCreate()


## Loading aws access keys in a dataframe
aws_keys_df = spark.read.format('csv').option('header','true').option('inferSchema','true').load('/FileStore/tables/TejasviMedi_accessKeys.csv')

ACCESS_KEY = aws_keys_df.select('Access key ID').take(1)[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').take(1)[0]['Secret access key']



## Setting up configuration to make connection with S3
bucket_name='aws-patient-health-analytics'
file_path = "s3a://{}/input/exercise-data.xlsx".format(bucket_name)

spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", ACCESS_KEY)
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", SECRET_KEY)



# import urllib

# ENCODED_SECRET_KEY = urllib.parse.quote(string = SECRET_KEY,safe="")

# AWS_S3_BUCKET = 'aws-patient-health-analytics'
# MOUNT_NAME = '/mnt/mount_s3'

# SOURCE_URL = f's3a://{ACCESS_KEY}:{ENCODED_SECRET_KEY}@{AWS_S3_BUCKET}'



# dbutils.fs.mount(SOURCE_URL,MOUNT_NAME)

# %fs ls '/mnt/mount_s3'



# MAGIC %md
# MAGIC ## Extract Data



## Since data is in an excel sheet, used crealytics library to load the file
## Looped through the file names and created all the required Dataframes
sheet_names = ['encounter_e1','lab_e1','medications_e1','patient_e1']

for sheet_name in sheet_names:
    locals()[sheet_name] = spark.read.format("com.crealytics.spark.excel")\
    .option("dataAddress", sheet_name+"!").option("header", "true")\
    .option("inferSchema", "true").load(file_path)



# MAGIC %md
# MAGIC ## Clean up the tables
# MAGIC ##### Removing Duplicate rows
# MAGIC ##### stripping columns we do not need



medications_e1.count()

medications_e1 = medications_e1.dropDuplicates()
medications = medications_e1.select(['encounterid','patientid','medication_simple_generic_name','minimum_dose','dose_unit'])

medications_e1.count()

medications = medications.na.drop(subset = ['minimum_dose'])
medications.count()

encounter = encounter_e1.select(['patientid','encounterid','admit_diagnosis'])
patient = patient_e1.select(['patientid','Sex','Age','primary_care_provider'])



# MAGIC %md
# MAGIC ## Join the Medications and Encounter tables



med_enc = medications.join(encounter, on=['encounterid','patientid'],how='inner')

med_enc.printSchema()
med_enc.show(5)



med_enc.count()



# MAGIC %md
# MAGIC ## Perform the grouping operation



med_enc_grouped = med_enc.groupBy('patientid','medication_simple_generic_name','dose_unit','admit_diagnosis').agg(avg("minimum_dose").alias("avg_minimum_dose"))



med_enc_grouped.show(10)



patient_meds = patient.join(med_enc_grouped, on='patientid', how="inner")

patient_meds.printSchema()



# MAGIC %md
# MAGIC ## Converting all the columns to String



patient_meds = patient_meds.withColumn("patientid", col('patientid').cast("Decimal(14,0)")).withColumn("primary_care_provider", col('primary_care_provider').cast("Decimal(14,0)"))

for col_name, col_type in patient_meds.dtypes:
    if col_type != "string":
        patient_meds = patient_meds.withColumn(col_name, col(col_name).cast("string"))
        
patient_meds.printSchema()
patient_meds.show(5)




# MAGIC %md
# MAGIC ## Changing the dataframe to a single string column



date_str = datetime.now().strftime("%Y%m%d")
output_file = f"tmp/target_1"
# local_output = "/dbfs/FileStore/tables/result"
final_output = f"output/target_1_{date_str}.txt"



# file_key = 'path/to/your/target_1_YYYYMMDD.txt'
field_delimiter = '|'
text_qualifier = '"'

patient_meds.coalesce(1).write.mode("overwrite").option("header", True)\
  .option("delimiter", field_delimiter)\
  .option("quote", text_qualifier)\
  .option("encoding", "UTF-8").option("wholetext","true").csv(f"s3a://{bucket_name}/{output_file}")

#   .csv(f"s3a://{bucket_name}/{output_file}")



s3 = boto3.client('s3',aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY)

response = s3.list_objects_v2(Bucket=bucket_name,Prefix='tmp/')

objects = [object['Key'] for object in response['Contents'] if object['Key'].endswith('.csv')]



objects[0]



result = s3.copy_object(Bucket=bucket_name,Key=final_output,CopySource={'Bucket':bucket_name,'Key':objects[0]})



objects = [s3.delete_object(Bucket=bucket_name,Key=object['Key']) for object in response['Contents']]



# delimiter = "|"
# text_qualifier = '"'
# header = text_qualifier + delimiter.join(patient_meds.columns) + text_qualifier
# header



# df_string = patient_meds.select(concat_ws(delimiter, *[col(c).cast("string") for c in patient_meds.columns]).alias(header))
# df_string.coalesce(1).write.text(output_file)

# file_path = [file.path for file in dbutils.fs.ls(output_file+"/") if file.name.endswith(".csv")][0]

# dbutils.fs.cp(file_path, final_output)
# dbutils.fs.rm(output_file, recurse=True)



spark.stop()
