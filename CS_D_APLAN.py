from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.sql import Window
from pyspark.sql.types import datetime
import ConfigParser
import sys
import os
import pandas as pd
from pyspark.sql import Row
from functools import partial

#Create a Spark session
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()
#Initiate ConfigParser to read the Configuration file
config = ConfigParser.RawConfigParser()

#Read the Configuration file
configFilePath = r'DEV.cfg'
config.read(configFilePath)
SOURCEFILEPATH = config.get('gs_bucket_path', 'ps_sourceFile')
TARGETFILEPATH = config.get('gs_bucket_path', 'csv_targetFile')
CHARS_TO_TRIM = config.get('column_name', 'NoOfCharsToTrim')
SEQUENCE_ID = config.get('column_name', 'monotonically_inc_id')

#List all the required source tables
sourceTablelist = ["PS_ACAD_PLAN_TBL"]

#Get the target table filename from the python filename
fileName = os.path.basename(sys.argv[0])[:-int(CHARS_TO_TRIM)]
targetFileName =  TARGETFILEPATH+fileName

#Iterate through the source table list and load the data into a Spark dataframe. 
def createTempTables(sparkSess,sourceTablelist):
    for tabList in sourceTablelist:
        df_source = sparkSess.read.option("header", "true").csv(SOURCEFILEPATH+tabList+".csv")
        #Register the dataframe as a temporary table
        df_source.registerTempTable(tabList)
    return df_source
df_source=createTempTables(spark,sourceTablelist)

#Run a Spark SQL over the temporary table
df_staging = spark.sql("""
     SELECT CS_APLAN_D.INSTITUTION||'~'||CS_APLAN_D.ACAD_CAREER||'~'||CS_APLAN_D.ACAD_PROG||'~'||CS_APLAN_D.ACAD_PLAN AS UNIFICATION_ID,
CS_APLAN_D.INSTITUTION,
CS_APLAN_D.ACAD_PLAN,
CS_APLAN_D.EFFDT,
CS_APLAN_D.EFF_STATUS,
CS_APLAN_D.DESCR,
CS_APLAN_D.DESCRSHORT,
CS_APLAN_D.ACAD_PLAN_TYPE,
CS_APLAN_D.ACAD_PROG,
CS_APLAN_D.PLN_REQTRM_DFLT,
CS_APLAN_D.DEGREE,
CS_APLAN_D.DIPLOMA_DESCR,
CS_APLAN_D.DIPLOMA_PRINT_FL,
CS_APLAN_D.DIPLOMA_INDENT,
CS_APLAN_D.TRNSCR_DESCR,
CS_APLAN_D.TRNSCR_PRINT_FL,
CS_APLAN_D.TRNSCR_INDENT,
CS_APLAN_D.FIRST_TERM_VALID,
CS_APLAN_D.CIP_CODE,
CS_APLAN_D.HEGIS_CODE,
CS_APLAN_D.ACAD_CAREER,
CS_APLAN_D.TRANSCRIPT_LEVEL,
CS_APLAN_D.STUDY_FIELD,
CS_APLAN_D.EVALUATE_PLAN
FROM
(

SELECT 
COALESCE(RTRIM(LTRIM(ACAD_PLAN_TBL.INSTITUTION)),'-') AS INSTITUTION,
COALESCE(RTRIM(LTRIM(ACAD_PLAN_TBL.ACAD_PLAN)),'-') AS ACAD_PLAN,
COALESCE(TO_DATE(ACAD_PLAN_TBL.EFFDT,'dd/mm/yyyy'),'1900-01-01') AS EFFDT,
COALESCE(RTRIM(LTRIM(ACAD_PLAN_TBL.EFF_STATUS)),'-') AS EFF_STATUS,
COALESCE(RTRIM(LTRIM(ACAD_PLAN_TBL.DESCR)),'-') AS DESCR,
COALESCE(RTRIM(LTRIM(ACAD_PLAN_TBL.DESCRSHORT)),'-') AS DESCRSHORT,
COALESCE(RTRIM(LTRIM(ACAD_PLAN_TBL.ACAD_PLAN_TYPE)),'-') AS ACAD_PLAN_TYPE,
COALESCE(RTRIM(LTRIM(ACAD_PLAN_TBL.ACAD_PROG)),'-') AS ACAD_PROG,
COALESCE(RTRIM(LTRIM(ACAD_PLAN_TBL.PLN_REQTRM_DFLT)),'-') AS PLN_REQTRM_DFLT,
COALESCE(RTRIM(LTRIM(ACAD_PLAN_TBL.DEGREE)),'-') AS DEGREE,
COALESCE(RTRIM(LTRIM(ACAD_PLAN_TBL.DIPLOMA_DESCR)),'-') AS DIPLOMA_DESCR,
COALESCE(RTRIM(LTRIM(ACAD_PLAN_TBL.DIPLOMA_PRINT_FL)),'-') AS DIPLOMA_PRINT_FL,
COALESCE(RTRIM(LTRIM(ACAD_PLAN_TBL.DIPLOMA_INDENT)),'-') AS DIPLOMA_INDENT,
COALESCE(RTRIM(LTRIM(ACAD_PLAN_TBL.TRNSCR_DESCR)),'-') AS TRNSCR_DESCR,
COALESCE(RTRIM(LTRIM(ACAD_PLAN_TBL.TRNSCR_PRINT_FL)),'-') AS TRNSCR_PRINT_FL,
COALESCE(RTRIM(LTRIM(ACAD_PLAN_TBL.TRNSCR_INDENT)),'-') AS TRNSCR_INDENT,
COALESCE(RTRIM(LTRIM(ACAD_PLAN_TBL.FIRST_TERM_VALID)),'-') AS FIRST_TERM_VALID,
COALESCE(RTRIM(LTRIM(ACAD_PLAN_TBL.CIP_CODE)),'-') AS CIP_CODE,
COALESCE(RTRIM(LTRIM(ACAD_PLAN_TBL.HEGIS_CODE)),'-') AS HEGIS_CODE,
COALESCE(RTRIM(LTRIM(ACAD_PLAN_TBL.ACAD_CAREER)),'-') AS ACAD_CAREER,
COALESCE(RTRIM(LTRIM(ACAD_PLAN_TBL.TRANSCRIPT_LEVEL)),'-') AS TRANSCRIPT_LEVEL,
COALESCE(RTRIM(LTRIM(ACAD_PLAN_TBL.STUDY_FIELD)),'-') AS STUDY_FIELD,
COALESCE(RTRIM(LTRIM(ACAD_PLAN_TBL.EVALUATE_PLAN)),'-') AS EVALUATE_PLAN,
DENSE_RANK() OVER (PARTITION BY ACAD_PLAN_TBL.INSTITUTION,ACAD_PLAN_TBL.ACAD_CAREER,ACAD_PLAN_TBL.ACAD_PROG,ACAD_PLAN_TBL.ACAD_PLAN ORDER BY ACAD_PLAN_TBL.EFFDT DESC ) AS DNS_RNK
FROM 
PS_ACAD_PLAN_TBL ACAD_PLAN_TBL
) CS_APLAN_D
WHERE 
CS_APLAN_D.DNS_RNK=1
""")

#Below function generates the integers monotonically increasing and consecutive in a partition
df_staging = df_staging.withColumn(SEQUENCE_ID,row_number().over(Window.orderBy(monotonically_increasing_id())))

#Add one Unspecified row
#Get the list of columns
colList = df_staging.columns
list = []
dateColList = ["EFFDT"]
#Interate through the column list and 
for colList in colList:
    if isinstance(colList, str):
        if colList in dateColList:
            colVal = '1900-01-01'  
        else:
            colVal = '0'       
    elif isinstance(colList, int):
        colVal = 0       
    list.append(colVal)
unspecifiedRow = spark.createDataFrame([(list)])
df_final = df_staging.union(unspecifiedRow)

#Export the transformed data back to Storage bucket
df_final.write.option("header", "true").mode('overwrite').csv(targetFileName)

colList = df_final.columns
dict = {}
for colList in colList:
    colVal = colList
    if isinstance(colList, str):
        colType = "String"
    elif isinstance(colList, int):
        colType = "Integer"
    dict[colVal] = colType

df=spark.createDataFrame([dict,])
def flatten_table(column_names, column_values):
    row = zip(column_names, column_values)

    return [
        Row(ColumnName=column, ColumnValue=value, TableName=fileName)
        for column, value in row]

df_schema = df.rdd.flatMap(partial(flatten_table, df.columns)).toDF()
df_schema.show()
df_schema.write.option("header", "false").mode('overwrite').csv("gs://tf-sjsu-dev/ProcessedSchemaFiles/"+fileName)