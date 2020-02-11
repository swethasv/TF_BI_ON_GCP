from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, monotonically_increasing_id
from pyspark.sql import Window
from pyspark.sql.types import datetime
import ConfigParser
import sys
import os

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
sourceTablelist = ["PSXLATITEM"]

#This is prod
#Get the target table filename from the python filename
#targetFileName =  TARGETFILEPATH+os.path.basename(sys.argv[0])[:-int(CHARS_TO_TRIM)]

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
  SELECT 
FIELDNAME||'~'||ACADEMIC_LEVEL_CODE AS UNIFICATION_ID,
ACADEMIC_LEVEL_CODE,
EFFDT,
EFF_STATUS,
ACADEMIC_LEVEL,
ACADEMIC_LEVEL_DESCR
FROM(
SELECT 
COALESCE(RTRIM(LTRIM(XLATITEM.FIELDNAME)),'-') AS FIELDNAME,
COALESCE(RTRIM(LTRIM(XLATITEM.FIELDVALUE)),'-') AS ACADEMIC_LEVEL_CODE,
COALESCE(TO_DATE(XLATITEM.EFFDT,'dd/mm/yyyy'),'1900-01-01') AS EFFDT,
COALESCE(RTRIM(LTRIM(XLATITEM.EFF_STATUS)),'-') AS EFF_STATUS,
COALESCE(RTRIM(LTRIM(XLATITEM.XLATLONGNAME)),'-') AS ACADEMIC_LEVEL_DESCR,
COALESCE(RTRIM(LTRIM(XLATITEM.XLATSHORTNAME)),'-') AS ACADEMIC_LEVEL
 FROM PSXLATITEM XLATITEM 
 WHERE XLATITEM.FIELDNAME='ACADEMIC_LEVEL'
 ) 
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
    elif isinstance(colList, datetime):
        colVal = '1900-01-01' 
    list.append(colVal)
unspecifiedRow = spark.createDataFrame([(list)])
df_final = df_staging.union(unspecifiedRow)

#Export the transformed data back to Storage bucket
df_final.write.option("header", "true").mode('overwrite').csv(targetFileName)
if __name__=="__main__":
	main()