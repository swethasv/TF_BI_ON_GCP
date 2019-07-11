from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
filePath = "gs://tf-bi-dev1/"
df_source = spark.read.option("header", "true").csv(filePath+"PS_TERM_TBL.csv")
df_source.registerTempTable("SOURCE_TMP")
df_staging = spark.sql("select A.INSTITUTION,A.ACAD_CAREER,A.ACAD_YEAR,A.STRM,A.TERM_BEGIN_DT,A.TERM_END_DT,A.TERM_CATEGORY,"
                       "A.HOLIDAY_SCHEDULE,A.WEEKS_OF_INSTRUCT,A.SIXTY_PCT_DT,TRIM(A.INSTITUTION)||'-'||TRIM(A.ACAD_CAREER)||'-'"
                       "||TRIM(A.STRM) as PRIMARY_ID,CURRENT_DATE() as CHANGED_ON_DT from SOURCE_TMP A")
df_staging.write.option("header", "true").mode('overwrite').csv(filePath+"TF_TERM_STAGE")