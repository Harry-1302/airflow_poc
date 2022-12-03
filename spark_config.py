from pyspark.sql.functions import sha2, concat_ws, split, regexp_replace, col
from pyspark.sql import SQLContext
from pyspark.sql.types import *
sqlContext = SQLContext(sc)

schema = StructType([StructField("advertising_id",StringType(),True), StructField("city",StringType(),True), StructField("location_category",StringType(),True), StructField("location_granularities",StringType(),True), StructField("location_source",StringType(),True), StructField("state",StringType(),True), StructField("timestamp",StringType(),True), StructField("user_id",StringType(),True), StructField("user_latitude",DoubleType(),True), StructField("user_longitude",DoubleType(),True), StructField("month",StringType(),True), StructField("date",StringType(),True)])

df = sqlContext.read.load('/Users/hariomsharma/PycharmProjects/nabeel_prjct/dataset.csv', format='com.databricks.spark.csv', header='true', inferSchema='true')
df2 = df.withColumn("location_source", split(regexp_replace(col("location_source"), '[\\[\\]]', ""), ","))
df3 = df2.withColumn("location_source", concat_ws(",",col("location_source")))

df3.createOrReplaceTempView('df_base')


df_new = spark.sql("select sha2(concat(advertising_id,user_id),256) as unique_id, city, location_category, location_granularities, location_source, state, timestamp, user_id, cast(user_latitude AS DECIMAL(10,7)), cast(user_longitude AS DECIMAL(10,7)), month, date from df_base")

