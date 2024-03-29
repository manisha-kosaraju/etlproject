# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
import pyspark.sql.functions as f

# COMMAND ----------

#extraction
spark=SparkSession.builder.appName("etlpipeline").getOrCreate()
df=spark.read.text("/FileStore/tables/WordData2.txt")
df.show()

# COMMAND ----------

#transformation
df2=df.withColumn("splitteddata",f.split("value"," "))
df3=df2.withColumn("words",f.explode("splitteddata"))
wordscount=df3.groupby("words").count()

# COMMAND ----------

#load
driver= "org.postgresql.Driver"
url="jdbc:postgresql://database-1.c7aqcemk4bk2.us-east-2.rds.amazonaws.com/"
table= "awsrds_schema_pyspark.wordcount"
username="postgres"
password="Abundance"

wordscount.write.format("jdbc").option("driver",driver).option("url",url).option("dbtable",table).option("mode","append").option("user",username).option("password",password).save()

# COMMAND ----------


