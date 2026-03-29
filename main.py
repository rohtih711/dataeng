from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('appname').getOrCreate()

print(spark)
