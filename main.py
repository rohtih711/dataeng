from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('appname').getOrCreate()

print('ok')
print(spark)
