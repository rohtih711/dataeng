from pyspark.sql import SparkSession
import getpass
username = getpass.getuser()

spark = SparkSession. \
    builder. \
    config('spark.ui.port','0'). \
    config("spark.sql.warehouse.dir", '/Users/{username}/PycharmProjects/dataeng'). \
    enableHiveSupport().  \
    getOrCreate()



word = ("big","Data","is","super","interesting","Big","data","is","A","Trending","techology")

rrd1 = spark.sparkContext.parallelize(word)

rrd2 = rrd1.map(lambda x: x.upper())

print(rrd2.collect())