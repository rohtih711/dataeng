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


word_rdd = spark.sparkContext.parallelize(word)

print(word_rdd.getNumPartitions())

print(spark.sparkContext.defaultParallelism)

print(spark.sparkContext.defaultMinPartitions)


