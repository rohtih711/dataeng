from pyspark.sql import SparkSession
import getpass
username = getpass.getuser()

spark = SparkSession. \
    builder. \
    config('spark.ui.port','0'). \
    config("spark.sql.warehouse.dir", '/Users/{username}/PycharmProjects/dataeng'). \
    enableHiveSupport().  \
    getOrCreate()


#SparkSession acts an entry point to the spark cluster.
#spark context
#hive context
#sql context


#RDD - Spark context
#DataFrame - sparksession
#Spark SQL - sparksession

print(spark)


rdd1 = spark.sparkContext.textFile('/Users/rohithb/PycharmProjects/dataeng/word.txt')

rdd2 = rdd1.flatMap(lambda x: x.split( ))

rdd3 = rdd2.map(lambda  x : (x,1))

rdd4 = rdd3.reduceByKey(lambda x,y: x+y)

print(rdd4.collect())
