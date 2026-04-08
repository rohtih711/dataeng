
from pyspark.sql import SparkSession
import getpass
username = getpass.getuser()

spark = SparkSession. \
    builder. \
    config('spark.ui.port','0'). \
    config("spark.sql.warehouse.dir", '/Users/{username}/PycharmProjects/dataeng'). \
    enableHiveSupport().  \
    getOrCreate()



order_rdd = spark.sparkContext.textFile('/Users/rohithb/PycharmProjects/dataeng/order.csv')

order_rdd1 = order_rdd.map(lambda x: (x.split(",")[3],1))

order_rdd2 = order_rdd1.reduceByKey(lambda x,y: x+y)

reduct_sorted = order_rdd2.sortBy(lambda x : x[1],False)

print(reduct_sorted.collect())


print(spark.sparkContext.uiWebUrl)