from pyspark.sql import SparkSession
import getpass
username = getpass.getuser()

spark = SparkSession. \
    builder. \
    config('spark.ui.port','0'). \
    config("spark.sql.warehouse.dir", '/Users/{username}/PycharmProjects/dataeng'). \
    enableHiveSupport().  \
    getOrCreate()





mylist = [1,4,6,8,9,10,12]
base_rdd = spark.sparkContext.parallelize(mylist)


print(base_rdd.reduce(lambda x,z : (x*z)))

