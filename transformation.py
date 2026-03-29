from pyspark.sql import SparkSession

spark = SparkSession.builder.config('spark.ui.port','0').config('spark.shuffle.useOldFetchProtocol','true').enableHiveSupport().master('yarn').appName(' ').getOrCreate()


from pyspark.sql.types import *

order_schema_stuc  = StructType([
    StructField("order_id",LongType()),
    StructField("order_date",DateType()),
    StructField("cust_id",IntegerType()),
    StructField("order_status",StringType())
])

orders_df = spark.read \
.format('csv') \
.schema(order_schema_stuc) \
.load("/public/trendytech/orders/orders_1gb.csv")

print(orders_df.rdd.getNumPartitions())