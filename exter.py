from pyspark.sql import SparkSession
import getpass
username = getpass.getuser()
spark = SparkSession.builder.config('spark.ui.port','0').enableHiveSupport().master('yarn').appName('agg').getOrCreate()


from pyspark.sql.types import *

schema = StructType([
    StructField('user_first_name',StringType(),nullable=False),
    StructField('user_last_name',StringType(),nullable=False),
    StructField('user_id',IntegerType(),nullable=False),
    StructField('user_gender',StringType(),nullable=False),
    StructField('user_email',StringType(),nullable=False),
    StructField('user_phone_numbers',ArrayType(StringType()),nullable=False),
    StructField('user_address', StructType([
        StructField('street',StringType(),nullable=False),
        StructField('city',StringType(),nullable=False),
        StructField('state',StringType(),nullable=False),
        StructField('postal',StringType(),nullable=False)
    ]),nullable=False)

])

user_df = spark.read.format('json').schema(schema).load('/public/sms/users')

user_df.show()

user_df.rdd.getNumPartitions()

user_df.count()


print(user_df)