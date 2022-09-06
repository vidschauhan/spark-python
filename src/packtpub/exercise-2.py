# Created by vidit.singh at 25-08-2022
import findspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, FloatType, TimestampType
from pyspark.sql.functions import udf, col, to_date, to_timestamp, year, month

findspark.init()
sales_data_path = 'D:\\Data Engineering\\PacktPub - Apache Spark 3 for Data Engineering and Analytics with ' \
                  'Python\\resources\\Section 5 Resources\\salesdata\\Sales_April_2019.csv'
spark = SparkSession.builder.appName('Exercise - 2').getOrCreate()
schema = StructType([StructField('Order ID', StringType(), True),
                     StructField('Product', StringType(), True),
                     StructField('Quantity Ordered', StringType(), True),
                     StructField('Price Each', StringType(), True),
                     StructField('Order Date', StringType(), True),
                     StructField('Purchase Address', StringType(), True)
                     ])
# spark.read.format('csv').schema(schema).option("header", True).load(sales_data_path).show(10, truncate=False)
sales_df = spark.read.csv(sales_data_path, schema=schema, header=True)
sales_df.show(10, truncate=False)

sales_df.printSchema()


# Cleaning data -> Extract city and state from the sales DF

def extract_city_and_state(address):
    if address is None:
        return None
    add = address.split(',')
    city = add[1]
    state = add[2].split(' ')[1]
    return [city, state]


get_city_or_state = udf(lambda addr: extract_city_and_state(addr), ArrayType(StringType()))

sales_updated_df = sales_df.filter(sales_df['Order ID'].isNotNull()) \
    .withColumn('City', get_city_or_state(sales_df['Purchase Address'])[0]) \
    .withColumn('State', get_city_or_state(sales_df['Purchase Address'])[1])

sales_df_extra = sales_updated_df.withColumn('OrderID', col('Order ID').cast(IntegerType())) \
    .withColumn('Quantity', col('Quantity Ordered').cast(IntegerType())) \
    .withColumn('Price', col('Price Each').cast(FloatType())) \
    .withColumn('OrderDate', to_timestamp(col('Order Date'), "MM/dd/yy HH:mm").cast(TimestampType())) \
    .withColumnRenamed('Purchase Address', 'StoreAddress') \
    .withColumn('ReportYear', year('OrderDate')) \
    .withColumn('Month', month('OrderDate')) \
    .drop('Order ID') \
    .drop('Quantity Ordered') \
    .drop('Price Each') \
    .drop('Order Date')
sales_df_extra.show(10, truncate=False)
sales_df_extra.printSchema()
