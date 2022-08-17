# Created by vidit.singh at 05-07-2022
from pyspark.sql import SparkSession
from src.utils.schema import custom_schema
from src.utils import Paths
from pyspark.sql.functions import col, when, expr,sum

spark = SparkSession.builder.appName('Creating Dataframes').getOrCreate()
orders_df = spark.read.schema(custom_schema['orders']).csv(Paths.base_dir() + '\\retail_db\\orders\\part-00000',
                                                           header=False)
order_items_df = spark.read.schema(custom_schema['order_items']).csv(Paths.base_dir() + '\\retail_db\\order_items'
                                                                                        '\\part-00000', header=False)

customers_df = spark.read.json(Paths.get_json_path('retail_db_json\\customers'))
orders_join = orders_df.join(order_items_df, on=orders_df['order_id'] == order_items_df['order_item_order_id'],
                             how='inner')

customers_df.persist()
orders_join.persist()
orders_df.persist()
orders_join.printSchema()

customers_orders_left = customers_df.join(orders_df, on=customers_df['customer_id'] == orders_df['order_customer_id'],
                                          how='left')
# customers_orders_left.show()

customers_orders_left.select(customers_df.customer_id, customers_df.customer_fname, orders_df['*']).distinct() \
    .show(10, truncate=False)

# Those customers who haven't placed any orders.
customers_orders_left.filter(orders_df.order_id.isNull()) \
    .select(customers_df.customer_id, customers_df.customer_fname, orders_df['*']).distinct() \
    .show(10, truncate=False)

customers_orders_left.printSchema()

orders_2013 = orders_df.filter(col('order_date').like('2013%'))

customer_orders_2013_left = customers_df.join(orders_2013, customers_df.customer_id == orders_2013.order_customer_id,
                                              'left')

cust = customer_orders_2013_left.filter(orders_2013['order_customer_id'].isNull()).count()
print(cust)

customer_orders_2013_left.groupBy('customer_id', 'customer_email') \
    .agg(sum(expr('CASE WHEN order_id IS NULL THEN 0 ELSE 1 END')).alias('order_count'))\
    .orderBy('order_count','customer_id').show()
# get count of all customer who haven't placed any order in 2013
