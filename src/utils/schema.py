# Created by vidit.singh at 03-07-2022

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


def orders_schema():
    return StructType([
        StructField('order_id', IntegerType(), True),
        StructField('order_date', StringType(), True),
        StructField('order_customer_id', IntegerType(), True),
        StructField('order_status', StringType(), True)
    ])


def order_items_schema():
    return StructType([
        StructField('order_item_id', IntegerType(), True),
        StructField('order_item_order_id', IntegerType(), True),
        StructField('order_item_product_id', IntegerType(), True),
        StructField('order_item_quantity', IntegerType(), True),
        StructField('order_item_subtotal', FloatType(), True),
        StructField('order_item_product_price', FloatType(), True)
    ])


custom_schema = {'orders': orders_schema(),
                 'order_items': order_items_schema()}
