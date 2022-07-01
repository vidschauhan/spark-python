# Created by vidit.singh at 28-06-2022
from pyspark import SparkConf, SparkContext
import findspark


class SparkConfiguration:
    @staticmethod
    def get_spark_context(app_name):
        findspark.init()
        conf = SparkConf().setAppName(app_name)
        return SparkContext.getOrCreate(conf=conf)
