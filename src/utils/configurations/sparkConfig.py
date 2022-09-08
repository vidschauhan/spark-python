# Created by vidit.singh at 28-06-2022
from pyspark import SparkConf, SparkContext
import findspark
from pyspark.sql import SparkSession


class SparkConfiguration:
    @staticmethod
    def get_spark_context(app_name):
        findspark.init()
        conf = SparkConf().setAppName(app_name)
        return SparkContext.getOrCreate(conf=conf)

    @staticmethod
    def get_spark_session(app_name):
        findspark.init()
        return SparkSession.builder.master('local[*]').appName(app_name).getOrCreate()
