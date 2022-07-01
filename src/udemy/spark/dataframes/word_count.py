# Created by vidit.singh at 30-06-2022
from pyspark.sql import SparkSession
from src.utils import Paths

spark = SparkSession.builder.appName('Word count').getOrCreate()
words_df = spark.read.text(Paths.base_dir() + 'sample-text-file.txt')
words_df.groupBy('value').count().show(10,truncate=False)
