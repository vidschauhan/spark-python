# Created by vidit.singh at 25-08-2022
import findspark
from pyspark.sql import SparkSession, Row

findspark.init()
bad_movies_list = [Row(None, None, None),
                   Row(None, None, 2020),
                   Row("John Doe", "Awesome Movie", None),
                   Row(None, "Awesome Movie", 2021),
                   Row("Mary Jane", None, 2019),
                   Row("Vikter Duplaix", "Not another teen movie", 2001)]

spark = SparkSession.builder.appName('Drop missing or bad records').getOrCreate()
movies_df = spark.createDataFrame(bad_movies_list, ['actor_name', 'movie_title', 'produced_year'])
movies_df.show()

movies_df.fillna('NA').show()
# dropping records with null values.
movies_df.na.drop('any').show()
movies_df.na.drop('all').show()
movies_df.na.drop().show()
