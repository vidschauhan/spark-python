# Created by vidit.singh at 25-08-2022
import findspark
from pyspark.sql import SparkSession, Row


findspark.init()
persons_row_list = [
    Row(1, 'Drucy', 'Poppy', ['I giorni contati'], 1463.36, 'http://dummyimage.com/126x166.png/cc0000/ffffff',
        '1991-02-16', True),
    Row(2, 'Loki', 'Pata', ['I giorni contati'], 1463.36, 'http://dummyimage.com/126x166.png/cc0000/ffffff',
        '1991-02-17', True),
    Row(3, 'Pudy', 'Lats', ['I giorni contati'], 1463.36, 'http://dummyimage.com/126x166.png/cc0000/ffffff',
        '1991-02-18', True)]

spark = SparkSession.builder.appName('Rows to DF').getOrCreate()
persons_df = spark.createDataFrame(persons_row_list,
                                   ['id', 'first_name', 'last_name', 'fav_movies', 'salary', 'image_url',
                                    'date_of_birth', 'active'])
persons_df.show(5, truncate=False)
