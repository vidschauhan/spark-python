# Created by vidit.singh at 28-06-2022
from src.utils.configurations.sparkConfig import SparkConfiguration
from src.utils import Paths

sc = SparkConfiguration.get_spark_context('Movie min/max review')
movies_rdd = sc.textFile(Paths.base_dir() + 'movies_review.csv')
movie_tuple_rdd = movies_rdd.map(lambda x: (x.split(',')[0], int(x.split(',')[1])))
min_movie_rating = movie_tuple_rdd.groupByKey().map(lambda movie: (movie[0], min(movie[1]))).collect()
max_movie_rating = movie_tuple_rdd.groupByKey().map(lambda movie: (movie[0], max(movie[1]))).collect()
print(min_movie_rating)
print(max_movie_rating)
