# Created by vidit.singh at 28-06-2022

from src.utils.configurations.sparkConfig import SparkConfiguration
from src.utils import Paths

sc = SparkConfiguration.get_spark_context('Movie average review')
movies_rdd = sc.textFile(Paths.base_dir() + 'movies_review.csv')
movie_tuple_rdd = movies_rdd.map(lambda x: (x.split(',')[0], (int(x.split(',')[1]), 1)))  # (movie,(4,1))

# (movie,(rating1,count1))    (movie,(rating2,count2))  => (movie,(rating1+rating2,count1+count2)... so on
movie_total_and_count_rdd = movie_tuple_rdd.reduceByKey(lambda m1, m2: (m1[0] + m2[0], m1[1] + m2[1]))

# (movie,(Total_rating,total_count))
movie_avg = movie_total_and_count_rdd.map(lambda movie: (movie[0], round(movie[1][0] / movie[1][1],2))).collect()
print(movie_avg)
