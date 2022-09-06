# Created by vidit.singh at 23-08-2022
import findspark
from pyspark import SparkConf, SparkContext

findspark.init()
conf = SparkConf().setAppName("exercise")
sc = SparkContext.getOrCreate(conf=conf)

data_2001_list = ['RIN1', 'RIN2', 'RIN3', 'RIN4', 'RIN5', 'RIN6', 'RIN7']
data_2002_list = ['RIN4', 'RIN3', 'RIN7', 'RIN8', 'RIN9']
data_2003_list = ['RIN4', 'RIN8', 'RIN10', 'RIN11', 'RIN12']

all_projects = set(data_2001_list + data_2003_list + data_2002_list)

# Task 1 -> How may research project were initiated in 3 years.

no_of_distinct_projects = sc.parallelize(all_projects).count()
print(f'Total no. of project in 3 years {no_of_distinct_projects}')
