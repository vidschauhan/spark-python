# Created by vidit.singh at 02-09-2022

from src.utils.configurations.sparkConfig import SparkConfiguration

# sample (withReplacement = True,fraction=.10,seed=None)
# where : withReplacement -> when true then duplicates allowed,else not
# fraction means the percentage of sample data from RDD, but it is not guranted by spark
# seed = To get the same data every time.

sc = SparkConfiguration.get_spark_context('RDD Sampling')
rdd = sc.parallelize(range(1, 100), 4)
rdd_sample = rdd.sample(withReplacement=False, fraction=0.1, seed=10)  # Sample is a Transformation

print(rdd_sample.collect())

# num = number of records.
action = rdd.takeSample(withReplacement=False, num=10, seed=10)  # This is an action
print(action)
