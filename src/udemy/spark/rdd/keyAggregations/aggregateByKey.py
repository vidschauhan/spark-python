# Created by vidit.singh at 01-09-2022
from src.utils.configurations.sparkConfig import SparkConfiguration

data = [
    (2, 'Vidit', 300), (3, 'Amit', 400), (4, 'Vansh', 240), (2, 'Vidit', 202), (6, 'Nina', 600),
    (4, 'Vidit', 222), (3, 'Singh', 200), (2, 'Vidit', 200), (3, 'Vidit', 200), (3, 'Loki', 230),
]

sc = SparkConfiguration.get_spark_context('aggregateByKey')
rdd = sc.parallelize(data).map(lambda x: (x[0], (x[1], x[2])))

zero_val = 0


def seq_op(acc, elem):
    if acc > elem[1]:
        return acc
    else:
        return elem


def combiner_op(acc1, acc2):
    if acc1 > acc2:
        return acc1
    else:
        return acc2


aggr_rdd = rdd.aggregateByKey(zero_val, seq_op, combiner_op)

for i in aggr_rdd.collect(): print(i)
