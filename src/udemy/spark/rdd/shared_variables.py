# Created by vidit.singh at 05-09-2022
from src.utils.configurations.sparkConfig import SparkConfiguration

# Broadcast variables
days = {'sun': 'sunday',
        'mon': 'monday',
        'tue': 'tuesday',
        'wed': 'wednesday'
        }

data = [('Vidit', 'Singh', 'meeting', 'mon'),
        ('Anup', 'Chola', 'appointment', 'wed'),
        ('Shivani', 'Srivastava', 'visit', 'tue'),
        ('Ketki', 'Goel', 'appointment', 'sun'),
        ('Anupama', 'Pathak', 'meeting', 'mon')
        ]

sc = SparkConfiguration.get_spark_context('Broadcast variables')
days_bc = sc.broadcast(days)
data_rdd = sc.parallelize(data)


# returning value from broadcast variable map.
def fetch_broadcast_variable_data(element):
    return days_bc.value[element]


output = data_rdd.map(lambda x: (x[0], x[1], x[2], fetch_broadcast_variable_data(x[3]))).collect()

for item in output:
    print(item)

# Accumulators : These are associative and commutative operations ie . sum(5+7) = sum(6+6)

acc = sc.accumulator(0)
rdd = sc.parallelize([1,2,3,4,5])
da = rdd.foreach(lambda x : acc.add(x))
print(acc.value)