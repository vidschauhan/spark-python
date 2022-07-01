# Created by vidit.singh at 01-07-2022
from pyspark.sql import SparkSession
from src.utils import Paths
import findspark

findspark.init()

spark = SparkSession.builder.appName('Udf').getOrCreate()
employee_df = spark.read.csv(Paths.base_dir() + 'employee_data.csv', header=True, inferSchema=True)

employee_df.cache()
employee_df.createOrReplaceTempView('employee')
spark.sql('select * from employee').show()
spark.sql('select state,count(*) as count from employee group by state order by state').show()

employee_df.write.csv(Paths.output_dir() + 'stu.csv', mode='overwrite')  # doesn't work due to Hadoop home missing.
