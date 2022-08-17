# Created by vidit.singh at 05-07-2022

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, lpad, date_format, to_date, count, lit

from src.utils import Paths

spark = SparkSession.builder.appName('Employee Dataframes').getOrCreate()
spark.conf.set('spark.sql.shuffle.partitions', "2")  # Default is 200.

airport_codes_df = spark.read.csv(Paths.base_dir() + '\\flightmonth200801\\airport-codes-na.txt', sep='\t',
                                  header=True, inferSchema=True)
flight_data_df = spark.read.parquet(Paths.base_dir() + '\\flightmonth200801\\part-00000.parquet')
flight_data_df.show()
flight_data_df.printSchema()
airport_codes_df.show()
airport_codes_df.printSchema()

# flight_data_df.select("Year", "Month", "DayOfMonth", "Origin", "Dest", "CRSDepTime").show()
# Get number of flights departed from each of the US airport in the month of 2008 January.
usa_airport = airport_codes_df.filter(col('Country') == 'USA')
usa_airport_flight_inner_join_data = flight_data_df.join(usa_airport, usa_airport.IATA == flight_data_df.Origin,
                                                         'inner')

usa_airport_flight_inner_join_data. \
    select("Year", "Month", "DayOfMonth", "Origin", "Dest", usa_airport['*'], "CRSDepTime").show()

usa_airport_flight_inner_join_data \
    .groupBy("Origin") \
    .agg(count(lit(1)).alias("FlightCount")). \
    orderBy(col("FlightCount").desc()). \
    show()

dormant_airport_count = usa_airport.join(flight_data_df, usa_airport['IATA'] == flight_data_df['Origin'], 'left') \
    .filter(col('Origin').isNull()) \
    .count()

print(f'No Flight departed from {dormant_airport_count} airports in Jan 2008')

# Get the total number of flights departed from the airports in January 2008 that do not contain entries in
# airport-codes.

flight_data_df.join(airport_codes_df, airport_codes_df['IATA'] == flight_data_df['Origin'], 'left')\
    .filter(airport_codes_df['IATA'].isNull()) \
    .select("Year", "Month", "DayOfMonth", "Origin", "Dest", airport_codes_df['*'], "CRSDepTime").show()