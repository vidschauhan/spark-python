# Created by vidit.singh at 03-07-2022
import pyspark.sql
import findspark

findspark.init()


# Explicitly mapping here return type as Dataframe as main program unable to refer
def get_employee_df(spark) -> pyspark.sql.DataFrame:
    employees = [(1, "Scott", "Tiger", 1000.0, "10", "united states", "+1 123 456 7890", "123 45 6789"),
                 (2, "Henry", "Ford", 1250.0, None, "India", "+91 234 567 8901", "456 78 9123"),
                 (3, "Nick", "Junior", 750.0, '', "united KINGDOM", "+44 111 111 1111", "222 33 4444"),
                 (4, "Bill", "Singh", 1500.0, "10", "AUSTRALIA", "+61 987 654 3210", "789 12 6118")
                 ]
    return spark.createDataFrame(employees, schema="""employee_id INT, first_name STRING, 
                    last_name STRING, salary FLOAT, bonus STRING, nationality STRING,
                    phone_number STRING, ssn STRING""")
