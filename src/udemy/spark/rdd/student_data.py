# Created by vidit.singh at 28-06-2022
from src.utils.configurations.sparkConfig import SparkConfiguration
from src.utils import Paths

sc = SparkConfiguration.get_spark_context('Student data analysis')
student_rdd = sc.textFile(Paths.base_dir() + 'student_data.csv')
headers = student_rdd.first()
print('Total no. of Students : ',
      student_rdd.filter(lambda x: x != headers).count())  # Returns the number pf elements in RDD

# Find total marks achieved by Male and Females

total_marks = student_rdd.filter(lambda x: x != headers) \
    .map(lambda student: (student.split(',')[1], int(student.split(',')[5]))) \
    .groupByKey() \
    .map(lambda student: (student[0], sum(student[1]))) \
    .collect()

print('Total marks :', total_marks)

total_enrolment_per_course = student_rdd.filter(lambda x: x != headers) \
    .map(lambda student: student.split(',')[3]) \
    .countByValue()

print('Total Enrollment per course :: ', total_enrolment_per_course)

student_age_avg = student_rdd.filter(lambda x: x != headers) \
    .map(lambda student: (student.split(',')[1], (int(student.split(',')[0]), 1))) \
    .reduceByKey(lambda data1, data2: (data1[0] + data2[0], data1[1] + data2[1])) \
    .map(lambda student: (student[0], round(student[1][0] / student[1][1], 2))).collect()

# You may also use mapValues() to work only on values returned by reduceByKeys.
# .mapValues(lambda student: round(student[0] / student[1],2)).collect()

print("Students age average : ", student_age_avg)

# Get Avg Marks of All female Students in DSA course.

dsa_avg_female_marks = student_rdd.filter(lambda x: x != headers) \
    .filter(lambda subject: subject.split(',')[3] == 'DSA' and subject.split(',')[1] == 'Female') \
    .map(lambda student: (student.split(',')[1], (int(student.split(',')[4]), 1))) \
    .reduceByKey(lambda m1, m2: (m1[0] + m2[0], m1[1] + m2[1])) \
    .mapValues(lambda avg: round(avg[0] / avg[1], 2)).collect()

print(dsa_avg_female_marks)