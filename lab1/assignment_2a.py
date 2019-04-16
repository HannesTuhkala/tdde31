from pyspark import SparkContext


def count(a1, a2):
    return a1+a2

sc = SparkContext(appName="Lab 1 Exercise 2")
temperature_file = sc.textFile("/user/x_hantu/data/temperature-readings.csv")

# Split lines
lines = temperature_file.map(lambda line: line.split(";"))

# filter out everything before 1950 and after 2014
year_temperature = lines.filter(lambda x: int(x[1][0:4]) >= 1950 and int(x[1][0:4]) <= 2014)

# filter everything above 10 degrees
over_ten = lines.filter(lambda x: float(x[3]) > 10)

# map (year, month) with 1
over_ten = over_ten.map(lambda x: ((int(x[1][0:4]), int(x[1][5:7])), 1))

# reduce by adding occurances
result = over_ten.reduceByKey(count)

# write to file
result.saveAsTextFile("assignment2")
