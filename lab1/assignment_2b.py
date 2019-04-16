from pyspark import SparkContext


def count(a1, a2):
    return a1+a2

sc = SparkContext(appName="Lab 1 Exercise 2")
temperature_file = sc.textFile("/user/x_hantu/data/temperature-readings.csv")

# Split lines
lines = temperature_file.map(lambda line: line.split(";"))

# filter out everything before 1950 and after 2014
year_temperature = lines.filter(lambda x: int(x[1][0:4]) >= 1950 and int(x[1][0:4]) <= 2014)

# filter everything below 10 degrees
over_ten = lines.filter(lambda x: float(x[3]) > 10)

# map (year, month) with 1
over_ten = over_ten.map(lambda x: ((x[1][0:4] + "-" + x[1][5:7], int(x[0])), 1))

# reduce by adding occurances
result = over_ten.reduceByKey(count)

new_result = result.map(lambda x: ((int(x[0][0].split("-")[0]), int(x[0][0].split("-")[1])), x[1]))

# write to file
new_result.saveAsTextFile("assignment2")
