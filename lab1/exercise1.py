from pyspark import SparkContext

def max_temperature(a, b):
	if a >= b:
		return a
	else:
		return b

def min_temperature(a, b):
	if a <= b:
		return a
	else:
		return b

sc = SparkContext(appName = "exercise 1")
temperature_file = sc.textFile("/user/x_hantu/data/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))
year_temperature = lines.map(lambda x: (x[1][0:4], float(x[3], x[0])))
year_temperature = year_temperature.filter(lambda x: int(x[0]) >= 1950 and int(x[0]) <= 2014)

max_temperatures = year_temperature.reduceByKey(max_temperature)
max_temperaturesSorted = max_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1])
min_temperatures = year_temperature.reduceByKey(min_temperature)
min_temperaturesSorted = min_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1])

max_temperaturesSorted.saveAsTextFile("max_temperature")
min_temperaturesSorted.saveAsTextFile("min_temperature")



