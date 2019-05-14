from pyspark import SparkContext


# Function to find the max of two values
def max_temperature(a, b):
    if a >= b:
        return a
    else:
        return b


# Function to find the min of two values
def min_temperature(a, b):
    if a <= b:
        return a
    else:
        return b

sc = SparkContext(appName = "exercise 1")

# Create RDD
temperature_file = sc.textFile("/user/x_robsl/data/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))
year_temperature = lines.map(lambda x: (x[1][0:4], (float(x[3]), x[0])))
year_temperature = year_temperature.filter(lambda x: int(x[0]) >= 1950 and int(x[0]) <= 2014)

# Find the max temperatures
max_temperatures = year_temperature.reduceByKey(max_temperature)
max_temperaturesSorted = max_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1][0])

# Find the min temperatures
min_temperatures = year_temperature.reduceByKey(min_temperature)
min_temperaturesSorted = min_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1][0])

# Save to file
max_temperaturesSorted.saveAsTextFile("max_temperature")
min_temperaturesSorted.saveAsTextFile("min_temperature")
