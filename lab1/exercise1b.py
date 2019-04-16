from time import sleep
from datetime import datetime

start_time = datetime.now()
print("Start time: ", start_time)

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

with open("/nfshome/hadoop_examples/shared_data/temperature-readings.csv") as temperature_file:
	lines = map(lambda line: line.split(";"), temperature_file)
	year_temperature = map(lambda x: (x[1][0:4], (float(x[3]), x[0])), lines)
	year_temperature = filter(lambda x: int(x[0]) >= 1950 and int(x[0]) <= 2014, year_temperature)

	max_temperatures = year_temperature.reduceByKey(max_temperature)
	max_temperaturesSorted = max_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1][0])
	min_temperatures = year_temperature.reduceByKey(min_temperature)
	min_temperaturesSorted = min_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1][0])

	max_temperaturesSorted.saveAsTextFile("max_temperature")
	min_temperaturesSorted.saveAsTextFile("min_temperature")


stop_time = datetime.now()
print("Stop time: ", stop_time)

elapsed_time = stop_time - start_time
print("Elapsed time: ", elapsed_time)
