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


sc = SparkContext(appName = "exercise 3")
temperature_file = sc.textFile("/user/x_hantu/data/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# Of type (date, (temp, stationnumber))
day_temperature = lines.map(lambda x: ((x[1], x[0]), float(x[3])))

# Remove all tuples which has a year < 1960 or > 2014
day_temperature = day_temperature.filter(lambda x: int(x[0][0][0:4]) >= 1960 and int(x[0][0][0:4]) <= 2014)

# Only save tuples that has the max and min temperatures for that day
max_day_temperatures = day_temperature.reduceByKey(max_temperature)
min_day_temperatures = day_temperature.reduceByKey(min_temperature)

# Combine max and min into the following tuple (key, (max_temp, min_temp))
temperatures = max_day_temperatures.join(min_day_temperatures)

# Removes the day from date, all grouped up by (year-month, stnnumber) now.
temperatures = temperatures.map(lambda x: ((x[0][0][0:7], x[0][1]), x[1]))

# Calculates how many days there are in each month with the following output ((year-month, stationnumber), daysInMonth)
days = temperatures.countByKey().items()
ndays = sc.parallelize(days)

# ((year-month, stationnumber), (max_temp, min_temp)).
# Trying to sum up max_temp with min_temp for each day in a month, but it fails. Not sure what I'm doing wrong here.
# Output should be ((year-month, stationnumber), avg_temp)
average_temp = temperatures.reduceByKey(lambda x, y: (x[0] + x[1] + y[0] + y[1]))

# Todo: Divide each average_temp with the corresponding month in ndays. ndays and average_temp has the same keys.

# Maps average_temp to this structure and saves it: (year, month, station number, average monthly temperature)
output_temps = average_temp.map(lambda x: (x[0][0][0:4], x[0][0][5:7], x[0][1], x[1]))
output_temps.saveAsTextFile("avg_temp8")
