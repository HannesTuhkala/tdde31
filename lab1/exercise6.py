from pyspark import SparkContext
STAT_FILE = "/user/x_hantu/data/stations-Ostergotland.csv"
TEMP_FILE = "/user/x_hantu/data/temperature-readings.csv"


def max_temperature(a, b):
	if a >= b:
		return a
	return b


def min_temperature(a, b):
	if a <= b:
		return a
	return b


def seq_op(accumulator, element):
	return accumulator + element[0] + element[1]


def comb_op(accumulator1, accumulator2):
	return accumulator1 + accumulator2


sc = SparkContext(appName = "exercise 6")

stations_file = sc.textFile(STAT_FILE)
ostg_stations = stations_file.map(lambda line: line.split(";")[0]).collect()

temperature_file = sc.textFile(TEMP_FILE)
temp_data = temperature_file.map(lambda line: line.split(";"))

# Filter out the data outside the wanted dates
temp_data = temp_data.filter(lambda x: int(x[1][0:4]) >= 1950 and int(x[1][0:4]) <= 2014)

# Filter only those station numbers that are in STAT_FILE.
temp_data = temp_data.filter(lambda x: x[0] in ostg_stations)

# Structure: ((number, year-month-day), temperature)
temp_data = temp_data.map(lambda x: ((x[0], x[1]), float(x[3])))

max_temp = temp_data.reduceByKey(max_temperature)
min_temp = temp_data.reduceByKey(min_temperature)

# Structure: ((number, year-month-day), (max, min))
temp_data = max_temp.join(min_temp)

# Structure: ((number, year-month), (max, min))
temp_data = temp_data.map(lambda x: ((x[0][0], x[0][1][0:7]), x[1]))

# Calculate days per month
days = temp_data.countByKey().items()
ndays = sc.parallelize(days)

# Sum all days in a month
average_temp = temp_data.aggregateByKey(0, seq_op, comb_op)

# Structure: ((number, year-month), (sum, days))
average_temp = average_temp.join(ndays)

# Structure: ((number, year-month), avg_temp)
average_temp = average_temp.map(lambda x: (x[0], float(x[1][0]/(2*x[1][1]))))

# Calculate the average over stations
# Structure: (year-month, (avg_temp, #stations))
average_temp = average_temp.map(lambda x: (x[0][1], (x[1], 1)))
average_temp = average_temp.reduceByKey(lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1]))
# Structure: (year-month, avg_temp)
average_temp = average_temp.map(lambda x: (x[0], float(x[1][0]/x[1][1])))

new_temp = average_temp.filter(lambda x: int(x[0][0:4]) <= 1980)
# Structure: (month, avg_temp)
new_temp = new_temp.map(lambda x: (x[0][5:7], x[1]))

YEAR_PERIOD = 30

new_temp = new_temp.aggregateByKey(0, lambda acc, ele: acc + ele, comb_op)
new_temp = new_temp.map(lambda x: (x[0], round(x[1]/YEAR_PERIOD), 1)).collect()

def get_month_temp(x, match):
	for element in x:
		if int(element[0]) == match:
			return element[1]

#diff_temp = average_temp.map(lambda x: (x[0], (float(x[1]) - float(new_temp[int(x[0][5:7])]))))
diff_temp = average_temp.map(lambda x: (x[0], round(float(x[1]) - get_month_temp(new_temp, int(x[0][5:7])))))

output_precs = diff_temp.sortBy(ascending = True, keyfunc = lambda k: k[0])
output_precs.saveAsTextFile("long_term_diff")
