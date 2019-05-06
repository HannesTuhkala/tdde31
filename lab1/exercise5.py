from pyspark import SparkContext
STAT_FILE = "/user/x_robsl/data/stations-Ostergotland.csv"
PREC_FILE = "/user/x_robsl/data/precipitation-readings.csv"

sc = SparkContext(appName = "exercise 5")

def max_precipitation(a, b):
    if a >= b:
        return a
    else:
        return b


def min_precipitation(a, b):
    if a <= b:
        return a
    else:
        return b

def seq_op(accumulator, element):
    return accumulator + element[0] + element[1]


def comb_op(accumulator1, accumulator2):
    return accumulator1 + accumulator2


stations_file = sc.textFile(STAT_FILE)
ostg_stations = stations_file.map(lambda line: line.split(";")[0]).collect()

precipitations_file = sc.textFile(PREC_FILE)
prec_data = precipitations_file.map(lambda line: line.split(";"))

# data: (station_number, (year-month-day, precipitation))
#prec_data = prec_lines.map(lambda x: (x[0], (x[1], x[3])))
# Filter out the data outside the wanted dates
prec_data = prec_data.filter(lambda x: int(x[1][0:4]) >= 1993 and int(x[1][0:4]) <= 2016)


########################################
#THIS IS WHERE IT FAILS. IN THE FILTER.
#########################################

# Filter only those station numbers that are in STAT_FILE.
prec_data = prec_data.filter(lambda x: x[0] in ostg_stations)

# map data like: (station_number;year-month, value)
prec_data = prec_data.map(lambda x: (x[0] + ";" + x[1][0:7], float(x[3])))
# calculate monthly precipitation for each station
prec_data = prec_data.reduceByKey(lambda val1, val2: val1 + val2)

# We remove the stationnumber, since it is not required anymore
# New structure: (year-month, (value, 1))
# The last 1 in the tuple is just used to track the amount of stations to
# divide with in the end.
prec_data = prec_data.map(lambda x: (x[0].split(';')[1], (x[1], 1)))
# Add all monthly values together
prec_data = prec_data.reduceByKey(lambda val1, val2: (val1[0] + val2[0], val1[1] + val2[1]))
# Divide the total monthly value to get average
prec_data = prec_data.map(lambda x: (x[0], x[1][0]/x[1][1]))

## Only save tuples that has the max and min temperatures for that day
#max_day_precipitations = prec_data.reduceByKey(max_precipitation)
#min_day_precipitations = prec_data.reduceByKey(min_precipitation)
#
## Combine max and min into the following tuple (date, (max_prec, min_prec))
#precipitations = max_day_precipitations.join(min_day_precipitations)
#
## Removes the day from date, structure is now (year-month, (max_prec, min_prec))
#precipitations = precipitations.map(lambda x: (x[0][0:7], x[1]))
#
## Calculates how many days there are in each month with the following output (year-month, daysInMonth)
#days = precipitations.countByKey().items()
#ndays = sc.parallelize(days)
#
## Calculates the sum of all max and min prec values per month.
#average_prec = precipitations.aggregateByKey(0, seq_op, comb_op)
#average_prec = average_prec.join(ndays)
#
## Maps average_temp to this structure and saves it: (year, month, station number, average monthly temperature)
#output_precs = average_prec.map(lambda x: (int(x[0][0:4]), int(x[0][5:7]), round(x[1][0]/(2*x[1][1]), 1)))
output_precs = prec_data.sortBy(ascending = True, keyfunc = lambda k: (k[0], k[1]))
output_precs.saveAsTextFile("avg_prec")
