from pyspark import SparkContext
STAT_FILE = "/user/x_robsl/data/stations-Ostergotland.csv"
PREC_FILE = "/user/x_robsl/data/precipitation-readings.csv"

sc = SparkContext(appName = "exercise 5")

stations_file = sc.textFile(STAT_FILE)
ostg_stations = stations_file.map(lambda line: line.split(";")[0]).collect()

precipitations_file = sc.textFile(PREC_FILE)
prec_data = precipitations_file.map(lambda line: line.split(";"))

# Filter out the data outside the wanted dates
prec_data = prec_data.filter(lambda x: int(x[1][0:4]) >= 1993 and int(x[1][0:4]) <= 2016)

# Filter only those station numbers that are in STAT_FILE.
prec_data = prec_data.filter(lambda x: x[0] in ostg_stations)

# map data like: (station_number;year-month, value)
prec_data = prec_data.map(lambda x: (x[0] + ";" + x[1][0:7], float(x[3])))
# calculate monthly precipitation for each station
prec_data = prec_data.reduceByKey(lambda val1, val2: val1 + val2)

# We remove the stationnumber, since it is not required anymore
# The last 1 in the tuple is just used to track the amount of stations to
# divide with in the end.
prec_data = prec_data.map(lambda x: (x[0].split(';')[1], (x[1], 1)))
# Add all monthly values together
prec_data = prec_data.reduceByKey(lambda val1, val2: (val1[0] + val2[0], val1[1] + val2[1]))
# Divide the total monthly value to get average
prec_data = prec_data.map(lambda x: (x[0], x[1][0]/x[1][1]))

# Sort and save
output_precs = prec_data.sortBy(ascending = True, keyfunc = lambda k: (k[0], k[1]))
output_precs.saveAsTextFile("avg_prec")
