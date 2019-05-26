from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext

sc = SparkContext(appName="lab_kernel")


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km


def day_differences(pred_date, prev_date):
	d1 = datetime.strptime(pred_date, "%Y-%m-%d")
	d2 = datetime.strptime(prev_date, "%Y-%m-%d")
	return abs((d2 - d1).days)


def time_differences(pred_time, prev_time):
	# Convert to seconds
	t1_hour = pred_time.split(":")[0] * 3600
	t2_hour = prev_time.split(":")[0] * 3600
	return abs(t2_hour - t1_hour)


def gauss_kernel_sum(in_dist, in_date, in_time):
    return gauss_kernel_dist(in_dist) + \
            gauss_kernel_date(in_date) + \
            gauss_kernel_time(in_time)


def gauss_kernel_dist(in_dist):
    return exp(-(in_dist/h_distance)**2)


def gauss_kernel_date(in_date):
    return exp(-(in_date/h_date)**2)


def gauss_kernel_time(in_time):
    return exp(-(in_time/h_time)**2)


# Width of guass
h_distance = 100  # In km 
h_date = 30  # In days
h_time = 6*3600  # In seconds

# Variables for prediction
a_lat = 58.4274  # Up to you
b_lon = 14.826  # Up to you
date = "2013-07-04"  # Up to you


station_file = sc.textFile("/user/x_hantu/data/stations.csv")
temp_file = sc.textFile("/user/x_hantu/data/temperature-readings.csv").sample(False, 0.1)

stations = station_file.map(lambda line: line.split(";"))
stations = stations.map(lambda x: [(int(x[0]), float(x[3]), float(x[4]))])

print("ROBIN")
temperatures = temp_file.map(lambda line: line.split(";"))
# Remove all future dates
temperatures = temperatures.filter(lambda x: str(x[1]) < date)
print("MEEEEEEEEEEE")
temperatures = temperatures.map(lambda x: [(int(x[0]), x[1], x[2], float(x[3]))])

print("HOLA")

# Structure: 
#hav_map = sc.broadcast(stations.map(lambda x: [(x[0], haversine(x[2], x[1], b_lon, a_lat))]).collectAsMap())
hav_map = stations.map(lambda x: [(x[0], haversine(x[2], x[1], b_lon, a_lat))]).collectAsMap()

print("YESSSSSSSSSSSSSSS")

# Structure: (station, km, days, time, temp)
temperatures = temperatures.map(lambda x: [(x[0], hav_map[x[0]], day_differences(date, x[1]), x[2], x[3])])

print("I GOT HERE MF")

forecast = []
# For every other hour between 4 and 24..
for i in range(4, 25, 2):
	# Structure (i, value)
	forecast1 = temperatures.map(lambda x: [(i, gauss_kernel_sum(x[1], x[2], time_differences(x[3], i))*x[4])]).reduceByKey(lambda x, y: x + y)
	forecast2 = temperatures.map(lambda x: [(i, gauss_kernel_sum(x[1], x[2], time_differences(x[3], i)))]).reduceByKey(lambda x, y: x + y)
	forecast += [(date, a_lat, b_lon, i, forecast1[1] / forecast2[1])]

print("FUUUUU")
forecast = sc.parallelize(forecast)
forecast.saveAsTextFile("forecast")

