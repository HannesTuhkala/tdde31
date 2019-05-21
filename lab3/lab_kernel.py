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


# Width of guass
h_distance = 2  # Up to you
h_date = 2  # Up  to  you
h_time = 2  # Up to you

# Variables for prediction
a_lat = 58.4274  # Up to you
b_lon = 14.826  # Up to you
date = "2013-07-04"  # Up to you

station_file = sc.textFile("/user/x_hantu/data/stations.csv")
temp_file = sc.textFile("/user/x_hantu/data/temperature-readings.csv")

stations = station_file.map(lambda line: line.split(";"))
stations = stations.map(lambda x: [(x[0], x[3], x[4])]).cache()

temperatures = temp_file.map(lambda line: line.split(";"))
temperatures = temperatures.map(lambda x: [(x[0], x[1], x[2])]).cache()

hav_map = stations.map(lambda x: [(x[0], haversine(x[2], x[1], b_lon, a_lat))])
hav_map.saveAsTextFile("hav_map")

# Remove all future dates
temperatures.reduce(lambda x: x[1] < date).cache()


def gauss_kernel_sum(in_dist, in_date, in_time):
    return gauss_kernel_dist(in_dist) + \
            gauss_kernel_date(in_date) + \
            gauss_kernel_time(in_time)


def gauss_kernel_dist(in_dist):
    # return exp(-NORM(x))
    return exp(-(in_dist/h_distance)**2)


def gauss_kernel_date(in_date):
    return exp(-(in_date/h_date)**2)


def gauss_kernel_time(in_time):
    return exp(-(in_time/h_time)**2)
