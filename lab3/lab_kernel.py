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
    dlon = lon2 -lon1
    dlat = lat2 -lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km


h_distance = 2# Up to you
h_date  = 2 #  Up  to  you
h_time = 2 # Up to you
a_lat = 58.4274 # Up to you
b_lon = 14.826 # Up to you
date = "2013-07-04" # Up to you

station_file = sc.textFile("/user/x_hantu/data/stations.csv")
temp_file = sc.textFile("/user/x_hantu/data/temperature-readings.csv")

stations = station_file.map(lambda line: line.split(";"))
stations = stations.map(lambda x: [(x[0], x[3], x[4])]).cache()

temperatures = temp_file.map(lambda line: line.split(";"))
temperatures = temperatures.map(lambda x: [(x[0], x[1], x[2])]).cache()

hav_map = stations.map(lambda x: [(x[0], haversine(x[2], x[1], b_lon, a_lat))])
hav_map.saveAsTextFile("hav_map")


def gauss_kernel_sum(x):
    
    return gauss_kernel_dist(x/h_distance) + \
            gauss_kernel_date(x/h_date) + \
            gauss_kernel_time(x/h_time)


def gauss_kernel_dist(x):
    """
    Input is a place? (longitude and latitude)
    """
    #return exp(-NORM(x))
    pass


def gauss_kernel_date(x):
    pass


def gauss_kernel_time(x):
    pass
