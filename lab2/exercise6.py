from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

STAT_FILE = "/user/x_robsl/data/stations-Ostergotland.csv"
TEMPERATURES_FILE = "/user/x_robsl/data/temperature-readings.csv"

sc = SparkContext(appName = "exercise 6")
sqlContext = SQLContext(sc)


# Read temperatures file and make an RDD
temperature_file = sc.textFile(TEMPERATURES_FILE)
temp_lines = temperature_file.map(lambda line: line.split(";"))
tempReadings = temp_lines.map(lambda p: Row(station=int(p[0]), year=int(p[1][0:4]), month=int(p[1][5:7]), day=int(p[1][8:10]), temp=float(p[3])))
schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.registerTempTable("tempReadings")

# Read stations file and make an RDD
stations_file = sc.textFile(STAT_FILE)
stations_lines = stations_file.map(lambda line: line.split(";"))
stations = stations_lines.map(lambda p: Row(station=int(p[0])))
schema_stations = sqlContext.createDataFrame(stations)
schema_stations.registerTempTable("stations")

# Filter out everything outside the relevant period
valid_readings = schemaTempReadings.where("year >= 1950 and year <= 2014")

# Join on station number to filter Ostergotland stations
valid_readings = valid_readings.join(schema_stations, ['station'], 'inner').select('station', 'year', 'month', 'day', 'temp')

# Get max temperatures per day per station
max_readings = valid_readings.groupBy(['year', 'month', 'day', 'station']).agg(F.max('temp').alias('max_temp'))

# Get min temperature per day per station
min_readings = valid_readings.groupBy(['year', 'month', 'day', 'station']).agg(F.min('temp').alias('min_temp'))

# Join the max and min
min_max_readings = max_readings.join(min_readings, ['year', 'month', 'day', 'station'], 'inner')

# Calculate the average of min+max
min_max_readings = min_max_readings.withColumn('daily_average', (min_max_readings.max_temp + min_max_readings.min_temp)/2)

# Calculate the monthly average
monthly_average = min_max_readings.groupBy(['year', 'month', 'station']).agg(F.avg('daily_average').alias('avg_monthly_temp')).select('year', 'month', 'station', 'avg_monthly_temp')

# Filter out the unwanted period for long-term monthly average
long_term_monthly = monthly_average.where("year <= 1980")

# Calculate the long-term average
long_term_monthly = long_term_monthly.groupBy(['month']).agg(F.avg('avg_monthly_temp').alias('long_term_average'))

# Join the two averages by month
average_diff = long_term_monthly.join(monthly_average, ['month'], 'inner')

# Calculate the difference
average_diff = average_diff.withColumn('avg_diff', average_diff.avg_monthly_temp - average_diff.long_term_average).select('year', 'month', 'avg_diff').show()
