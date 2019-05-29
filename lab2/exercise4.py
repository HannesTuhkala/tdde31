from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 4")
sqlContext = SQLContext(sc)
temperature_file = sc.textFile("/user/x_robsl/data/temperature-readings.csv")
temp_lines = temperature_file.map(lambda line: line.split(";"))
tempReadings = temp_lines.map(lambda p: Row(station=int(p[0]), year=int(p[1][0:4]), month=int(p[1][5:7]), day=int(p[1][8:10]), temp=float(p[3])))
schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.registerTempTable("tempReadings")

precipitation_file = sc.textFile("/user/x_robsl/data/precipitation-readings.csv")
prec_lines = precipitation_file.map(lambda line: line.split(";"))
precReadings = prec_lines.map(lambda p: Row(station=int(p[0]), year=int(p[1][0:4]), month=int(p[1][5:7]), day=int(p[1][8:10]), prec=float(p[3])))
schemaPrecReadings = sqlContext.createDataFrame(precReadings)
schemaPrecReadings.registerTempTable("precReadings")

# Max temperature per station
valid_temperatures = schemaTempReadings.groupBy('station').agg(F.max('temp').alias('maxTemp'))

# Filter temperatures
valid_temperatures = valid_temperatures.where("maxTemp >= 25.0 and maxTemp <= 30.0")

# Daily precipitation
daily_precipitation = schemaPrecReadings.groupBy(['station', 'year', 'month', 'day']).agg(F.sum('prec').alias('dailyPrec'))

# Max precipitations
max_precs = daily_precipitation.groupBy('station').agg(F.max('dailyPrec').alias('maxDailyPrec'))

# Filter precipitations
valid_precipitations = max_precs.where("maxDailyPrec >= 100.0 and maxDailyPrec <= 200.0")

max_stations = valid_temperatures.join(valid_precipitations, ['station'], 'inner').select('station', 'maxTemp', 'maxDailyPrec')

max_stations = max_stations.orderBy('station', ascending=False)
max_stations = max_stations.rdd

max_stations.saveAsTextFile("lab2_ex4")
