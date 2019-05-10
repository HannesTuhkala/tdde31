
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 2")
sqlContext = SQLContext(sc)
temperature_file = sc.textFile("/user/x_robsl/data/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))
tempReadings = lines.map(lambda p: Row(station=int(p[0]), year=int(p[1].split("-")[0]), month=int(p[1].split("-")[1]), value=float(p[3])))
schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.registerTempTable("tempReadings")

max_temperatures = schemaTempReadings.where('year >= 1950 and year <= 2014 and value > 10')

#max_temperatures = max_temperatures.groupBy(['year', 'month'])

# For each month
#month_count = max_temperatures.count()
#month_count = month_count.orderBy(['count'], ascending=False)

# Once per station
station_count = max_temperatures.select('year', 'month', 'station').distinct().groupBy(['year', 'month'])
station_count = station_count.count().orderBy(['count'], ascending=False)

station_count = station_count.rdd
station_count.saveAsTextFile("lab2_e2")
