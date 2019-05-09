from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 1")
sqlContext = SQLContext(sc)
temperature_file = sc.textFile("/user/x_hantu/data/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))
tempReadings = lines.map(lambda p: Row(station=int(p[0]), year=int(p[1].split("-")[0]), value=float(p[3])))
schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.registerTempTable("tempReadings")

#max_temperatures = sqlContext.sql("SELECT year, max(value) as value FROM tempReadings WHERE year >= 1950 and year <= 2014 GROUP BY year ORDER BY value DESC")
#max_temperatures = schemaTempReadings.select('year', 'station', 'value')
max_temperatures = schemaTempReadings.select('station', 'year', 'value').groupby('year', 'value')
max_temperatures = max_temperatures.agg(F.max('value').alias('yearlymax'))
max_temperatures = max_temperatures.where("year >= 1950 and year <= 2014")
max_temperatures = max_temperatures.orderBy(['year', 'station', 'yearlymax'], ascending=[0, 0, 1])

max_temperatures = max_temperatures.rdd
max_temperatures.saveAsTextFile("lab2_max_temperature")

