from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 2")
sqlContext = SQLContext(sc)
temperature_file = sc.textFile("/user/x_hantu/data/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))
tempReadings = lines.map(lambda p: Row(station=int(p[0]), year=int(p[1].split("-")[0]), month=int(p[1][5:7]), value=float(p[3])))
schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.registerTempTable("tempReadings")

max_temperatures = sqlContext.sql("SELECT year, month, DISTINCT station, value FROM tempReadings WHERE year >= 1950 and year <= 2014 and value > 10 GROUP BY year, month, COUNT(value) ORDER BY value DESC")

max_temperatures = max_temperatures.rdd
max_temperatures.saveAsTextFile("lab2_10")


