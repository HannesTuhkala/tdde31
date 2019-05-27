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

# Filter out readings outside of wanted period
valid_temperatures = schemaTempReadings.where("year >= 1950 and year <= 2014")

# Find max values
max_temperatures = valid_temperatures.groupBy('year').agg(F.max('value').alias('value'))
max_temperatures = max_temperatures.join(valid_temperatures, ['year', 'value'], 'inner').select('year', 'station', 'value')
# Order the max values
max_temperatures = max_temperatures.orderBy('value', ascending=False)
max_temperatures = max_temperatures.rdd

# Write to file
max_temperatures.saveAsTextFile("lab2_max_temperature")

# Find min values
min_temperatures = valid_temperatures.groupBy('year').agg(F.min('value').alias('value'))
min_temperatures = min_temperatures.join(valid_temperatures, ['year', 'value'], 'inner').select('year', 'station', 'value')
# Order the min values
min_temperatures = min_temperatures.orderBy('value', ascending=False).show()


