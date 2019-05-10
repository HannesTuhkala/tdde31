from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 3")
sqlContext = SQLContext(sc)
temperature_file = sc.textFile("/user/x_robsl/data/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))
tempReadings = lines.map(lambda p: Row(station=int(p[0]), year=int(p[1].split("-")[0]), month=int(p[1][5:7]), day=int(p[1].split("-")[2]), value=float(p[3])))
schemaTempReadings = sqlContext.createDataFrame(tempReadings)
schemaTempReadings.registerTempTable("tempReadings")

# Filter the data
schemaTempReadings = schemaTempReadings.where('year >= 1960 and year <= 2014')

# Get max temperatures per day per station
max_readings = schemaTempReadings.groupBy(['year', 'month', 'day', 'station']).agg(F.max('value').alias('max_temp'))

# Get min temeprature per day per station
min_readings = schemaTempReadings.groupBy(['year', 'month', 'day', 'station']).agg(F.min('value').alias('min_temp'))

min_max_readings = max_readings.join(min_readings, ['year', 'month', 'day', 'station'], 'inner')

min_max_readings = min_max_readings.withColumn('daily_average', (min_max_readings.max_temp + min_max_readings.min_temp)/2)

monthly_average = min_max_readings.groupBy(['year', 'month', 'station']).agg(F.avg('daily_average').alias('avg_monthly_temp')).select('year', 'month', 'station', 'avg_monthly_temp').show()
