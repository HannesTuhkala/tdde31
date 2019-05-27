from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext(appName = "exercise 5")
sqlContext = SQLContext(sc)
station_file = sc.textFile("/user/x_hantu/data/stations-Ostergotland.csv")
stat_lines = station_file.map(lambda line: line.split(";"))
statReadings = stat_lines.map(lambda p: Row(station=int(p[0])))
schemaStatReadings = sqlContext.createDataFrame(statReadings)
schemaStatReadings.registerTempTable("statReadings")

precipitation_file = sc.textFile("/user/x_hantu/data/precipitation-readings.csv")
prec_lines = precipitation_file.map(lambda line: line.split(";"))
precReadings = prec_lines.map(lambda p: Row(station=int(p[0]), year=int(p[1][0:4]), month=int(p[1][5:7]), prec=float(p[3])))
schemaPrecReadings = sqlContext.createDataFrame(precReadings)
schemaPrecReadings.registerTempTable("precReadings")

# Filter data
valid_precipitations = schemaPrecReadings.where("year >= 1993 and year <= 2016")
# Join stations with the data
precipitations = valid_precipitations.join(schemaStatReadings, 'station', 'inner')

precipitations = precipitations.groupBy('year', 'month', 'station').agg(F.sum('prec').alias('prec'))
precipitations = precipitations.groupBy('year', 'month').agg(F.avg('prec').alias('avgPrec')).orderBy(['year', 'month'], ascending=False).show()
