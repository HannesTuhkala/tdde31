from pyspark import SparkContext
temp_file = "/user/x_robsl/data/temperature-readings.csv"
prec_file = "/user/x_robsl/data/precipitation-readings.csv"


def max_temperature(temp):
    return temp >= 25 and temp <= 30


def max_precipitation(prec):
    return prec >= 100 and prec <= 200


# Find the value of tuple b with the "key" key.
def getItem(key, b):
    for item in b:
        if item[0] == key:
            return item[1]


# Combines the two tuples into one as (number, max_temp, max_prec)
def combine(a, b):
    result = []
    for item in a:
        if (item[0] in [i[0] for i in b]):
            result += [item[0], item[1], getItem(item[0], b)]


sc = SparkContext(appName = "exercise 4")
temperature_file = sc.textFile(temp_file)
temp_lines = temperature_file.map(lambda line: line.split(";"))
# Creates a tuple as (stationnumber, max_temp)
year_temperature = temp_lines.map(lambda x: (x[0]+";"+x[1], float(x[3])))
# Removes all tuples that do not satisfy max_temperature
max_temp = year_temperature.filter(max_temperature)


precipitation_file = sc.textFile(prec_file)
prec_lines = precipitation_file.map(lambda line: line.split(";"))
# Creates a tuple as (stationnumber, date, max_prec)
prec_lines = prec_lines.map(lambda x: (x[0]+";"+x[1], float(x[3])))
# Calculate maximum daily precipitation
prec_lines = prec_lines.reduceByKey(lambda x, y: x+y)
# Removes all tuples that do not satisfy max_precipitation
prec_lines = prec_lines.filter(max_precipitation)

# Combine the two tuples into (number, max_temp, max_prec)
# We have to use an existing function to be able to map using their data objects
# look at https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations
result = max_temp.join(prec_lines)
result.saveAsTextFile("max_temp_prec")
