from datetime import datetime

# Start the timer
start_time = datetime.now()
print("Start time: ", start_time)

# Dictionaries to store max and min values
max_res = {}
min_res = {}

# Add the period 1950-2014 to the min and max dictionaries
for year in range(1950, 2015):
    max_res[year] = float("-inf")
    min_res[year] = float("inf")

# Find the maximum and minimum temperatures for each year
with open("/home/robin/Documents/tdde31/station_data/temperature-readings.csv") as temperature_file:
    for line in temperature_file:
        split = line.split(';')
        year = int(split[1][0:4])
        if year <= 1950 or year >= 2014:
            continue
        temp_val = float(split[3])

        if max_res[year] < temp_val:
            max_res[year] = temp_val
        if min_res[year] > temp_val:
            min_res[year] = temp_val

    sorted_max = sorted(max_res.items(), key=lambda i: i[1], reverse=True)
    sorted_min = sorted(min_res.items(), key=lambda i: i[1], reverse=True)

    max_file = open("max_file.txt", "w")
    min_file = open("min_file.txt", "w")

    # Write result to file
    for val in sorted_max:
        max_file.write(str(val)+"\n")
    for val in sorted_min:
        min_file.write(str(val)+"\n")

# Stop timer
stop_time = datetime.now()
print("Stop time: ", stop_time)

# Calculate time it took to execute
elapsed_time = stop_time - start_time
print("Elapsed time: ", elapsed_time)
