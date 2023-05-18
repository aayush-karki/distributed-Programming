import sys

from pyspark import SparkContext, SparkConf

# list the average price for all makes

if __name__ == "__main__":
    # create spark contxt
    sc = SparkContext("local", "Pyspark maximum make price UCI Auto Data Set")

    #reading data
    lines = sc.textFile("/input/auto.txt").map(lambda line: line.split(","))
    # mapper code to map make to price and count
    mapByMake = lines.map(lambda line: (str(line[2]), int(line[25])) if(line[25] != "?") else (str(line[2]), 0))

    # reducer code to reduce by make
    reduceByMake = mapByMake.reduceByKey(lambda x, y: x if (x > y) else y)

    for make in reduceByMake.collect():
        print(make)

    # save output
    reduceByMake.saveAsTextFile("/output/UCI_autoData_maxMakePrice")