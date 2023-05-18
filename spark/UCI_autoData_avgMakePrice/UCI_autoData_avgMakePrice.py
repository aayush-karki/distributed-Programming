import sys

from pyspark import SparkContext, SparkConf

# list the average price for all makes

if __name__ == "__main__":
    # create spark contxt
    sc = SparkContext("local", "Pyspark average make price UCI Auto Data Set")

    #reading data
    lines = sc.textFile("/input/auto.txt").map(lambda line: line.split(","))
    # mapper code to map make to price and count
    mapByMake = lines.map(lambda line: (str(line[2]), (int(line[25]), 1)) if(line[25] != "?") else (str(line[2]), (0, 1)))

    # reducer code to reduce by make
    reduceByMake = mapByMake.reduceByKey(lambda x, y: ( x[0]+ y[0], x[1] + y[1]))

    #for all make get average
    avgMakePrice = reduceByMake.mapValues(lambda x: (x[0]/ x[1], x[0]))
    
    for make in avgMakePrice.collect():
        print(make)


    # save output
    avgMakePrice.saveAsTextFile("/output/UCI_autoData_avgMakePrice")