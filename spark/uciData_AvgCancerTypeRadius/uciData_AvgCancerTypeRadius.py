import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    # create spark contxt
    sc = SparkContext("local", "Pyspark average cancer Radius")

    #reading data
    lines = sc.textFile("/input/wdbc.txt").map(lambda line: line.split(","))

    # mapper code
    mappedResult = lines.map(lambda line: (str(line[1]), (float(line[2]), 1)))

    # reducer code
    sumRadius = mappedResult.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

    # calculating the average
    avgRadius = sumRadius.mapValues(lambda x: (x[0] / x[1], (x[0], x[1])))

    # save output
    # output format:  (cancerType, (cancerRadius, (totalCancerRadius, toatlCancerCount)))
    avgRadius.saveAsTextFile("/output/avgRadius")