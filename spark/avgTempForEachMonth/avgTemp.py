import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    # create spark context
    sc = SparkContext("local", "Pyspark average temperature")

    # read data from a text file and split each line into words
    lines = sc.textFile("/input/maxTemp.txt").map(lambda line: line.split(" "))

    # mapper code: mapping the line to month (temp, count)
    mappedResult = lines.map(lambda x: (str(x[0]), (int(x[1]), 1)))

    # reducer code: reducing each month to add temp and count
    sumTemp = mappedResult.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

    # calculating the average
    avgTemp = sumTemp.mapValues(lambda x: (x[0]/ x[1], x[0]))
    # save the counts to output
    avgTemp.saveAsTextFile("/output/avgTempOutput")