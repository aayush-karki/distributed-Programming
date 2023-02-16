import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    # context
    sc = SparkContext("local", "Pyspark average rating for each movie")

    # reading
    lines = sc.textFile("/input/input.txt").map(lambda line: line.split(","))

    # mapping
    mappedResult = lines.map(lambda x: (int(x[1]), (float(x[2]), 1)))

    # reduce
    reduceResults = mappedResult.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

    # avg out the values of each reduced result
    reduceResults = reduceResults.mapValues(lambda x: x[0] / x[1])

    # out
    reduceResults.saveAsTextFile("/output/output_avgRatingMovie")