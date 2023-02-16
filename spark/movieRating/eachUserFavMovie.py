import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    # context
    sc = SparkContext("local", "Pyspark each users fav movie")

    # reading
    lines = sc.textFile("/input/input.txt").map(lambda line: line.split(","))

    # mapping
    mappedResult = lines.map(lambda x: (int(x[0]), (int(x[1]), float(x[2]))))

    # reduce
    reduceResults = mappedResult.reduceByKey(lambda x, y: x if (x[1] > y[1]) else y)

    # out
    reduceResults.saveAsTextFile("/output/output_eachUserFavMovie")