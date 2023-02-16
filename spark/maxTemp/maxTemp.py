import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    sc = SparkContext("local", "Pyspark max temperature")
    
    # read
    lines = sc.textFile("/input/maxTemp.txt").map(lambda line: line.split(" "))

    # mapper and reducer
    result = lines.map(lambda x: (str(x[0]), int(x[1]))).reduceByKey(lambda x, y: x if x > y else y)

    # output
    result.saveAsTextFile("/output/maxTempOutput")