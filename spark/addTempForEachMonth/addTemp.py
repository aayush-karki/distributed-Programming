import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    # create spark context
    sc = SparkContext("local", "Pyspark total temperature")

    # read data from a text file and split each line into words
    lines = sc.textFile("/input/maxTemp.txt").map(lambda line: line.split(" "))

    # mapper and reducer code 
    results = lines.map(lambda x: (str(x[0]), int(x[1]))).reduceByKey(lambda a, b: a + b)
    # lambda a, b: a + b ----------- here previous value (a) = a + new value ( b )

    # save the counts to output
    results.saveAsTextFile("/output/addTempOutput")
