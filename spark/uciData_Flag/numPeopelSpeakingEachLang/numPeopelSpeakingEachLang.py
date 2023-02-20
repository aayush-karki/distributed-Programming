import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    # context
    sc = SparkContext("local", "Pyspark total number of people speaking each language")

    # reading
    lines = sc.textFile("/input/flag.txt").map(lambda line: line.split(","))

    # mapping
    mappedResult = lines.map(lambda data: (int(data[5]), int(data[4])))
    
    # reduce
    reduceResult = mappedResult.reduceByKey(lambda x, y: x + y)

    # out
    reduceResult.saveAsTextFile("/output/totalNumForEachLang")
