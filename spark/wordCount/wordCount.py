import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    # create spark context
    sc = SparkContext("local", "Pyspark word count example")

    # read data from a text file and split each line into words
    words = sc.textFile("/input/input.txt").flatMap(lambda line: line.split(" "))

    # mapper and reducer code 
    wordCounts = words.map(lambda word: (word,1)).reduceByKey(lambda a, b: a + b)
    # lambda a, b: a + b ----------- here previous value (a) = a + new value ( b )

    # save the counts to output
    wordCounts.saveAsTextFile("/output/wordCountOutput")