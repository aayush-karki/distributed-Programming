import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    # create spark context
    sc = SparkContext("local", "Pyspark word count example")

    # read data from a text file and split each line into letters
    letters = sc.textFile("/input/Input_highestOccurance.txt").flatMap(lambda line: line.split(","))

    # mapper and reducer code 
    lettersCount = letters.map(lambda letter: (letter,1)).reduceByKey(lambda a, b: a + b)

    highestLetterCount = lettersCount.map(lambda tup: (1,tup)).reduceByKey(lambda x,y: x if (x[1] > y[1]) else y)

    # save the output
    highestLetterCount.saveAsTextFile("/output/wordCountOutput")