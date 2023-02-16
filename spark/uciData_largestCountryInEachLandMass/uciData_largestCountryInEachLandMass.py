import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    # context
    sc = SparkContext("local", "Pyspark largest country in each land mass")

    # reading the data
    lines = sc.textFile("/input/flag.txt").map(lambda line: line.split(","))

    # mapping 
    mappedResult = lines.map(lambda data: (int(data[1]), (str(data[0]), int(data[3]))))

    # reducing
    reduceResult = mappedResult.reduceByKey(lambda x, y: y if( y[1] > x[1]) else x)

    # output
    reduceResult.saveAsTextFile("/output/OutputLargestCountryInLandmass")