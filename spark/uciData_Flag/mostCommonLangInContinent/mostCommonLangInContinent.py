import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    # context
    sc = SparkContext("local", "Pyspark most common language in a continent")

    # reading
    lines = sc.textFile("/input/flag.txt").map(lambda line: line.split(","))

    # mapping continent and the lang as key to population
    mappedResult = lines.map(lambda data: ((int(data[1]), int(data[5])), int(data[4])))
    
    # reduce adding the population who speak same language in each continent
    # each reduce Result is in the form ((continent, langugage), population)
    reduceResult = mappedResult.reduceByKey(lambda x, y: x + y)

    # mapping so that we can compare to get the most common language 
    mapForCompare = reduceResult.map( lambda x: (x[0][0], (x[0][1], x[1])))

    # reduce
    reduceForCompare = mapForCompare.reduceByKey( lambda x, y: x if (x[1] > y[1]) else y)

    # out
    reduceForCompare.saveAsTextFile("/output/mostCommonLangInContinent")
