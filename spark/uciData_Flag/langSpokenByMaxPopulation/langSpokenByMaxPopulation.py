import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    # context
    sc = SparkContext("local", "Pyspark language spoken by max number of population")

    # reading
    lines = sc.textFile("/input/flag.txt").map(lambda line: line.split(","))

    # mapping language as key to  population
    mappedResult = lines.map(lambda data: (int(data[5]), int(data[4])))
    
    # reduce add all the population that speaks a particular language
    reduceResult = mappedResult.reduceByKey(lambda x, y: x + y)

    # mapping each reduced result to 1 as key and everything else as value
    mapMaxPopulation = reduceResult.map(lambda redResult: (1, (redResult)))

    # getting the language spoken by max poppulation
    reduceMaxPopulationResult = mapMaxPopulation.reduceByKey( lambda x , y: x if (x[1] > y[1]) else y)

    reduceMaxPopulationResult = reduceMaxPopulationResult.map( lambda x: x[1])

    # out
    reduceMaxPopulationResult.saveAsTextFile("/output/langSpokenByMaxPopulation")
