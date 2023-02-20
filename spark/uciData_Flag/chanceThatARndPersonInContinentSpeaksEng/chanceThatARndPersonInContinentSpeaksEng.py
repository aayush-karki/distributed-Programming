import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    # context
    sc = SparkContext("local", "Pyspark chance that a randomly chosen person in a continent speaks english")

    # reading
    lines = sc.textFile("/input/flag.txt").map(lambda line: line.split(","))

    # mapping continent as key to  language,  population, englishSpeakingPopulation
    # here englishSpeakingPoulation is going to be 0
    # population is going to hold the total population
    mappedResult = lines.map(lambda data: (int(data[1]), (int(data[5]), int(data[4]), 0)))
    
    # reduce adding the population who speak same language in each continent
    # each reduce Result is in the form (continent, language,  toatlPopulation, englishSpeakingPopulation)
    reduceResult = mappedResult.reduceByKey(lambda x, y: (y[0], x[1] + y[1], x[2] + y[1]) if (y[0] == 1) else (x[0], x[1] + y[1], x[2]))

    # getting the probability
    reduceResult = reduceResult.mapValues( lambda values: values[2] / values[1])

    # out
    reduceResult.saveAsTextFile("/output/chanceThatARndPersonInContinentSpeaksEng")
