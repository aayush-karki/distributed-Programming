import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    # context
    sc = SparkContext("local", "Pyspark  the highest spoken language by countries in each landmasss")

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

    landmassDic = {"1":"N.America", "2":"S.America", "3":"Europe", "4":"Africa", "5":"Asia", "6":"Oceania"}
    langDic = {"1":"English", "2":"Spanish", "3":"French", "4":"German", "5":"Slavic", "6":"Other Indo-European", "7":"Chinese", "8":"Arabic", "9":"Japanese/Turkish/Finnish/Magyar", "10":"Others"}
    mappedReducedResult = reduceForCompare.map(lambda x: (landmassDic[str(x[0])], langDic[str(x[1][0])], x[1][1]))

    for x in mappedReducedResult.collect():
        print(x)

    # out
    mappedReducedResult.saveAsTextFile("/output/UCI_flag_Data_highestSpokencountryForEachLandMass")
