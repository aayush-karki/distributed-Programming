import sys
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    # context
    sc = SparkContext("local", "Pyspark  the country which has the highest population for each landass")

    # reading
    lines = sc.textFile("/input/flag.txt").map(lambda line: line.split(","))

    # mapping landmass to country and population
    mappedResult = lines.map(lambda data: (int(data[1]), (str(data[0]), int(data[4]))))
    
    # reduce by landmass
    reduceResult = mappedResult.reduceByKey(lambda x, y: x if (x[1] > y[1]) else y)

    landmassKey = {"1":"N.America", "2":"S.America", "3":"Europe", "4":"Africa", "5":"Asia", "6":"Oceania"}
    mappedReducedResult = reduceResult.map(lambda x: (landmassKey[str(x[0])], x[1]))

    for x in mappedReducedResult.collect():
        print(x)

    # out
    mappedReducedResult.saveAsTextFile("/output/UCI_flag_Data_countryWithLargestPopForEachLandMass")
