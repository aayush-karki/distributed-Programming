import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    # create spark contxt
    sc = SparkContext("local", "Pyspark average cancer Radius")

    #reading data
    lines = sc.textFile("/input/breast-cancer.txt").map(lambda line: line.split(","))

    # mapper code to degree and age
    mapByDegree = lines.map(lambda line: ((int(line[6]), str(line[1])), 1))

    # reducer code to degree and age
    reduceByDegree = mapByDegree.reduceByKey(lambda x, y: ( x + y))

    # filter all degree 1 and 3
    degree2 = reduceByDegree.filter(lambda x: x[0][0] == 2)
    
    # map by age 
    mapByAge = degree2.map(lambda tup: (1, (tup[0][1], tup[1])))

    # get highes by reduce by age 
    highestCountByAge = mapByAge.reduceByKey(lambda x,y : x if (x[1] > y[1]) else y)

    # save output
    highestCountByAge.saveAsTextFile("/output/highestAgeWithD2Tumor")