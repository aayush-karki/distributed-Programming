import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    # context
    sc = SparkContext("local", "Pyspark count number of males in the input file within age group 40 to 71 and females with age group 30 to 60.")

    # reading data
    lines = sc.textFile("/input/countPeopleBasedOnAgeAndGenderInput.txt").map(lambda line: line.split(","))

    # mapping into gender as key and (gender, age, count) as value
    # mappedResult = lines.map(lambda x: (int(x[1]), (int(x[1]), int(x[0]), 1)))
    mappedResult = lines.map(lambda x: (int(x[0]), (int(x[1]))) )
    mappedResult = mappedResult.map(lambda x: (1, (x[1], 1)) if (x[1] == 1 and x[0] > 30 and x[0] < 60) else ( (0, (x[1], 1)) if (x[1] == 0 and x[0] > 40 and x[0] <71) else (2, (0,0)) ) )

    # reducing: reduced by gender
    # results = mappedResult.reduceByKey(lambda x, y: (x[0] + y[0], x[1]+ y[1], x[2] + y[2]) if ((y[0] == 0 and (y[1] >= 40 and y[1] <= 71) ) or (y[0] == 1 and (y[1] >= 30 and y[1] <= 60) )) else (x[0], x[1], x[2]))
    reduceResults = mappedResult.reduceByKey(lambda x, y: (x[0], x[1] + y[1]))
    
    # removing the gender as we know the gender from key
    reduceResults = reduceResults.mapValues(lambda x: x[1])

    # removing the unnecessary key
    reduceResults = reduceResults.filter(lambda x: x[0] < 2)
    
    # out
    reduceResults.saveAsTextFile("/output/countPeople")