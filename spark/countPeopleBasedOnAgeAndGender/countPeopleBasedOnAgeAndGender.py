import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    # context
    sc = SparkContext("local", "Pyspark count number of males in the input file within age group 40 to 71 and females with age group 30 to 60.")

    # reading data
    lines = sc.textFile("/input/countPeopleBasedOnAgeAndGenderInput.txt").map(lambda line: line.split(","))

    # mapping into gender as key and (gender, age, count) as value
    mappedResult = lines.map(lambda x: (int(x[1]), (int(x[1]), int(x[0]), 1)))

    # reducing: reduced by gender
    results = mappedResult.reduceByKey(lambda x, y: (x[0] + y[0], x[1]+ y[1], x[2] + y[2]) if ((y[0] == 0 and (y[1] >= 40 and y[1] <= 71) ) or (y[0] == 1 and (y[1] >= 30 and y[1] <= 60) )) else (x[0], x[1], x[2]))

    # out
    results.saveAsTextFile("/output/countPeople")