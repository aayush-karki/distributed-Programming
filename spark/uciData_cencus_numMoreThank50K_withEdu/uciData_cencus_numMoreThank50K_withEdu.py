import sys

from pyspark import SparkContext, SparkConf

#  the total number of people earning over 50k with bachelor, master and HS-grad

if __name__ == "__main__":
        # create spark contxt
        sc = SparkContext("local", "Pyspark  the total number of people earning over 50k with bachelor, master and HS-grad")

        #reading data
        readFile = sc.textFile("/input/adult.txt")

        lines = readFile.filter(lambda line: line != "")
        lines = lines.map(lambda line: line.split(", "))

        # mapper code
        mappedResult = lines.map(lambda line: ((str(line[3]), str(line[14])), 1))

        # reducer code
        educationCount = mappedResult.reduceByKey(lambda x, y: x + y)

        # calculating the average
        educationCountFilterd = educationCount.filter(lambda line: line[0][1] == ">50K" and line[0][0] in {"Bachelors", "HS-grad", "Masters"})

        total = 0
        for line in educationCountFilterd.collect():
                total = total + line[1]

        print("the total number of people earning over 50k with bachelor, master and HS-grad is ", total)