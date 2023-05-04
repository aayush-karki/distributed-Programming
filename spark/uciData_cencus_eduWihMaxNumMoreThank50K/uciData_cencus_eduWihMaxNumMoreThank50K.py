import sys

from pyspark import SparkContext, SparkConf

#  the education level of the maximum number of people earning over 50k

if __name__ == "__main__":
        # create spark contxt
        sc = SparkContext("local", "Pyspark  the education level of the maximum number of people earning over 50k")

        #reading data
        readFile = sc.textFile("/input/adult.txt")

        lines = readFile.filter(lambda line: line != "")
        lines = lines.map(lambda line: line.split(", "))

        # mapper code
        mappedResult = lines.map(lambda line: ((str(line[3]), str(line[14])), 1))

        # reducer code
        educationCount = mappedResult.reduceByKey(lambda x, y: x + y)

        # calculating the average
        educationCountFilterd = educationCount.filter(lambda line: line[0][1] == ">50K")

        # mapper to map 1 to education, totalNum
        educationMap = educationCountFilterd.map(lambda x: (1, (x[0][0], x[1])))
        educationReduce = educationMap.reduceByKey(lambda x, y: x if ( x[1] > y[1] ) else y )

        for line in educationReduce.collect():
                print(line)