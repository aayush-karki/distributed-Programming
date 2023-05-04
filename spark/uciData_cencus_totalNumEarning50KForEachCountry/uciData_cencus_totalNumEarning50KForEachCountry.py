import sys

from pyspark import SparkContext, SparkConf

#   number of people earning over 50k for each countries

if __name__ == "__main__":
        # create spark contxt
        sc = SparkContext("local", "Pyspark  the total number of people earning over 50k with bachelor, master and HS-grad")

        #reading data
        readFile = sc.textFile("/input/adult.txt")

        lines = readFile.filter(lambda line: line != "")
        lines = lines.map(lambda line: line.split(", "))

        # mapper code
        mappedResult = lines.map(lambda line: ((str(line[13]), str(line[14])), 1))

        # reducer code
        educationCount = mappedResult.reduceByKey(lambda x, y: x + y)

        # calculating the average
        educationCountFilterd = educationCount.filter(lambda line: line[0][1] == ">50K")

        for line in educationCountFilterd.collect():
                print(line)

        # output
        result.saveAsTextFile("/output/output_uciData_cencus_totalNumEarning50KForEachCountry")
