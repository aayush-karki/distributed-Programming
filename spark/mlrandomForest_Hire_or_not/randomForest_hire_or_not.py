
import sys
from pyspark.sql.types import DoubleType
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext, SparkConf
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.linalg import Vectors

N = 0
Y = 1
BS = 0
MS = 1
PhD = 2

# Converts yes to 1 and no to 0
# converts BS MS PhD to 0, 1, 2
def convert(input):
        # convert Y to 1 and 
        if input == "Y":
                input = 1
        elif input == "N":
                input = 0
        elif input == "BS":
                input = 0
        elif input == "MS":
                input = 1
        elif input == "PhD":
                input = 2
        return float(input)

def parsePoint(line):
        values = [convert(x) for x in line.split(",")]
        return LabeledPoint(values[6], values[0:6])

sc = SparkContext("local", "Pyspark ML Random Forest")
data = sc.textFile("/input/Input_mlrandomForest_Hire_or_not.txt")

parsedData = data.map(parsePoint)

model = RandomForest.trainClassifier(parsedData, numClasses = 2, categoricalFeaturesInfo = {}, numTrees = 5, impurity = "gini", maxDepth = 6, maxBins = 64)

testData1 = ([7,N,6,BS,N,N])
prediction1 = model.predict(testData1)

testData2 = ([0,N,0,PhD,Y,N,Y])
prediction2 = model.predict(testData2)

f = open("Output_mlrandomForest_Hire_or_not.txt", "w+")
f.write("PRDICTION1 should be N. It is : " + str(prediction1) + " " +  ("N" if (prediction1 == float(0.0)) else "Y") + "\n")
f.write("PRDICTION2 should be Y. It is : " + str(prediction2) + " " +  ("N" if (prediction2 == float(0.0)) else "Y") + "\n")
f.close()