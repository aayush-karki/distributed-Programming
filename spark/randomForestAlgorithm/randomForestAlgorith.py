import sys
from pyspark.sql.types import DoubleType
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext, SparkConf
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.linalg import Vectors

def parsePoint(line):
    values = [float(x) for x in line.split(",")]
    return LabeledPoint(values[6], values[0:6])

sc = SparkContext("local", "Pyspark ML Random Forest")
data = sc.textFile("/input/randomForest_inputData.csv")

parsedData = data.map(parsePoint)

model = RandomForest.trainClassifier(parsedData, numClasses = 2, categoricalFeaturesInfo = {}, numTrees = 5, impurity = "gini", maxDepth = 5, maxBins = 32)

testData = ([10,1,3,1,0,0])
prediction = model.predict(testData)

f = open("OUTPUT", "w+")
f.write("PRDICTION: " + str(prediction) + "\n")
f.close()