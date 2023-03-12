import sys
from pyspark.sql.types import DoubleType
from pyspark.mllib.regression import LabeledPoint
from pyspark import SparkContext, SparkConf
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.linalg import Vectors

def convert(input):
        if input == "M":
                input = 1
        elif input == "B":
                input = 0
        return float(input)

def parsePoint(line):
        values = [convert(x) for x in line.split(",")]
        return LabeledPoint(values[1], values[2:32])

sc = SparkContext("local", "Pyspark ML Random Forest")
data = sc.textFile("/input/input.txt")

parsedData = data.map(parsePoint)

model = RandomForest.trainClassifier(parsedData, numClasses = 2, categoricalFeaturesInfo = {}, numTrees = 30, impurity = "gini", maxDepth = 10, maxBins = 1024)

testData = ([13.54,14.36,87.46,566.3,0.09779,0.08129,0.06664,0.04781,0.1885,0.05766,0.2699,0.7886,2.058,23.56,0.008462,0.0146,0.02387,0.01315,0.0198,0.0023,15.11,19.26,99.7,711.2,0.144,0.1773,0.239,0.1288,0.2977,0.07259])
prediction = model.predict(testData)

f = open("output_uciData_cancer_mlRandomForest.txt", "w+")
f.write("PRDICTION: " + str(prediction) + " " +  ("M" if (prediction == float(0.0)) else "B") + "\n")
f.close()