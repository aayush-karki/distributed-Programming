import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler

if __name__ == "__main__":

	sc = SparkContext("local", "Pyspark kmeans dow jones index")

	# convert sparkContext to sqlContext
	sqlContext = SQLContext(sc)

	# reading to dataFrame
	rawDataDF = sqlContext.read.format("com.databricks.spark.csv").options(header = "false", inferschema = "true").load("/input/flag.txt")
	rawDataDF.columns
	rawDataDF.printSchema()
	rawDataDF.show(1)

	# column names in data frame
	columnsNum = [1, 3, 4, 5, 6]
	columnsName = ["landmass", "area", "population", "language", "religion"]

	# Create the spark dataframe
	df = rawDataDF.select(*(rawDataDF.columns[i] for i in columnsNum))
	df.columns
	df.printSchema()
	df.show(1)

	# assembling all the col in to a feature
	vecAssembler = VectorAssembler(inputCols=["_c1","_c3","_c4","_c5","_c6"], outputCol="features")
	assembled_data = vecAssembler.transform(df)
	assembled_data.show(1)

	# creating and using kmeans
	kmeans = KMeans(k=10, seed=1)
	model = kmeans.fit(assembled_data.select('features'))
	prediction = model.transform(assembled_data)
	prediction.show()

	# evaluating the clusters
	evaluator = ClusteringEvaluator()
	score =  evaluator.evaluate(prediction)
	print( "Score: " + str(score))

	print("Testing data twice with same data: 5,648,16,10,2")

	# testing the model
	prediction_test1 = model.predict(Vectors.dense([5,648,16,10,2]))
	print("Prediction1: " + str(prediction_test1))

	# testing the model
	prediction_test2 = model.predict(Vectors.dense([5,648,16,10,2]))
	print("Prediction2: " + str(prediction_test2))