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

# list of column names that needs $ removed
removeDollarList = ["open", "high", "low", "close", "next_weeks_open", "next_weeks_close"]

# functin to convert stock to ascii
@F.udf(returnType=IntegerType())
def convertToAsciiVal(str):
	val = 0

	for char in str:
		val = val + ord(char)

	return val

# function to convert date mm/dd/yyyy to yyyymmdd and then into int
@F.udf(returnType=IntegerType())
def convertDateToInteger(str):
	splitedDate = str.split("/")

	# if the date and month is from 1 to 9 then add 0 in front of it
	if(len(splitedDate[0]) == 1):
		splitedDate[0] = "0" + splitedDate[0]

	if(len(splitedDate[1]) == 1):
		splitedDate[1] = "0" + splitedDate[1]

	return int(splitedDate[2] + splitedDate[0] + splitedDate[1])

if __name__ == "__main__":

	sc = SparkContext("local", "Pyspark kmeans dow jones index")

	# convert sparkContext to sqlContext
	sqlContext = SQLContext(sc)

	# reading to dataFrame
	df = sqlContext.read.format("com.databricks.spark.csv").options(header = "true", inferschema = "true").load("/input/dow_jones_index.txt")
	df.columns
	df.printSchema()
	df.show(1)

	# removing the $ from open, high, low, close, next_weeks_open, next_weeks_close and casting them to flaot
	for colName in removeDollarList:
		df = df.withColumn(colName, F.regexp_replace(colName, "\$", "").cast(FloatType()))

	df.show(1)
	df.printSchema()

	# converting stock name to int by using convertToAsciiVal function
	df = df.withColumn("stock", convertToAsciiVal(F.col("stock")))

	df.show(1)
	df.printSchema()

	# convertingdate to int by using convertDateToInteger function
	df = df.withColumn("date", convertDateToInteger(F.col("date")))

	df.show(1)
	df.printSchema()

	# filling all the NaN with 0
	df = df.fillna(0)
	df.show(1)

	# assembling all the col in to a feature
	vecAssembler = VectorAssembler(inputCols=["quarter", "stock", "date", "open", "high", "low", "close", "volume", "percent_change_price", "percent_change_volume_over_last_wk", "previous_weeks_volume", "next_weeks_open", "next_weeks_close", "percent_change_next_weeks_price", "days_to_next_dividend", "percent_return_next_dividend"], outputCol="features")
	assembled_data = vecAssembler.transform(df)
	assembled_data.show(1)

	# creating and using kmeans
	kmeans = KMeans(k=9, seed=1)
	model = kmeans.fit(assembled_data.select('features'))
	prediction = model.transform(assembled_data)
	prediction.show()

	# evaluating the clusters
	evaluator = ClusteringEvaluator()
	score =  evaluator.evaluate(prediction)
	print( "Score: " + str(score))


# k = 5,  Score: 0.8293369848994477
# k = 2,  Score: 0.8384933141374612
# k = 8,  Score: 0.7532213723081116
# k = 9,  Score: 0.7593414916778463
# k = 10, Score: 0.5717851253639384
# k = 15, Score: 0.5521492413420346
# k = 20, Score: 0.5598055802677462