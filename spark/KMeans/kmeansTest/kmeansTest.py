import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from  pyspark.ml.feature import VectorAssembler

sc = SparkContext("local", "Pyspark dataframe")

# convert sparkContext to sqlContext
sqlContext = SQLContext(sc)

# reading to dataFrame
df = sqlContext.read.format("com.databricks.spark.csv").options(header = "true", inferschema = "true").load("/input/inputData.txt")
df.show()

# sorting based on hired and showing only the filtered data
sorted_Df = df.sort("Hired").show()
not_hired = df.filter(df.Hired == 0).show()
hired = df.filter(df.Hired == 1).show()

vecAssembler = VectorAssembler(inputCols=["Hired"], outputCol="features")
new_df = vecAssembler.transform(df)
new_df.show()

kmeans = KMeans(k=2, seed=1)
model = kmeans.fit(new_df.select('features'))
transformed = model.transform(new_df)
transformed.show()

    