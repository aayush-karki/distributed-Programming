import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.linalg import Vectors
from  pyspark.ml.feature import VectorAssembler

if __name__ == "__main__":

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

    vecAssembler = VectorAssembler(inputCols=["Exp","isEmployed","PreviousJob","EducationLevel","TopTierSchool","Internship","Hired"], outputCol="features")
    new_df = vecAssembler.transform(df)
    new_df.show()

    kmeans = KMeans(k=2, seed=1)
    model = kmeans.fit(new_df.select('features'))
    prediction = model.transform(new_df)
    prediction.show()

    zeroPre = prediction.filter(prediction.prediction == 0).show()
    onePre = prediction.filter(prediction.prediction == 1).show()

    evaluator = ClusteringEvaluator()
    score =  evaluator.evaluate(prediction)
    print( "Score: " + str(score))

    # testing the model
    prediction_test = model.predict(Vectors.dense([1,1,1,3,1,0,1]))
    print("Prediction: " + str(prediction_test))

