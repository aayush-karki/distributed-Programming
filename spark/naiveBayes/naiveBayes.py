from pyspark.ml.classification import NaiveBayes
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import SQLContext

sc = SparkContext("local", "Pyspark naive bayes")
sqlCon = SQLContext(sc)

df = sqlCon.read.format("com.databricks.spark.csv").options(header = "true", inferschema = "true").load("/input/input.txt")

ss = SparkSession(sc)
gender_indexer = StringIndexer(inputCol = "gender", outputCol = "gender_index")
label_indexer = StringIndexer(inputCol = "purchased", outputCol = "label")

# intregating new features to the orginal dataset
df = gender_indexer.fit(df).transform(df)
df = label_indexer.fit(df).transform(df)

assembler = VectorAssembler(inputCols = ["age", "gender_index", "income"], outputCol = "features")
df = assembler.transform(df).select("label", "features")

nb = NaiveBayes(featuresCol = "features", labelCol = "label")
nb_model = nb.fit(df)

specific_case = ss.createDataFrame([(31, "Female", 50000)], ["age", "gender", "income"])

specific_case = gender_indexer.fit(specific_case).transform(specific_case)
specific_case = assembler.transform(specific_case).select("features")


specific_case_prediction = nb_model.transform(specific_case)
specific_case_prediction_label = specific_case_prediction.select(col("prediction")).first()[0]

print( " predicted label: {}".format(specific_case_prediction_label))