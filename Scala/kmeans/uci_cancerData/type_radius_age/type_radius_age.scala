import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.rdd._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._

val text = sc.textFile("/input/breast-cancer.txt")

// we only consider the following featuer:
//      sizeimport org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.rdd._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._

val text = sc.textFile("/input/breast-cancer.txt")

// we only consider the following featuer:
//      size
//      ageGroup 
//      deg-malig

// removing everything beside above feature and converting them to double
val splitedData = text.map(line => line.split(","))
val filtedData = splitedData.map(x => (x(1), x(3), x(6)))
filtedData.collect.foreach(println)

val parsedata = filtedData.map( tuple => Vectors.dense(tuple._1.split("-").head.toDouble, tuple._2.split("-").head.toDouble, tuple._3.toDouble ))
parsedata.collect.foreach(println)

val kmeans = new KMeans()
kmeans.setK(10)

val model = kmeans.run(parsedata)

model.clusterCenters.foreach(println)
model.predict(parsedata).foreach(println)

val testData = model.predict(Vectors.dense(30.0,30.0,3))

println(testData)(mean of distance from center to points on the perimeter), 
//      ageGroup 
//      deg-mag

// removing everything beside above feature and converting them to double
val splitedData = text.map(line => line.split(","))
val filtedData = splitedData.map(x => (x(1), x(3), x(6)))
filtedData.collect.foreach(println)

val parsedata = filtedData.map( tuple => Vectors.dense(tuple._1.split("-").head.toDouble, tuple._2.split("-").head.toDouble, tuple._3.toDouble ))
parsedata.collect.foreach(println)

val kmeans = new KMeans()
kmeans.setK(10)

val model = kmeans.run(parsedata)

model.clusterCenters.foreach(println)
model.predict(parsedata).foreach(println)

val testData = model.predict(Vectors.dense(30.0,30.0,3))

println(testData)