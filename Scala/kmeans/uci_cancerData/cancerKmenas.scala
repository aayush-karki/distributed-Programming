import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.rdd._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._

val text = sc.textFile("/input/inputCancerUCIData.txt")

// we only consider the following featuer:
//      radius(mean of distance from center to points on the perimeter), 
//      texture(sd of fray-scale values), 
//      perimeter,
//      area 

// removing everything beside above feature and converting them to double
val splitedData = text.map(line => line.split(","))
val filtedData = splitedData.map(x => (x(3), x(6), x(9), x(12)))
filtedData.collect.foreach(println)

val parsedata = filtedData.map( tuple => Vectors.dense(tuple._1.toDouble, tuple._2.toDouble, tuple._3.toDouble, tuple._4.toDouble ))
parsedata.collect.foreach(println)

val kmeans = new KMeans()
kmeans.setK(2)

val model = kmeans.run(parsedata)

model.clusterCenters.foreach(println)
model.predict(parsedata).foreach(println)

val testData = model.predict(Vectors.dense(17.99,1001,0.3001,0.07871))

println(testData)