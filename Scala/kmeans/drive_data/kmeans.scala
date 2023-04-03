import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.rdd._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._

val text = sc.textFile("/input/driver_data.csv")

// processing the header
val header = text.first()
val filterData = text.filter( line => line != header)

// removing driver id
val parsedata = filterData.map( line => Vectors.dense(line.split(",").drop(1).map(x => x.toDouble)))

parsedata.collect.foreach(println)

val kmeans = new KMeans()
kmeans.setK(2)

val model = kmeans.run(parsedata)

model.clusterCenters.foreach(println)
model.predict(parsedata).foreach(println)

val testData = model.predict(Vectors.dense(70,25))
println(testData)