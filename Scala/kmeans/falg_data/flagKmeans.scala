import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.rdd._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._

val text = sc.textFile("/input/flag.txt");

val parsedata = text.map( line => Vectors.dense(line.split(",").slice(0,7).drop(1).zipWithIndex.filter(_._2 != 1).map(_._1).map( x => x.toDouble)));

parsedata.collect.foreach(println);

val kmeans = new KMeans();
kmeans.setK(10);

val model = kmeans.run(parsedata);

model.clusterCenters.foreach(println);
model.predict(parsedata).foreach(println);


println("Testing data twice with same data: 5,648,16,10,2");

// testing the model
val prediction_test1 = model.predict(Vectors.dense(5,648,16,10,2));
println("Prediction1: " + prediction_test1);

// testing the model
val prediction_test2 = model.predict(Vectors.dense(5,648,16,10,2));
println("Prediction2: " + prediction_test2);