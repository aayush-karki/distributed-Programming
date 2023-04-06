o 3.2                                                                         autoDecisionTree.scala

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.rdd._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._

val text = sc.textFile("/input/autoInput.csv")

val data = text.map(line => {
        val splitedLine = line.split(",")
        val rawData =  splitedLine.slice(22,26).map{case "?" => 0; case a => a.toDouble}
        val featuresVector = Vectors.dense(rawData.slice(0,3))
        val label = rawData.last
        println(label)
        println(featuresVector)
        LabeledPoint(label, featuresVector)
})

val categoricalFeaturesInfo = Map[Int, Int]()

val model = DecisionTree.trainRegressor(data, categoricalFeaturesInfo, "variance", 3, 8)


val testData = Vectors.dense(5000,21,27)
val prediction = model.predict(testData)
println(prediction)

println( "Learning classigication tree model\n" + model.toDebugString)