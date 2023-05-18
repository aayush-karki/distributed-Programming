import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.rdd._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._

val text = sc.textFile("/input/heart.txt")

def splitAndConvertData (rawDataStr: String) : Array[Double] = {
        val splitedLine = rawDataStr.split(",")
        val rawData =  splitedLine.map{case "?" => 0; case a => a.toDouble}
        return rawData
}

val data = text.map(line => {
        val rawData =  splitAndConvertData(line)
        val label = rawData.last
        val featuresVector = Vectors.dense(rawData.dropRight(1))
        println(label)
        println(featuresVector)
        LabeledPoint(label, featuresVector)
})

val categoricalFeaturesInfo = Map[Int, Int]()

val model = DecisionTree.trainRegressor(data, categoricalFeaturesInfo, "variance", 14, 16)

val testData = Vectors.dense(splitAndConvertData("65,1,4,130,275,0,1,115,1,1,2,?,?"))
val prediction = model.predict(testData)
println(prediction)

println( "Learning classigication tree model\n" + model.toDebugString)