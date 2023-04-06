import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.rdd._
import org.apache.spark.mllib.clustering._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._

val text = sc.textFile("/input/input.csv")

// removing header
val header = text.first()
val rawData = text.filter(line => line != header)

val splitData = rawData.map(line => line.split(","))
val parsedData = splitData.map(x => ( if ( x(0) =="?") {-1} else {x(0).toDouble},
        if ( x(1) == "Y" ) { 1 } else { 0 },
        if ( x(2) =="?") {-1} else {x(0).toDouble},
        if ( x(3) == "BS" ) { 1 } else if ( x(3) == "MS" ) { 2 } else if ( x(3) == "PhD" ) { 3 } else { 4 },
        if ( x(4) == "Y" ) { 1 } else { 0 },
        if ( x(5) == "Y" ) { 1 } else { 0 },
        if ( x(6) == "Y" ) { 1 } else { 0 }
        ))

val data = parsedData.map{ x => 
        val featuresVector = Vectors.dense(x._1, x._2, x._3, x._4, x._5, x._6)
        val label = x._7
        println(label)
        println(featuresVector)
        LabeledPoint(label, featuresVector)
}

val categoricalFeaturesInfo = Map[Int,Int]()
val model = DecisionTree.trainClassifier(data, 2, categoricalFeaturesInfo, "gini", 5, 32)

val testData = Vectors.dense(10,1,3,1,0,0)
val prediction = model.predict(testData)
println(prediction)

println( "Learning classigication tree model\n" + model.toDebugString)