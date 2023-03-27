val textFile = sc.textFile("/input/input_favMovie.txt")
val records = textFile.map(line => line.split(","))

val mapEachUser = records.map(record =>( record(0), (record(1).toInt, record(2).toFloat )))
val favMovieOfEachUser  = mapEachUser.reduceByKey{case(a,b) => ( if(a._2 > b._2) {a} else {b} )}

favMovieOfEachUser.saveAsTextFile("out")