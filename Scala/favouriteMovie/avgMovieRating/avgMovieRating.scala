val textFile = sc.textFile("/input/input_favMovie.txt")
val records = textFile.map(line => line.split(","))

val mapEachMovie = records.map(record =>( record(1).toInt, ( record(2).toFloat, 1 )))
val reduceAvgMovie  = mapEachMovie.reduceByKey{case(a,b) => ( a._1 + b._1, a._2 + b._2 )}

val  avgMovieRating = reduceAvgMovie.mapValues(tuple => (tuple._1 / tuple._2))

avgMovieRating.saveAsTextFile("out")