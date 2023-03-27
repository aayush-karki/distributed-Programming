val textFile = sc.textFile("/input/avgTempInput.txt")
val records = textFile.map(line => line.split(" "))

val mapper = records.map(record =>( record(0), (record(1).toFloat, 1 )))
val reducer  = mapper.reduceByKey{case(a,b) => ( a._1 + b._1, a._2 + b._2 )}

val  avgTemp = reducer.mapValues(tuple => (tuple._1 / tuple._2))

avgTemp.saveAsTextFile("out")