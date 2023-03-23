val textFile = sc.textFile("/input/maxTempInput.txt")
val records = textFile.map(line => line.split(","))

// mapper
val mapper = records.map(record =>(record(0), record(1).toInt))

// reducer
val maxTemp = mapper.reduceByKey{case(a,b) => if(a > b) {a} else {b}}

maxTemp.saveAsTextFile("out")
