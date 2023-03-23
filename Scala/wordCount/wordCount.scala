val textFile = sc.textFile("/input/input.txt")
val words = (textFile.flatMap(line => line.split(" ")))
// writing mapper
val counts = words.map(word => (word,1)).reduceByKey(case(x,y) => x+y)
counts.saveAsTextFile("out")