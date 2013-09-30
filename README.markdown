# SUIM

Spark for Unstructured Information, provides a simple interface for [UIMA](http://uima.apache.org/) analyitics 
on top of [Spark](http://spark.incubator.apache.org/). 
The main abstraction Spark provides is a resilient distributed dataset (RDD), which is a collection 
of elements partitioned across the nodes of the cluster that can be operated on in parallel [1].


## Examples
Count distinct buildings from a file collection:


```scala
    val typeSystem = TypeSystemDescriptionFactory.createTypeSystemDescription()
    val params = Seq(FileSystemCollectionReader.PARAM_INPUTDIR, "data")
    val rdd = makeRDD(createCollectionReader(classOf[FileSystemCollectionReader], params: _*), sc)
    val rnum = createEngineDescription(classOf[RoomNumberAnnotator])
    val rooms = rdd.map(process(_, rnum)).flatMap(scas => JCasUtil.select(scas.jcas, classOf[RoomNumber]))
    val counts = rooms.map(room => room.getBuilding()).map((_,1)).reduceByKey(_ + _)
    counts.foreach(println(_))
```

If the collection is to large to fit in memory use an HDFS RDD:

```scala
    val rdd = sequenceFile(reateCollectionReader(classOf[FileSystemCollectionReader], params: _*),
      "hdfs://localhost:9000/documents", sc)
```

Use DKPro Core [2] to annotate and Spark to count words.

```scala
    val typeSystem = TypeSystemDescriptionFactory.createTypeSystemDescription()
    val rdd = makeRDD(createCollectionReader(classOf[TextReader],
      ResourceCollectionReaderBase.PARAM_SOURCE_LOCATION, "data",
      ResourceCollectionReaderBase.PARAM_LANGUAGE, "en",
      ResourceCollectionReaderBase.PARAM_PATTERNS,  Array("[+]*.txt")), sc)
    val seg = createPrimitiveDescription(classOf[BreakIteratorSegmenter])
    val tokens = rdd.map(process(_, seg)).flatMap(scas => JCasUtil.select(scas.jcas, classOf[Token]))
    val counts = tokens.map(token => token.getCoveredText())
      .filter(filter(_))
      .map((_,1)).reduceByKey(_ + _)
      .map(pair => (pair._2, pair._1)).sortByKey(true)
    counts.foreach(println(_))
```

### Common Tasks

To build:

    mvn compile

To run:

    mvn scala:run

To test:

    mvn test

To create standalone with dependencies:

    mvn package
    java -jar target/spark-uima-tools-0.0.1-SNAPSHOT-jar-with-dependencies.jar

## References
* [1] http://spark.incubator.apache.org/docs/latest/scala-programming-guide.html
* [2] https://code.google.com/p/dkpro-core-asl/
