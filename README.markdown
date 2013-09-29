# uima-on-spark

This toolkit aims to provide a simple interface for UIMA analyitics on top of the Spark framework.

For example: Count distinct buildings from a file collection:


```scala
    val typeSystem = TypeSystemDescriptionFactory.createTypeSystemDescription()
    val params = Seq(FileSystemCollectionReader.PARAM_INPUTDIR, "data")
    val rdd = makeRDD(createCollectionReader(classOf[FileSystemCollectionReader], params: _*), sc)
    val roomAnnotator = createEngineDescription(classOf[RoomNumberAnnotator])
    val rooms = rdd.map(process(_, roomAnnotator)).flatMap(scas => JCasUtil.select(scas.jcas, classOf[RoomNumber]))
    val counts = rooms.map(room => room.getBuilding()).countByValue()
    println(counts)
```

If the collection is to large to fit in memory use an HDFS RDD:

```scala
    val rdd = sequenceFile(reateCollectionReader(classOf[FileSystemCollectionReader], params: _*),
      "hdfs://localhost:9000/documents", sc)
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
