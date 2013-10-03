# SUIM

Spark for Unstructured Information, provides a thin abstraction layer for [UIMA](http://uima.apache.org/)  
on top of [Spark](http://spark.incubator.apache.org/). 
SUIM leverages on Spark resilient distributed dataset (RDD) to run UIMA pipelines 
distributed across the nodes on a cluster that can be operated on in parallel [1].

SUIM allows you to run analytical pipelines on the resulting (or intermediate) `CAS` to execute furhter text analytics or 
machine learning algorithms.

## Examples

#### Count buildings from the UIMA tutorial.

Using the `RoomAnnotator` from the UIMA tutorial:


```scala
    val typeSystem = TypeSystemDescriptionFactory.createTypeSystemDescription()
    val params = Seq(FileSystemCollectionReader.PARAM_INPUTDIR, "data")
    val rdd = makeRDD(createCollectionReader(classOf[FileSystemCollectionReader], params: _*), sc)
    val rnum = createEngineDescription(classOf[RoomNumberAnnotator])
    val rooms = rdd.map(process(_, rnum)).flatMap(scas => JCasUtil.select(scas.jcas, classOf[RoomNumber]))
    val counts = rooms.map(room => room.getBuilding()).map((_,1)).reduceByKey(_ + _)
    counts.foreach(println(_))
```

If the collection is to large to fit in memory, or you already have a collection of `SCAS`es use an HDFS RDD:

```scala
    val rdd = sequenceFile(reateCollectionReader(classOf[FileSystemCollectionReader], params: _*),
      "hdfs://localhost:9000/documents", sc)
```

#### Tokenize and count words with DKPro Core

Use DKPro Core [2] to tokenize and Spark to do token level analytics.

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
