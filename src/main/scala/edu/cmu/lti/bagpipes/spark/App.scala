package cmu.edu.lti.bagpipes.spark

// import cmu.edu.lti.bagpipes.spark._

import org.apache.uima.collection.CollectionReader
import org.apache.uima.util.CasCreationUtils
import org.apache.uima.cas.CAS
import org.apache.uima.cas.impl.Serialization
import org.apache.uima.analysis_engine.AnalysisEngineDescription
import org.apache.uima.resource.metadata.TypeSystemDescription
import org.apache.uima.resource.metadata.ResourceMetaData
import org.apache.uima.examples.cpe.FileSystemCollectionReader
import org.apache.uima.tutorial.ex1.RoomNumberAnnotator
import org.apache.uima.tutorial.RoomNumber

import org.apache.uima.fit.factory._
import org.apache.uima.fit.util.JCasUtil

import cmu.edu.lti.bagpipes.spark.SparkUimaUtils._

import scala.collection.JavaConversions._

import spark.SparkContext
import spark.SparkContext._


object App {

  def main(args: Array[String]) = {
    val sc = new SparkContext(args(0), "App",
      System.getenv("SPARK_HOME"), System.getenv("SPARK_CLASSPATH").split(":"))

    val typeSystem = TypeSystemDescriptionFactory.createTypeSystemDescription()
    val params = Seq(FileSystemCollectionReader.PARAM_INPUTDIR, "data")
    val rdd = makeRDD(CollectionReaderFactory.createCollectionReader(
      classOf[FileSystemCollectionReader], params: _*), sc)
    val rnum = AnalysisEngineFactory.createEngineDescription(classOf[RoomNumberAnnotator])
    val rooms = rdd.map(process(_, rnum)).flatMap(scas => JCasUtil.select(scas.jcas, classOf[RoomNumber]))
    val counts = rooms.map(room => room.getBuilding()).countByValue()
    println(counts)
      //.foreach(println(_))
  }
}
