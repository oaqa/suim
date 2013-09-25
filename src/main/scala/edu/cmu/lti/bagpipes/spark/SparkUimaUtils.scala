package cmu.edu.lti.bagpipes.spark

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

import scala.collection.JavaConversions._

import spark.SparkContext
import spark.SparkContext._

import java.io.ByteArrayOutputStream 
import java.io.ByteArrayInputStream 

object SparkUimaUtils {

  def makeRDD(reader: CollectionReader, sc: SparkContext) = {
    val buffer = collection.mutable.MutableList[SCAS]()
    while (reader.hasNext()) {
      val jcas = JCasFactory.createJCas();
      val cas = jcas.getCas()
      reader.getNext(cas)
      buffer += new SCAS(cas)
    }
    sc.makeRDD(buffer)
  }

  def process(scas: SCAS, description: AnalysisEngineDescription) = {
    val ae = AnalysisEngineFactory.createAggregate(description)
    ae.process(scas.jcas)
    scas
  }
}
