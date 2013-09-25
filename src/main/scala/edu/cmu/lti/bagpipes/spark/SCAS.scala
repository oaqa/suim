package cmu.edu.lti.bagpipes.spark

import org.apache.uima.cas.CAS
import org.apache.uima.cas.impl.Serialization
import org.apache.uima.resource.metadata.TypeSystemDescription
import org.apache.uima.resource.metadata.ResourceMetaData
import org.apache.uima.examples.cpe.FileSystemCollectionReader
import org.apache.uima.tutorial.ex1.RoomNumberAnnotator

import org.apache.uima.fit.factory._
import org.apache.uima.fit.factory.TypeSystemDescriptionFactory;

import scala.collection.JavaConversions._

import spark.SparkContext
import spark.SparkContext._

import java.io._

class SCAS(val cas: CAS) extends Externalizable {

  def this() {
    this(JCasFactory.createJCas().getCas())
  }

  override def readExternal(in: ObjectInput) {
    val bytes = in.readObject().asInstanceOf[Array[Byte]]
    val bais = new ByteArrayInputStream(bytes)
    Serialization.deserializeCAS(cas, bais);
  }

  override def writeExternal(out: ObjectOutput) {
    val baos = new ByteArrayOutputStream();
    Serialization.serializeWithCompression(cas, baos)
    out.writeObject(baos.toByteArray())
  }

  def jcas = {
    cas.getJCas()
  }
}
