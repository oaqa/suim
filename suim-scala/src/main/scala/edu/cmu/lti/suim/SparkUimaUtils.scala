/*
 *  Copyright 2013 Carnegie Mellon University
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.cmu.lti.suim

import java.net.URI

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.bufferAsJavaList

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.SequenceFile
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.SparkContext.writableWritableConverter
import org.apache.uima.analysis_engine.AnalysisEngineDescription
import org.apache.uima.collection.CollectionReader
import org.apache.uima.fit.factory.AnalysisEngineFactory
import org.apache.uima.fit.factory.JCasFactory

object SparkUimaUtils {

  def createSequenceFile(reader: CollectionReader, uri: String) {
    val conf = new Configuration()
    val fs = FileSystem.get(URI.create(uri), conf)
    val path = new Path(uri)
    val nw = NullWritable.get
    val writer = SequenceFile.createWriter(fs, conf, path, nw.getClass(), classOf[SCAS])
    while (reader.hasNext()) {
      val jcas = JCasFactory.createJCas();
      val cas = jcas.getCas()
      reader.getNext(cas)
      val scas = new SCAS(cas)
      writer.append(nw, scas)
    }
    IOUtils.closeStream(writer)
  }

  def sequenceFile(reader: CollectionReader, uri: String, sc: SparkContext) = {
    createSequenceFile(reader, uri)
    sc.sequenceFile[NullWritable, SCAS](uri).values
  }

  def readFrom(reader: CollectionReader): java.util.List[SCAS] = {
    val buffer = collection.mutable.ArrayBuffer[SCAS]()
    while (reader.hasNext()) {
      val jcas = JCasFactory.createJCas();
      val cas = jcas.getCas()
      reader.getNext(cas)
      buffer += new SCAS(cas)
    }
    buffer
  }

  def makeRDD(reader: CollectionReader, sc: SparkContext) = {
    val buffer = readFrom(reader)
    sc.parallelize(buffer)
  }

  def process(scas: SCAS, description: AnalysisEngineDescription) = {
    val ae = AnalysisEngineFactory.createEngine(description)
    ae.process(scas.jcas)
    scas
  }
}
