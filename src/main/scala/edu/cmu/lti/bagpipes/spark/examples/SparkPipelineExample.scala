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

package cmu.edu.lti.bagpipes.spark.examples

// import cmu.edu.lti.bagpipes.spark._

import org.apache.uima.collection.CollectionReader
import org.apache.uima.util.CasCreationUtils
import org.apache.uima.cas.CAS
import org.apache.uima.jcas.JCas
import org.apache.uima.cas.impl.Serialization
import org.apache.uima.analysis_engine.AnalysisEngineDescription
import org.apache.uima.resource.metadata.TypeSystemDescription
import org.apache.uima.resource.metadata.ResourceMetaData
import org.apache.uima.examples.cpe.FileSystemCollectionReader
import org.apache.uima.tutorial.ex1.RoomNumberAnnotator
import org.apache.uima.tutorial.RoomNumber

import org.apache.uima.fit.factory._
import org.apache.uima.fit.factory.AnalysisEngineFactory._
import org.apache.uima.fit.util.JCasUtil._
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;

import cmu.edu.lti.bagpipes.spark.SparkUimaUtils._

import scala.collection.JavaConversions._

import spark.SparkContext
import spark.SparkContext._


object SparkPipelineExample {

  def main(args: Array[String]) = {
    val sc = new SparkContext(args(0), "App",
      System.getenv("SPARK_HOME"), System.getenv("SPARK_CLASSPATH").split(":"))

    val typeSystem = TypeSystemDescriptionFactory.createTypeSystemDescription()
    val params = Seq(FileSystemCollectionReader.PARAM_INPUTDIR, "data")
    val rdd = makeRDD(CollectionReaderFactory.createCollectionReader(
      classOf[FileSystemCollectionReader], params: _*), sc)
    val partial = rdd.map(process(_, createEngineDescription(classOf[SimpleAnnotator]))).cache
    val result = partial.map(process(_, createEngineDescription(classOf[RoomNumberAnnotator])))
    result.flatMap(scas => select(scas.jcas, classOf[RoomNumber])).foreach(println(_))
  }
}

class SimpleAnnotator extends JCasAnnotator_ImplBase {
  override def process(jcas: JCas) {
    val annotation = new RoomNumber(jcas, 0, 0);
    annotation.setBuilding("empty building");
    annotation.addToIndexes();
  }
}
