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

package edu.cmu.lti.suim.examples

import org.apache.uima.fit.factory.AnalysisEngineFactory.createEngineDescription
import org.apache.uima.fit.factory.CollectionReaderFactory.createReader
import org.apache.uima.fit.factory._
import org.apache.uima.fit.util.JCasUtil

import de.tudarmstadt.ukp.dkpro.core.io.text.TextReader
import de.tudarmstadt.ukp.dkpro.core.tokit.BreakIteratorSegmenter
import de.tudarmstadt.ukp.dkpro.core.api.io.ResourceCollectionReaderBase
import de.tudarmstadt.ukp.dkpro.core.api.segmentation.`type`.Token

import edu.cmu.lti.suim.SparkUimaUtils._

import scala.collection.JavaConversions._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object Annotators {

  def main(args: Array[String]) = {
    val sc = new SparkContext(args(0), "App",
      System.getenv("SPARK_HOME"), System.getenv("SPARK_CLASSPATH").split(":"))

    val typeSystem = TypeSystemDescriptionFactory.createTypeSystemDescription()
    val rdd = makeRDD(createReader(classOf[TextReader],
      ResourceCollectionReaderBase.PARAM_SOURCE_LOCATION, "data/*.txt",
      ResourceCollectionReaderBase.PARAM_LANGUAGE, "en"), sc)
    val seg = createEngineDescription(classOf[BreakIteratorSegmenter])
    val tokens = rdd.map(process(_, seg)).flatMap(scas => JCasUtil.select(scas.jcas, classOf[Token]))
    val counts = tokens.map(token => token.getCoveredText()).filter(filter(_)).map((_,1)).reduceByKey(_ + _).map(pair => (pair._2, pair._1)).sortByKey(false)
    counts.take(20).foreach(println(_))
  }

  def filter(input: String): Boolean = !input.forall(_.isDigit) && input.matches("""\w*""")
}
