/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import org.apache.uima.tutorial.UimaAcronym
import org.apache.uima.tutorial.Meeting
import org.apache.uima.tutorial.UimaMeeting

import org.apache.uima.fit.factory._
import org.apache.uima.fit.factory.AnalysisEngineFactory._
import org.apache.uima.fit.util.JCasUtil._
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;

import cmu.edu.lti.bagpipes.spark.SparkUimaUtils._

import scala.collection.JavaConversions._

import java.util.StringTokenizer

import spark.SparkContext
import spark.SparkContext._

import scala.io.Source

object SparkPipelineExample {

  def readMap(file: String) = {
    val s = Source.fromFile(file)
    s.getLines.map(line => {
      val pair = line.split("\t")
      (pair(0), pair(1))
    }).toMap
  }

  def main(args: Array[String]) = {
    val sc = new SparkContext(args(0), "App",
      System.getenv("SPARK_HOME"), System.getenv("SPARK_CLASSPATH").split(":"))

    // Share variable
    val mMap = sc.broadcast(readMap("src/main/resources/org/apache/uima/tutorial/ex6/uimaAcronyms.txt"))
    val typeSystem = TypeSystemDescriptionFactory.createTypeSystemDescription()
    val params = Seq(FileSystemCollectionReader.PARAM_INPUTDIR, "data")
    val rdd = makeRDD(CollectionReaderFactory.createCollectionReader(
      classOf[FileSystemCollectionReader], params: _*), sc)
    val result = rdd.map(process(_, createAggregateDescription(
      createEngineDescription(classOf[UimaAcronymAnnotator]),
      createEngineDescription(classOf[UimaMeetingAnnotator])))).cache
    result.flatMap(scas => select(scas.jcas, classOf[UimaAcronym])).foreach(println(_))
    result.flatMap(scas => select(scas.jcas, classOf[UimaMeeting])).foreach(println(_))
  }
}

class UimaAcronymAnnotator extends JCasAnnotator_ImplBase {

  val mMap = spark.SparkEnv.get.blockManager.getSingle("broadcast_0").get.asInstanceOf[Map[String, String]]

  override def process(jcas: JCas) {
     // go through document word-by-word
    val text = jcas.getDocumentText();
    var pos = 0;
    val tokenizer = new StringTokenizer(text, """ \t\n\r.<.>/?";:[{]}\|=+()!""", true);
    while (tokenizer.hasMoreTokens()) {
      val token = tokenizer.nextToken();
      // look up token in map to see if it is an acronym
      val expandedForm = mMap.get(token);
      if (expandedForm.isDefined) {
        // create annotation
        val annot = new UimaAcronym(jcas, pos, pos + token.length());
        annot.setExpandedForm(expandedForm.get);
        annot.addToIndexes();
      }
      // incrememnt pos and go to next token
      pos += token.length();
    }
  }
}


class UimaMeetingAnnotator extends JCasAnnotator_ImplBase {

  val mMap = spark.SparkEnv.get.blockManager.getSingle("broadcast_0").get.asInstanceOf[Map[String, String]]

  override def process(jcas: JCas) {
    // get document text
    val text = jcas.getDocumentText();

    // We iterate over all Meeting annotations, and if we determine that
    // the topic of a meeting is UIMA-related, we create a UimaMeeting
    // annotation. We add each UimaMeeting annotation to a list, and then
    // later go back and add these to the CAS indexes. We need to do this
    // because it's not allowed to add to an index that you're currently
    // iterating over.
    val uimaMeetings = scala.collection.mutable.Buffer[UimaMeeting]()

    select(jcas, classOf[Meeting]).foreach(meeting => {
      // get span of text within 50 chars on either side of meeting
      // (window size should probably be a config. param)
      var begin = meeting.getBegin() - 50
      var end = meeting.getEnd() + 50
      if (begin < 0) {
        begin = 0
      }
      if (end > text.length()) {
        end = text.length()
      }
      val window = text.substring(begin, end)

      // look for UIMA acronyms within this window
      val tokenizer = new StringTokenizer(window, """ \t\n\r.<.>/?";:[{]}\|=+()!""");
      var continue = true
      while (tokenizer.hasMoreTokens() && continue) {
        val token = tokenizer.nextToken();
        // look up token in map to see if it is an acronym
        if (mMap.get(token) != null) {
          // create annotation
          val annot = new UimaMeeting(jcas, meeting.getBegin(), meeting.getEnd());
          annot.setRoom(meeting.getRoom());
          annot.setDate(meeting.getDate());
          annot.setStartTime(meeting.getStartTime());
          annot.setEndTime(meeting.getEndTime());
          // Add annotation to a list, to be later added to the
          // indexes.
          // We need to do this because it's not allowed to add to an
          // index that you're currently iterating over.
          uimaMeetings.add(annot);
          continue = false
        }
      }
    })
    uimaMeetings.foreach(meeting => meeting.addToIndexes())
  }
}
