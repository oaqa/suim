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

package cmu.edu.lti.suim;

import org.apache.uima.collection.CollectionReader;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.Serialization;
import org.apache.uima.jcas.JCas;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.resource.metadata.ResourceMetaData;
import org.apache.uima.examples.cpe.FileSystemCollectionReader;
import org.apache.uima.tutorial.ex1.RoomNumberAnnotator;
import org.apache.uima.tutorial.RoomNumber;
import org.apache.uima.resource.ResourceInitializationException;

import org.apache.uima.fit.factory.*;
import org.apache.uima.fit.util.JCasUtil;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import org.apache.hadoop.io.NullWritable;

import java.util.List;
import java.util.ArrayList;

public final class JavaSparkUima {

  public static JavaRDD<SCAS> sequenceFile(CollectionReader reader, String uri, JavaSparkContext sc) throws Exception {
    SparkUimaUtils.createSequenceFile(reader, uri);
    return sc.sequenceFile(uri, NullWritable.class, SCAS.class).values();
  }

  public static JavaRDD<SCAS> makeRDD(CollectionReader reader, JavaSparkContext sc) throws Exception {
    List<SCAS> buffer = SparkUimaUtils.readFrom(reader);
    return sc.parallelize(buffer);
  }

  public final static class PipelineFunction extends Function<SCAS, SCAS> {

    private final AnalysisEngineDescription description;
    
    public PipelineFunction(AnalysisEngineDescription... descs) throws ResourceInitializationException {
      this.description = AnalysisEngineFactory.createAggregateDescription(descs);
    }

    public PipelineFunction(AnalysisEngineDescription desc) {
      this.description = desc;
    }

    public SCAS call(SCAS scas) {
      return SparkUimaUtils.process(scas, description);
    }
  }
}
