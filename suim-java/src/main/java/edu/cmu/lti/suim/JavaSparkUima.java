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

package edu.cmu.lti.suim;

import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.resource.ResourceInitializationException;

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

	private static final long serialVersionUID = -6881223764488277676L;
	
	private final AnalysisEngineDescription description;
    
    public PipelineFunction(AnalysisEngineDescription... descs) throws ResourceInitializationException {
      this.description = AnalysisEngineFactory.createEngineDescription(descs);
    }

    public PipelineFunction(AnalysisEngineDescription desc) {
      this.description = desc;
    }

    public SCAS call(SCAS scas) {
      return SparkUimaUtils.process(scas, description);
    }
  }
}
