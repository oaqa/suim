package cmu.edu.lti.bagpipes.spark;

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

import spark.api.java.*;
import spark.api.java.function.*;

import java.util.List;
import java.util.ArrayList;

public final class JavaSparkUima {

  public static JavaRDD<SCAS> makeRDD(CollectionReader reader, JavaSparkContext sc) throws Exception {
    List<SCAS> buffer = new ArrayList<SCAS>();
    while (reader.hasNext()) {
      JCas jcas = JCasFactory.createJCas();
      CAS cas = jcas.getCas();
      reader.getNext(cas);
      buffer.add(new SCAS(cas));
    }
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
