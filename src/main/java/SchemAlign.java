import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import com.integration.em.aggregator.AlignedAggregator;
import com.integration.em.blocker.*;
import com.integration.em.matches.InstanceBasedSchemaMatching;
import com.integration.em.matches.MAtchingEngine;
import com.integration.em.model.*;
import com.integration.em.model.Record;
import com.integration.em.processing.DataSetNormalizer;
import com.integration.em.processing.Processable;
import com.integration.em.rules.compare.IComparatorLogger;
import com.integration.em.tables.PatternbasedTypeDetector;

public class SchemAlign {

    public static void main(String args[]) throws Exception {
        DataSet<Record, Attribute> data1 = new HashDataSet<>();

        new CSVRecordReader(0).loadFromCSV(new File("funddemographicstw.csv"), data1);
        DataSet<Record, Attribute> data2 = new HashDataSet<>();
        new CSVRecordReader(0).loadFromCSV(new File("funddemographicshk.csv"), data2);

       //new DataSetNormalizer<Record>().normalizeDataset(data1, new PatternbasedTypeDetector());

        MAtchingEngine<Record, Attribute> engine = new MAtchingEngine<>();


        ValueBasedBlocker<Record, Attribute, Attribute> blocker = new ValueBasedBlocker<>(new DefaultAttributeValueGenerator(data2.getSchema()),new DefaultAttributeValueGenerator(data1.getSchema()));

        System.out.println(data1.getSchema());
//        IComparatorLogger iComparatorLogger=new IComparatorLogger();
//        iComparatorLogger.setComparatorName("Label");
        Collection<Attribute> attributes1=data1.getSchema().get();
        Collection<Attribute> attributes2=data2.getSchema().get();

        Set<String> set1 = attributes1.stream().map(att -> att.getName()).collect(Collectors.toSet());
        Set<String> set2 = attributes2.stream().map(att -> att.getName()).collect(Collectors.toSet());


        LabelComparatorJaccard labelComparatorJaccard=new LabelComparatorJaccard();
        Processable<Aligner<Attribute, Attribute>> alignerProcessable
                = engine.runLabelBasedSchemaMatching(data1.getSchema(), data2.getSchema(), labelComparatorJaccard, 0);
        alignerProcessable.get().stream().forEach(al ->{
            System.out.println(String.format("'%s' <-> '%s' (%.4f)",
                    al.getFirstRecordType().getName(),
                    al.getSecondRecordType().getName(),
                    al.getSimilarityScore()));
            set1.remove( al.getFirstRecordType().getName());
            set2.remove( al.getSecondRecordType().getName());
        });
        System.out.println("--- DataSet 1 UnMapped ----");
        set1.stream().forEach( d1 ->{
            System.out.println(String.format("'%s' X '%s' ",d1," ----"));
        });

        System.out.println("--- DataSet 2 UnMapped ----");
        set1.stream().forEach( d2 ->{
            System.out.println(String.format("'%s' X '%s' ",d2," ----"));
        });

//        for(Aligner<Attribute, Attribute> aligner : alignerProcessable.get()) {
//            System.out.println(String.format("'%s' <-> '%s' (%.4f)",
//                    aligner.getFirstRecordType().getName(),
//                    aligner.getSecondRecordType().getName(),
//                    aligner.getSimilarityScore()));
//        }

//        InstanceBasedSchemaMatching<Record, Attribute> algo = new InstanceBasedSchemaMatching<>(data2, data1, blocker, new AlignedAggregator<>(0.0));
//        algo.run();
//        Processable<Aligner<Attribute, MatchableValue>> algoResult = algo.getResult();
//        for(Aligner<Attribute, MatchableValue> cor : algoResult.get()) {
//            System.out.println(String.format("%s <-> %s (%.6f)", cor.getFirstRecordType(), cor.getSecondRecordType(), cor.getSimilarityScore()));
//        }

    }
}
