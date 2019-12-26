package com.lana.challenge;

import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang.StringUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class ExtraCountersCalculator {
    static class CountExtraStats extends DoFn<String, List<KV<String, Integer>>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            List<KV<String, Integer>> counters = new ArrayList<>();
            counters.add(KV.of("ACTS", StringUtils.countMatches(c.element(), "<ACT")));
            counters.add(KV.of("SCENES", StringUtils.countMatches(c.element(),"<SCENE")));
            counters.add(KV.of("SPEECHES", StringUtils.countMatches(c.element(),"<SPEECH")));
            c.output(counters);
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setJobName("CalculateExtraCounters");
        options.setRunner(SparkRunner.class);
        Pipeline p = Pipeline.create(options);

        PCollection<String> pCollection = p
                // .apply("ReadFromStorage", TextIO.read().from("s3://lana-code-challenge/in/ShakespearePlaysPlus/historical/*_characters/*"))
                .apply("ReadFromStorage", TextIO.read().from("s3://lana-code-challenge/in/ShakespearePlaysPlus/historical/The Life and Death of King John_characters/AUSTRIA.txt"))
                .apply("CalculateCounters", ParDo.of(new CountExtraStats()))
                .apply("FlattenList", Flatten.iterables())
                .apply("AggregateCounters", Sum.integersPerKey())
                .apply("FormatResult", MapElements.into(TypeDescriptors.strings()).via((KV<String, Integer> counter) -> String.format("%s: %d", counter.getKey(), counter.getValue())))
                .apply("WriteResults", MapElements.into(TypeDescriptors.strings()).via((String tokenStat) -> {
                    System.out.println(tokenStat);
                    TextIO.write().to("s3://lana-code-challenge/out/" + Instant.now().toEpochMilli() + "/");
                    return tokenStat;
                }));
        p.run().waitUntilFinish();
    }
}
