package com.lana.challenge;

import com.lana.challenge.comparator.SortStringByLength;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class LongestSentencesCalculator {
    static class CleanSentences extends DoFn<String, List<String>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            List<String> cleanedSentences = Arrays.stream((c.element() + " ").split("\\<.*?\\>")) // split by tags to avoid mixing sentences from different sections
                    .map(e -> e.replaceAll("\\n\\r|\\n|\\r", " ").replaceAll("\\s{2,}", " ")) // replace CRLF characters and multiples spaces
                    .map(e -> Arrays.stream(e.split("\\!")).collect(Collectors.toList())) // split by !
                    .flatMap(e -> {
                        if (e.size() == 1) return e.stream();
                        else {
                            List<String> formattedList = new ArrayList<>();
                            for (int i = 0; i <= e.size() - 2; i++) formattedList.add(e.get(i) + "!");
                            formattedList.add(e.get(e.size()-1));
                            return formattedList.stream();
                        }
                    }) // add ! at the end of each sentence to keep the original punctuation and meaning/sense of the line
                    .map(e -> Arrays.stream(e.split("\\?")).collect(Collectors.toList())) // split by ?
                    .flatMap(e -> {
                        if (e.size() == 1) return e.stream().map(String::trim);
                        else {
                            List<String> formattedList = new ArrayList<>();
                            for (int i = 0; i <= e.size() - 2; i++) formattedList.add((e.get(i) + "?").trim());
                            formattedList.add(e.get(e.size()-1).trim());
                            return formattedList.stream();
                        }
                    }) // add ? at the end of each sentence to keep the original punctuation and meaning/sense of the line
                    .filter(e -> !e.isEmpty()) // remove empty sentences
                    .collect(Collectors.toList());
            c.output(cleanedSentences);
        }
    }

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        options.setJobName("CalculateLongestSentences");
        options.setRunner(SparkRunner.class);
        Pipeline p = Pipeline.create(options);

        PCollection<String> pCollection = p
                .apply("ReadFromStorage", TextIO.read().from("s3://lana-code-challenge/in/ShakespearePlaysPlus/**/*_characters/*").withDelimiter(new byte[] {'.'}))
                // .apply("ReadFromStorage", TextIO.read().from("s3://lana-code-challenge/in/ShakespearePlaysPlus/historical/The Life and Death of King John_characters/AUSTRIA.txt").withDelimiter(new byte[] {'.'}))
                .apply("CleanSentences", ParDo.of(new CleanSentences()))
                .apply("FlattenList", Flatten.iterables())
                .apply("Top5", Top.of(5, new SortStringByLength()))
                .apply("FormatResult", FlatMapElements.into(TypeDescriptors.strings()).via((List<String> sentences) -> sentences.stream().map(e -> String.format("%s: %d", e, e.length())).collect(Collectors.toList())))
                .apply("WriteResults", MapElements.into(TypeDescriptors.strings()).via((String sentenceStat) -> {
                    System.out.println(sentenceStat);
                    TextIO.write().to("s3://lana-code-challenge/out/" + Instant.now().toEpochMilli() + "/");
                    return sentenceStat;
                }));
        p.run().waitUntilFinish();
    }
}
