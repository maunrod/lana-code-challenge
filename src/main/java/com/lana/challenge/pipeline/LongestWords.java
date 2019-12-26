package com.lana.challenge.pipeline;

import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.lana.challenge.comparator.SortStringByLength;
import com.lana.challenge.policy.S3FilenamePolicy;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.lana.challenge.utils.Utils.*;

public class LongestWords {
    /**
     * Calculate N longest words in a set of files stored in given S3 path
     */
    private static final Logger LOG = LoggerFactory.getLogger(LongestWords.class);
    private static class ExtractWords extends DoFn<String, List<String>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String lineCleaned = c.element().replaceAll("\\<.*?\\>", "").trim().toLowerCase(); // remove tags
            if (!lineCleaned.isEmpty()) c.output(Arrays.asList(lineCleaned.split("[^\\p{L}]+"))); // return list of tokens parsed without empty values
        }
    }

    public static void main(String[] args) {
        // -------------------------
        // GET ENVIRONMENT VARIABLES
        // -------------------------
        // max number of lines (statistics) to show
        int maxOutputLines = DEFAULT_MAX_OUTPUT_LINES;
        try {
            maxOutputLines = System.getenv("MAX_OUTPUT_LINES") == null ? maxOutputLines : Integer.parseInt(System.getenv("MAX_OUTPUT_LINES"));
        } catch (NumberFormatException ex) {
            LOG.warn(String.format("Could not parse environment variable MAX_OUTPUT_LINES. Using default value %d [Exception: %s]", maxOutputLines, ex.getMessage()));
        }

        // S3 out prefix
        String s3OutPrefix = System.getenv("S3_OUT_PREFIX") == null ? DEFAULT_S3_OUT_PREFIX : System.getenv("S3_OUT_PREFIX");

        // S3 search pattern
        String s3SearchPattern = System.getenv("S3_SEARCH_PATTERN") == null ? DEFAULT_S3_SEARCH_PATTERN : System.getenv("S3_SEARCH_PATTERN");

        // S3 temporal prefix
        String s3TmpPrefix = System.getenv("S3_TMP_PREFIX") == null ? DEFAULT_S3_TMP_PREFIX : (System.getenv("S3_TMP_PREFIX").endsWith("/") ? System.getenv("S3_TMP_PREFIX") : System.getenv("S3_TMP_PREFIX") + "/");

        // ----------------
        // PIPELINE OPTIONS
        // ----------------
        // runner configuration
        S3Options options = PipelineOptionsFactory.as(S3Options.class);
        options.setJobName("CalculateLongestWords");
        options.setRunner(SPARK_RUNNER);

        // AWS configuration
        options.setAwsCredentialsProvider(new EnvironmentVariableCredentialsProvider());
        options.setAwsRegion(System.getenv("AWS_DEFAULT_REGION") == null ? DEFAULT_AWS_REGION : System.getenv("AWS_DEFAULT_REGION").toLowerCase());
        options.setAwsServiceEndpoint(String.format("https://s3.%s.amazonaws.com", options.getAwsRegion()));

        // -------------------
        // PIPELINE DEFINITION
        // -------------------
        Pipeline p = Pipeline.create(options);
        ResourceId tmpResourceId = FileBasedSink.convertToFileResourceIfPossible(s3TmpPrefix);

        p.apply("ReadFromStorage", TextIO.read().from(s3SearchPattern))
                .apply("ExtractWords", ParDo.of(new ExtractWords()))
                .apply("FlattenList", Flatten.iterables())
                .apply("FilterUniques", Distinct.create())
                .apply("TopN", Top.of(maxOutputLines, new SortStringByLength()))
                .apply("FormatResult", FlatMapElements.into(TypeDescriptors.strings()).via((List<String> wordLength) -> wordLength.stream().map(e -> String.format("%s;%d", e, e.length())).collect(Collectors.toList())))
                .apply("PrintResult", MapElements.into(TypeDescriptors.strings()).via((String wordLength) -> {
                    LOG.info(wordLength);
                    return wordLength;
                }))
                .apply("WriteResultToStorage", TextIO.write().to(new S3FilenamePolicy(s3OutPrefix, options.getJobName(), "csv")).withTempDirectory(tmpResourceId).withHeader("word;length"));

        // ------------------
        // PIPELINE EXECUTION
        // ------------------
        // wait until the pipeline is completed and show the execution status
        PipelineResult.State pipelineState = p.run().waitUntilFinish();
        LOG.info(pipelineState.name());    }
}


