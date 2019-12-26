package com.lana.challenge.utils;

import org.apache.beam.runners.spark.SparkRunner;

public final class Utils {
    public static final Class<SparkRunner> SPARK_RUNNER = SparkRunner.class;
    public static final int DEFAULT_MAX_OUTPUT_LINES = 5;
    public static final String DEFAULT_AWS_REGION = "us-east-2";
    public static final String DEFAULT_S3_SEARCH_PATTERN= "s3://lana-code-challenge/in/ShakespearePlaysPlus/**/*_characters/*";
    public static final String DEFAULT_S3_OUT_PREFIX = "s3://lana-code-challenge/out/";
    public static final String DEFAULT_S3_TMP_PREFIX = "s3://lana-code-challenge/tmp/";

}