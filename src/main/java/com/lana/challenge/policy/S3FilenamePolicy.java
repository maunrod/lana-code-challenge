package com.lana.challenge.policy;

import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import javax.annotation.Nullable;
import java.time.Instant;

public class S3FilenamePolicy extends FileBasedSink.FilenamePolicy {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyyMMddHHmmss").withZone(DateTimeZone.UTC);
    private final ResourceId s3OutPrefixWindowed;
    private final ResourceId s3OutPrefixUnwindowed;
    private final String statName;
    private final String extension;

    public S3FilenamePolicy(String s3OutPrefix, String statName, String extension) {
        String commonS3OutPrefix = (s3OutPrefix.endsWith("/") ? s3OutPrefix : s3OutPrefix + "/") + statName + "/";
        this.s3OutPrefixWindowed = FileBasedSink.convertToFileResourceIfPossible(commonS3OutPrefix);
        this.s3OutPrefixUnwindowed = FileBasedSink.convertToFileResourceIfPossible(commonS3OutPrefix + Instant.now().toEpochMilli() + "/");
        this.statName = statName;
        this.extension = "." + extension.toLowerCase();
    }

    public S3FilenamePolicy(String s3OutPrefix, String statName) {
        String commonS3OutPrefix = (s3OutPrefix.endsWith("/") ? s3OutPrefix : s3OutPrefix + "/") + statName + "/";
        this.s3OutPrefixWindowed = FileBasedSink.convertToFileResourceIfPossible(commonS3OutPrefix);
        this.s3OutPrefixUnwindowed = FileBasedSink.convertToFileResourceIfPossible(commonS3OutPrefix + Instant.now().toEpochMilli() + "/");        this.statName = statName;
        this.extension = "";
    }

    @Override
    public ResourceId windowedFilename(int shardNumber, int numShards, BoundedWindow window, PaneInfo paneInfo, FileBasedSink.OutputFileHints outputFileHints) {
        IntervalWindow intervalWindow = (IntervalWindow) window;
        String filename =
                String.format("%s_%s-%s_%s-of-%s%s%s",
                        this.statName,
                        DATE_TIME_FORMATTER.print(intervalWindow.start()),
                        DATE_TIME_FORMATTER.print(intervalWindow.end()),
                        shardNumber,
                        numShards,
                        outputFileHints.getSuggestedFilenameSuffix(),
                        this.extension).trim();
        return this.s3OutPrefixWindowed.resolve(filename, StandardResolveOptions.RESOLVE_FILE);
    }

    @Nullable
    @Override
    public ResourceId unwindowedFilename(int shardNumber, int numShards, FileBasedSink.OutputFileHints outputFileHints) {
        String filename =
                String.format("%s_%s-of-%s%s%s",
                        this.statName,
                        shardNumber,
                        numShards,
                        outputFileHints.getSuggestedFilenameSuffix(),
                        this.extension).trim();
        return this.s3OutPrefixUnwindowed.resolve(filename, StandardResolveOptions.RESOLVE_FILE);
    }
}

