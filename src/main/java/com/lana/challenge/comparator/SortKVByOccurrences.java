package com.lana.challenge.comparator;

import org.apache.beam.sdk.values.KV;

import java.io.Serializable;
import java.util.Comparator;

public class SortKVByOccurrences implements Comparator<KV<String, Long>>, Serializable {
    @Override
    public int compare(KV<String, Long> a, KV<String, Long> b) {
        return Long.compare(a.getValue(), b.getValue());
    }
}

