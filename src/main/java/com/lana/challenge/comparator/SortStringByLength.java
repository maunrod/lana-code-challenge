package com.lana.challenge.comparator;

import java.io.Serializable;
import java.util.Comparator;

public class SortStringByLength implements Comparator<String>, Serializable {
    @Override
    public int compare(String a, String b) {
        if (a.length() != b.length()) return a.length() - b.length();
        else return a.compareTo(b);
    }
}
