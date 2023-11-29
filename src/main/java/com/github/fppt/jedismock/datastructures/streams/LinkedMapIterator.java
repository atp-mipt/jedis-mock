package com.github.fppt.jedismock.datastructures.streams;

import java.util.Iterator;
import java.util.Map;

public interface LinkedMapIterator<K extends Comparable<K>, V> extends Iterator<Map.Entry<K, V>> {
    @Override
    Map.Entry<K, V> next();

    void findFirstSuitable(K border);
}
