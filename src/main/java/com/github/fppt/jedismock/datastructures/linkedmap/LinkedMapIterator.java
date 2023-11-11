package com.github.fppt.jedismock.datastructures.linkedmap;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Iterator for {@link LinkedMap LinkedMap}
 *
 * @param <K> keys type, must implement {@link java.lang.Comparable Comparable}
 * @param <V> values type
 */
public class LinkedMapIterator<K extends Comparable<K>, V> implements Iterator<Map.Entry<K, V>> {
    /**
     * Iterator takes place before this key. If is {@code null} then iterator takes place before the head of the map.
     */
    private K curr;

    /**
     * Map that iterator refers to
     */
    private final LinkedMap<K, V> map;

    public LinkedMapIterator(K curr, LinkedMap<K, V> map) {
        this.curr = curr;
        this.map = map;
    }

    @Override
    public boolean hasNext() {
        if (curr == null) {
            return map.getHead() != null;
        }

        return map.getNextKey(curr) != null;
    }

    @Override
    public Map.Entry<K, V> next() {
        if (!hasNext()) {
            throw new NoSuchElementException("There is no elements left");
        }

        if (curr == null) {
            curr = map.getHead();
        } else {
            curr = map.getNextKey(curr);
        }

        return Map.entry(curr, map.get(curr));
    }

    @Override
    public void remove() {
        if (curr == null) {
            map.remove(map.getHead());
        } else {
            map.remove(map.getNextKey(curr));
        }
    }
}
