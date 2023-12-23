package com.github.fppt.jedismock.datastructures.streams;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;

public class SequencedMapForwardIteratorTests {
    @Test
    void createSimpleIterator() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();

        map.append(1, 2);
        map.append(2, 3);
        map.append(3, 4);

        SequencedMapIterator<Integer, Integer> it = map.iterator();

        int iterCount = 0;

        while (it.hasNext()) {
            ++iterCount;
            Map.Entry<Integer, Integer> entry = it.next();
            assertEquals(iterCount, entry.getKey());
            assertEquals(iterCount + 1, entry.getValue());
        }

        assertEquals(3, iterCount);
    }

    @Test
    void createComplexIterator() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();

        map.append(1, 2);
        map.append(2, 3);
        map.append(3, 4);
        map.append(4, 5);
        map.append(5, 6);

        SequencedMapIterator<Integer, Integer> it = map.iterator(3);

        int iterCount = 0;

        while (it.hasNext()) {
            ++iterCount;
            Map.Entry<Integer, Integer> entry = it.next();
            assertEquals(iterCount + 2, entry.getKey());
            assertEquals(iterCount + 3, entry.getValue());
        }

        assertEquals(3, iterCount);

        it = map.iterator(5);

        iterCount = 0;

        while (it.hasNext()) {
            ++iterCount;
            Map.Entry<Integer, Integer> entry = it.next();
            assertEquals(iterCount + 4, entry.getKey());
            assertEquals(iterCount + 5, entry.getValue());
        }

        assertEquals(1, iterCount);

        it = map.iterator(1);

        iterCount = 0;

        while (it.hasNext()) {
            ++iterCount;
            Map.Entry<Integer, Integer> entry = it.next();
            assertEquals(iterCount, entry.getKey());
            assertEquals(iterCount + 1, entry.getValue());
        }

        assertEquals(5, iterCount);
    }

    @Test
    void emptyMapTest() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();
        SequencedMapIterator<Integer, Integer> it = map.iterator();

        assertEquals(false, it.hasNext());
        assertThrows(NoSuchElementException.class, it::next);
    }

    @Test
    void removeTest() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();

        map.append(1, 2);
        map.append(2, 3);
        map.append(3, 4);
        map.append(4, 5);
        map.append(5, 6);

        SequencedMapIterator<Integer, Integer> it = map.iterator();

        int iterCount = 0;

        it.next();
        it.next();
        it.remove();

        while (it.hasNext()) {
            ++iterCount;
            Map.Entry<Integer, Integer> entry = it.next();
            assertEquals(iterCount + 2, entry.getKey());
            assertEquals(iterCount + 3, entry.getValue());
        }

        assertEquals(3, iterCount);
        assertEquals(4, map.size());
        assertNull(map.get(2));
    }

    @Test
    void nextWasNotInvokedTest() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();

        map.append(1, 2);
        map.append(2, 3);
        map.append(3, 4);
        map.append(4, 5);
        map.append(5, 6);

        SequencedMapIterator<Integer, Integer> it = map.iterator();

        assertThrows(IllegalStateException.class, it::remove);
    }

    @Test
    void removeHeadTest() {
        SequencedMap<Integer, Integer> map = new SequencedMap<>();

        map.append(1, 2);
        map.append(2, 3);
        map.append(3, 4);
        map.append(4, 5);
        map.append(5, 6);

        SequencedMapIterator<Integer, Integer> it = map.iterator();

        int iterCount = 0;

        it.next();
        it.remove();

        assertEquals(4, map.size());
        assertEquals(2, map.getHead());
        assertNull(map.get(1));

        while (it.hasNext()) {
            ++iterCount;
            Map.Entry<Integer, Integer> entry = it.next();
            assertEquals(iterCount + 1, entry.getKey());
            assertEquals(iterCount + 2, entry.getValue());
        }

        assertEquals(4, iterCount);
    }
}
