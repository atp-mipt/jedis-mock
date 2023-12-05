package com.github.fppt.jedismock.datastructures.streams;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;

public class LinkedMapForwardIteratorTests {
    @Test
    void createSimpleIterator() {
        LinkedMap<Integer, Integer> map = new LinkedMap<>();

        map.append(1, 2);
        map.append(2, 3);
        map.append(3, 4);

        LinkedMapIterator<Integer, Integer> it = map.iterator();

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
        LinkedMap<Integer, Integer> map = new LinkedMap<>();

        map.append(1, 2);
        map.append(2, 3);
        map.append(3, 4);
        map.append(4, 5);
        map.append(5, 6);

        LinkedMapIterator<Integer, Integer> it = map.iterator(3);

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
        LinkedMap<Integer, Integer> map = new LinkedMap<>();
        LinkedMapIterator<Integer, Integer> it = map.iterator();

        assertEquals(false, it.hasNext());
        assertThrows(NoSuchElementException.class, it::next);
    }

    @Test
    void removeTest() {
        LinkedMap<Integer, Integer> map = new LinkedMap<>();

        map.append(1, 2);
        map.append(2, 3);
        map.append(3, 4);
        map.append(4, 5);
        map.append(5, 6);

        LinkedMapIterator<Integer, Integer> it = map.iterator();

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
        LinkedMap<Integer, Integer> map = new LinkedMap<>();

        map.append(1, 2);
        map.append(2, 3);
        map.append(3, 4);
        map.append(4, 5);
        map.append(5, 6);

        LinkedMapIterator<Integer, Integer> it = map.iterator();

        assertThrows(IllegalStateException.class, it::remove);
    }

    @Test
    void removeHeadTest() {
        LinkedMap<Integer, Integer> map = new LinkedMap<>();

        map.append(1, 2);
        map.append(2, 3);
        map.append(3, 4);
        map.append(4, 5);
        map.append(5, 6);

        LinkedMapIterator<Integer, Integer> it = map.iterator();

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

    @Test
    void findFirstWithEmptyMapTest() {
        LinkedMap<Integer, Integer> map = new LinkedMap<>();
        LinkedMapIterator<Integer, Integer> it = map.iterator();

        it.findFirstSuitable(7);
        assertFalse(it.hasNext());

        it.findFirstSuitable(0);
        assertFalse(it.hasNext());
    }

    @Test
    void findFirstWhenKeyExistsTest() {
        LinkedMap<Integer, Integer> map = new LinkedMap<>();

        map.append(1, 2);
        map.append(2, 3);
        map.append(3, 4);
        map.append(4, 5);
        map.append(5, 6);

        LinkedMapIterator<Integer, Integer> it = map.iterator();

        it.findFirstSuitable(3);
        assertTrue(it.hasNext());

        Map.Entry<Integer, Integer> entry = it.next();

        assertEquals(3, entry.getKey());
        assertEquals(4, entry.getValue());

        it.findFirstSuitable(5);
        assertTrue(it.hasNext());

        entry = it.next();

        assertEquals(5, entry.getKey());
        assertEquals(6, entry.getValue());

        it.findFirstSuitable(1);
        assertTrue(it.hasNext());

        entry = it.next();

        assertEquals(1, entry.getKey());
        assertEquals(2, entry.getValue());
    }

    @Test
    void findFirstWithNullBorderTest() {
        LinkedMap<Integer, Integer> map = new LinkedMap<>();

        map.append(1, 2);
        map.append(2, 3);
        map.append(5, 6);
        map.append(6, 7);
        map.append(100, 101);

        LinkedMapIterator<Integer, Integer> it = map.iterator();

        assertThrows(NullPointerException.class, () -> it.findFirstSuitable(null));
    }

    @Test
    void findFirstWhenKeyDoesNotExistTest() {
        LinkedMap<Integer, Integer> map = new LinkedMap<>();

        map.append(1, 2);
        map.append(2, 3);
        map.append(5, 6);
        map.append(6, 7);
        map.append(100, 101);

        LinkedMapIterator<Integer, Integer> it = map.iterator();

        it.findFirstSuitable(3);
        assertTrue(it.hasNext());

        Map.Entry<Integer, Integer> entry = it.next();

        assertEquals(5, entry.getKey());
        assertEquals(6, entry.getValue());

        it.findFirstSuitable(7);
        assertTrue(it.hasNext());

        entry = it.next();

        assertEquals(100, entry.getKey());
        assertEquals(101, entry.getValue());

        it.findFirstSuitable(Integer.MIN_VALUE);
        assertTrue(it.hasNext());

        entry = it.next();

        assertEquals(1, entry.getKey());
        assertEquals(2, entry.getValue());
    }

    @Test
    void findFirstSuitableFuzzingTest() {
        LinkedMap<Integer, Integer> map = new LinkedMap<>();
        LinkedMapForwardIterator<Integer, Integer> it = map.iterator();

        for (int i = 0; i < 5001; i += 5) {
            map.append(i, i + 3);
        }

        for (int i = 0; i < 1000; ++i) {
            int key = (int) (Math.random() * 5000);
            int correctKey = key + (key % 5 == 0 ? 0 : 5 - key % 5);
            it.findFirstSuitable(key);
            Map.Entry<Integer, Integer> entry = it.next();
            assertEquals(correctKey, entry.getKey());
            assertEquals(correctKey + 3, entry.getValue());
        }
    }
}
