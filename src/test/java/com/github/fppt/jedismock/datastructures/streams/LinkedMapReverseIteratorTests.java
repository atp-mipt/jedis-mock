package com.github.fppt.jedismock.datastructures.streams;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LinkedMapReverseIteratorTests {
    @Test
    void createSimpleIterator() {
        LinkedMap<Integer, Integer> map = new LinkedMap<>();

        map.append(3, 4);
        map.append(2, 3);
        map.append(1, 2);

        LinkedMapIterator<Integer, Integer> it = map.reverseIterator();

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

        map.append(5, 6);
        map.append(4, 5);
        map.append(3, 4);
        map.append(2, 3);
        map.append(1, 2);

        LinkedMapIterator<Integer, Integer> it = map.reverseIterator(3);

        int iterCount = 0;

        while (it.hasNext()) {
            ++iterCount;
            Map.Entry<Integer, Integer> entry = it.next();
            assertEquals(iterCount + 2, entry.getKey());
            assertEquals(iterCount + 3, entry.getValue());
        }

        assertEquals(3, iterCount);

        it = map.reverseIterator(5);

        iterCount = 0;

        while (it.hasNext()) {
            ++iterCount;
            Map.Entry<Integer, Integer> entry = it.next();
            assertEquals(iterCount + 4, entry.getKey());
            assertEquals(iterCount + 5, entry.getValue());
        }

        assertEquals(1, iterCount);

        it = map.reverseIterator(1);

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
        LinkedMapIterator<Integer, Integer> it = map.reverseIterator();

        assertFalse(it.hasNext());
        assertThrows(NoSuchElementException.class, it::next);
    }

    @Test
    void findFirstWithEmptyMapTest() {
        LinkedMap<Integer, Integer> map = new LinkedMap<>();
        LinkedMapIterator<Integer, Integer> it = map.reverseIterator();

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

        LinkedMapIterator<Integer, Integer> it = map.reverseIterator();

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

        map.append(100, 101);
        map.append(5, 6);
        map.append(6, 7);
        map.append(2, 3);
        map.append(1, 2);

        LinkedMapIterator<Integer, Integer> it = map.reverseIterator();

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

        LinkedMapIterator<Integer, Integer> it = map.reverseIterator();

        it.findFirstSuitable(3);
        assertTrue(it.hasNext());

        Map.Entry<Integer, Integer> entry = it.next();

        assertEquals(2, entry.getKey());
        assertEquals(3, entry.getValue());

        it.findFirstSuitable(99);
        assertTrue(it.hasNext());

        entry = it.next();

        assertEquals(6, entry.getKey());
        assertEquals(7, entry.getValue());

        it.findFirstSuitable(Integer.MAX_VALUE);
        assertTrue(it.hasNext());

        entry = it.next();

        assertEquals(100, entry.getKey());
        assertEquals(101, entry.getValue());
    }
}
