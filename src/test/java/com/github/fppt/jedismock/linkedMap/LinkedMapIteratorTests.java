package com.github.fppt.jedismock.linkedMap;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class LinkedMapIteratorTests {
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
            assertEquals(iterCount + 3, entry.getKey());
            assertEquals(iterCount + 4, entry.getValue());
        }

        assertEquals(2, iterCount);
        assertEquals(4, map.size());
        assertNull(map.get(3));
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
