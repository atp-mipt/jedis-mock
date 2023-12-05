package com.github.fppt.jedismock.datastructures.streams;


import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LinkedMapTests {
    @Test
    void initializeMapTest() {
        LinkedMap<Integer, Integer> map = new LinkedMap<>();

        assertNull(map.getHead());
        assertNull(map.getTail());
        assertEquals(0, map.size());
    }

    @Test
    void addPairsToMapTest() {
        LinkedMap<Integer, Integer> map = new LinkedMap<>();

        map.append(1, 2);
        assertEquals(1, map.getHead());
        assertEquals(1, map.getTail());
        assertEquals(1, map.size());
        assertEquals(2, map.get(1));
        assertNull(map.get(0));
    }

    @Test
    void addSequenceToMapTest() {
        LinkedMap<Integer, Integer> map = new LinkedMap<>();

        map.append(1, 2);
        map.append(2, 2);
        map.append(3, 2);
        map.append(4, 2);

        map.append(4, 2);
        map.append(0, 2);
        map.append(2, 2);
    }

    @Test
    void removePairsFromMapTest() {
        LinkedMap<Integer, Integer> map = new LinkedMap<>();

        map.append(1, 2);

        Map.Entry<Integer, Integer> entry = map.remove(0);

        assertNull(entry);
        assertEquals(1, map.getHead());
        assertEquals(1, map.getTail());
        assertEquals(1, map.size());
        assertEquals(2, map.get(1));

        entry = map.remove(1);

        assertEquals(1, entry.getKey());
        assertEquals(2, entry.getValue());

        assertNull(map.getHead());
        assertNull(map.getTail());
        assertEquals(0, map.size());
    }

    @Test
    void getNodeTest() {
        LinkedMap<Integer, Integer> map = new LinkedMap<>();

        map.append(1, 2);
        map.append(2, 3);
        map.append(3, 4);

        assertThrows(NullPointerException.class, () -> map.getNextKey(0));
        assertEquals(2, map.getNextKey(1));
        assertEquals(3, map.getNextKey(2));
        assertNull(map.getNextKey(3));

        assertThrows(NullPointerException.class, () -> map.getPreviousKey(0));
        assertNull(map.getPreviousKey(1));
        assertEquals(1, map.getPreviousKey(2));
        assertEquals(2, map.getPreviousKey(3));
    }

    @Test
    void setNextNodeTest() {
        LinkedMap<Integer, Integer> map = new LinkedMap<>();

        map.append(1, 2);
        map.append(2, 3);
        map.append(3, 4);

        assertThrows(NullPointerException.class, () -> map.setNextKey(0, 1));
        assertThrows(NullPointerException.class, () -> map.setNextKey(1, 0));

        map.setNextKey(1, 3);

        assertEquals(3, map.getNextKey(1));
        assertEquals(3, map.getNextKey(2));
        assertNull(map.getNextKey(3));

        assertNull(map.getPreviousKey(1));
        assertEquals(1, map.getPreviousKey(2));
        assertEquals(2, map.getPreviousKey(3));
    }

    @Test
    void setPreviousNodeTest() {
        LinkedMap<Integer, Integer> map = new LinkedMap<>();

        map.append(1, 2);
        map.append(2, 3);
        map.append(3, 4);

        assertThrows(NullPointerException.class, () -> map.setPreviousKey(0, 1));
        assertThrows(NullPointerException.class, () -> map.setPreviousKey(1, 0));

        map.setPreviousKey(3, 1);

        assertEquals(2, map.getNextKey(1));
        assertEquals(3, map.getNextKey(2));
        assertNull(map.getNextKey(3));

        assertNull(map.getPreviousKey(1));
        assertEquals(1, map.getPreviousKey(2));
        assertEquals(1, map.getPreviousKey(3));
    }

    @Test
    void addRemoveStressTest() {
        LinkedMap<Integer, Integer> map = new LinkedMap<>();

        map.append(0, 0);

        IntStream.range(1, 1_000_001).forEach(el -> {
            assertEquals(el - 1, map.getTail());
            assertEquals(0, map.getHead());
            map.append(el, el * 2);
            assertEquals(el, map.getTail());
            assertEquals(0, map.getHead());

            assertEquals(el + 1, map.size());
        });

        IntStream.range(1, 500_000).forEach(el -> {
            assertEquals(1_000_000, map.getTail());
            assertEquals(0, map.getHead());
            map.remove(el);
            assertEquals(1_000_000, map.getTail());
            assertEquals(0, map.getHead());

            assertEquals(1_000_001 - el, map.size());
        });

        IntStream.range(0, 249_999).map(el -> 1_000_000 - el).forEach(el -> {
            assertEquals(el, map.getTail());
            assertEquals(0, map.getHead());
            map.remove(el);
            assertEquals(el - 1, map.getTail());
            assertEquals(0, map.getHead());

            assertEquals(el - 499_999, map.size());
        });

        Map.Entry<Integer, Integer> entry = map.remove(0);

        assertEquals(0, entry.getKey());
        assertEquals(0, entry.getValue());
        assertEquals(250_002, map.size());

        IntStream.range(500_000, 749_999).forEach(el -> {
            assertEquals(750_001, map.getTail());
            assertEquals(el, map.getHead());

            Map.Entry<Integer, Integer> localEntry = map.remove(el);
            assertEquals(el, localEntry.getKey());
            assertEquals(el * 2, localEntry.getValue());

            assertEquals(750_001, map.getTail());
            assertEquals(el + 1, map.getHead());

            assertEquals( 750_001 - el, map.size());
        });
    }

    @Test
    void removeHeadTest() {
        LinkedMap<Integer, Integer> map = new LinkedMap<>();

        assertDoesNotThrow(map::removeHead);
        assertEquals(0, map.size());
        assertNull(map.getHead());

        map.append(0, 0);
        map.append(1, 0);
        map.append(2, 0);

        assertDoesNotThrow(map::removeHead);
        assertEquals(2, map.size());
        assertEquals(1, map.getHead());

        assertDoesNotThrow(map::removeHead);
        assertEquals(1, map.size());
        assertEquals(2, map.getHead());

        assertDoesNotThrow(map::removeHead);
        assertEquals(0, map.size());
        assertNull(map.getHead());
    }

    @Test
    void forEachTest() {
        LinkedMap<Integer, Integer> map = new LinkedMap<>();

        map.append(0, 9);
        map.append(1, 8);
        map.append(2, 7);
        map.append(3, 6);
        map.append(4, 5);
        map.append(5, 4);
        map.append(6, 3);
        map.append(7, 2);
        map.append(8, 1);
        map.append(9, 0);

        List<Integer> list = new ArrayList<>();

        map.forEach((key, value) -> {
            list.add(key + value);
        });

        assertEquals(map.size(), list.size());

        for (int el : list) {
            assertEquals(9, el);
        }
    }

    @Test
    void forEachWithNullActionTest() {
        LinkedMap<Integer, Integer> map = new LinkedMap<>();

        assertThrows(
                NullPointerException.class,
                () -> map.forEach((BiConsumer<? super Integer, ? super Integer>) null)
        );

        map.append(0, 9);
        map.append(1, 8);
        map.append(2, 7);
        map.append(3, 6);
        map.append(4, 5);
        map.append(5, 4);
        map.append(6, 3);
        map.append(7, 2);
        map.append(8, 1);
        map.append(9, 0);

        assertThrows(
                NullPointerException.class,
                () -> map.forEach((BiConsumer<? super Integer, ? super Integer>) null)
        );
    }
}
