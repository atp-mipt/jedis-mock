package com.github.fppt.jedismock.linkedMap;


import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

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

        assertDoesNotThrow(() -> map.append(1, 2));
        assertEquals(1, map.getHead());
        assertEquals(1, map.getTail());
        assertEquals(1, map.size());
        assertEquals(2, map.get(1));
        assertNull(map.get(0));
    }

    @Test
    void addSequenceToMapTest() {
        LinkedMap<Integer, Integer> map = new LinkedMap<>();

        assertDoesNotThrow(() -> map.append(1, 2));
        assertDoesNotThrow(() -> map.append(2, 2));
        assertDoesNotThrow(() -> map.append(3, 2));
        assertDoesNotThrow(() -> map.append(4, 2));

        assertThrows(WrongKeyException.class, () -> map.append(4, 2));
        assertThrows(WrongKeyException.class, () -> map.append(0, 2));
        assertThrows(WrongKeyException.class, () -> map.append(2, 2));
    }

    @Test
    void removePairsFromMapTest() throws WrongKeyException {
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
    void getNodeTest() throws WrongKeyException {
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
    void setNextNodeTest() throws WrongKeyException {
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
    void setPreviousNodeTest() throws WrongKeyException {
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
    void addRemoveStressTest() throws WrongKeyException {
        LinkedMap<Integer, Integer> map = new LinkedMap<>();

        map.append(0, 0);

        IntStream.range(1, 1_000_001).forEach(el -> {
            assertEquals(el - 1, map.getTail());
            assertEquals(0, map.getHead());
            assertDoesNotThrow(() -> map.append(el, el * 2));
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
}
