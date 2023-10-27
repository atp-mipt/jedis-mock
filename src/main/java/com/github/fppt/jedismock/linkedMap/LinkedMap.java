package com.github.fppt.jedismock.linkedMap;

import java.util.HashMap;
import java.util.Map;

/**
 * An associative array with O(1) get, delete operations.<br>
 * Can be interpreted as a sequence of nodes that allows to iterate map.
 *
 * @param <K> keys type, must implement {@link java.lang.Comparable Comparable}
 * @param <V> values type
 */
public class LinkedMap<K extends Comparable<K>, V> implements Iterable<Map.Entry<K, V>> {
    /**
     * A node that replaces value in {@code HashMap}. Contains additional data of next and previous nodes.
     */
    protected class LinkedMapNode {
        protected final V value;
        protected K next;
        protected K prev;

        LinkedMapNode(V value) {
            this.value = value;
        }

        public LinkedMapNode setNext(K next) {
            this.next = next;
            return this;
        }

        public LinkedMapNode setPrev(K prev) {
            this.prev = prev;
            return this;
        }
    }

    private final Map<K, LinkedMapNode> map;
    private K tail;
    private K head;
    private int size;

    public LinkedMap() {
        this.map = new HashMap<>();
        size = 0;
    }

    /**
     * Add a mapping to the end of {@code LinkedMap}
     * @asymptotic O(1)
     * @param key the key with which the specified value is to be associated
     * @param value the value to be associated with the specified key
     */
    public void append(K key, V value) throws WrongKeyException {
        if (size == 0) {
            head = key; // the map is empty, so the first appended element becomes the head
        } else {
            if (tail.compareTo(key) >= 0) {
                throw new WrongKeyException("Key " + key + " is less than " + tail);
            }

            map.get(tail).setNext(key); // the map is not empty, so we have to update the reference
        }

        map.put(key, new LinkedMapNode(value).setPrev(tail));


        ++size;
        tail = key;
    }

    /**
     * Remove the mapping for the given key from map if it exists
     * @asymptotic O(1) regardless the size of map
     * @param key the key of mapping to be removed
     * @return deleted entry if a mapping for the key exists otherwise {@code null}
     */
    public Map.Entry<K, V> remove(K key) {
        if (!map.containsKey(key)) {
            return null;
        }

        if (size == 1) {
            size = 0;
            tail = null;
            head = null;

            return Map.entry(key, map.remove(key).value);
        }

        if (key.equals(tail)) {
            tail = getPreviousKey(key);
            map.get(tail).next = null;
        } else if (key.equals(head)) {
            head = getNextKey(key);
            map.get(head).prev = null;
        } else {
            setNextKey(getPreviousKey(key), getNextKey(key));
            setPreviousKey(getNextKey(key), getPreviousKey(key));
        }

        --size;
        return Map.entry(key, map.remove(key).value);
    }

    /**
     * Get the value to which the given key is mapped
     * @asymptotic O(1)
     * @param key the key whose associated value is to be returned
     * @return the value to which the given key is mapped or {@code null} if map does not contain it
     */
    public V get(K key) {
        LinkedMapNode node = map.get(key);

        if (node == null) {
            return null;
        }

        return node.value;
    }

    /**
     * Get the size of map
     * @return existing mappings count
     */
    public int size() {
        return size;
    }

    /**
     * Get the key of the next node. If there is no mapping for the provided key {@code NullPointerException} is thrown.
     * <b>Private API:</b> is accessible only to {@code LinkedMapIterator}
     *
     * @param key the key of the node whose following key is being searched for
     * @return the key of the node that follows the given one
     */
    K getNextKey(K key) {
        return map.get(key).next;
    }

    /**
     * Set the key of the next node. If there is no mapping for the provided key {@code NullPointerException} is thrown.
     * <b>Private API:</b> is accessible only to {@code LinkedMapIterator}
     *
     * @param key the key of the node whose following key is being updated
     * @param next the key of the node which is to follow the given one
     */
    void setNextKey(K key, K next) {
        if (!map.containsKey(next)) {
            throw new NullPointerException("Map does not contain provided next key");
        }

        map.get(key).next = next;
    }

    /**
     * Get the key of the previous node. If there is no mapping for the key {@code NullPointerException} is thrown.
     * <b>Private API:</b> is accessible only to {@code LinkedMapIterator}
     *
     * @param key the key of the node whose previous key is being searched for
     * @return the key of the node that precedes the given one
     */
    public K getPreviousKey(K key) {
        return map.get(key).prev;
    }

    /**
     * Set the key of the previous node. If there is no mapping for the provided key {@code NullPointerException} is thrown.
     * <b>Private API:</b> is accessible only to {@code LinkedMapIterator}
     *
     * @param key the key of the node whose previous key is being updated
     * @param prev the key of the node which is to precede the given one
     */
    public void setPreviousKey(K key, K prev) {
        if (!map.containsKey(prev)) {
            throw new NullPointerException("Map does not contain provided next key");
        }

        map.get(key).prev = prev;
    }

    /**
     * Get the key of the first mapping.
     * <b>Private API:</b> is accessible only to {@code LinkedMapIterator}
     *
     * @return the first node key in the sequence
     */
    K getHead() {
        return head;
    }

    /**
     * Get the key of the last mapping.
     * <b>Private API:</b> is accessible only to {@code LinkedMapIterator}
     *
     * @return the last node key in the sequence
     */
    K getTail() {
        return tail;
    }

    /**
     * Get {@link com.github.fppt.jedismock.linkedMap.LinkedMapIterator LinkedMapIterator}
     * whose iteration starts from the head node
     *
     * @return iterator that allows to iterate map
     */
    @Override
    public LinkedMapIterator<K, V> iterator() {
        return new LinkedMapIterator<>(null, this);
    }

    /**
     * Get {@link com.github.fppt.jedismock.linkedMap.LinkedMapIterator LinkedMapIterator}
     * whose iteration starts from the provided key
     *
     * @param key the key which is the start of iteration
     * @return iterator that points to the provided key
     */
    public LinkedMapIterator<K, V> iterator(K key) {
        return new LinkedMapIterator<>(getPreviousKey(key), this);
    }
}
