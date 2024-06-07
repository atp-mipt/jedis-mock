package com.github.fppt.jedismock.storage;

import com.github.fppt.jedismock.datastructures.RMDataStructure;
import com.github.fppt.jedismock.datastructures.RMHash;
import com.github.fppt.jedismock.datastructures.Slice;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public class ExpiringKeyValueStorage {
    private final Map<Slice, RMDataStructure> values = new HashMap<>();
    private final Map<Slice, Long> ttls = new HashMap<>();
    private final Consumer<Slice> keyChangeNotifier;

    private Clock timer = Clock.systemDefaultZone();

    public ExpiringKeyValueStorage(Consumer<Slice> keyChangeNotifier) {
        this.keyChangeNotifier = keyChangeNotifier;
    }

    public void setTimer(Clock timer) {
        this.timer = timer;
    }

    private long currentTimeMillis() {
        return timer.millis();
    }

    public Map<Slice, RMDataStructure> values() {
        return values;
    }

    public Map<Slice, Long> ttls() {
        return ttls;
    }

    public void delete(Slice key) {
        keyChangeNotifier.accept(key);
        ttls().remove(key);
        values().remove(key);
    }

    public void deleteHashField(Slice key, Slice field) {
        keyChangeNotifier.accept(key);
        Objects.requireNonNull(field);

        if (!verifyKey(key)) {
            return;
        }
        RMHash hash = getRMHash(key);
        Map<Slice, Slice> storedData = hash.getStoredData();

        if (!storedData.containsKey(field)) {
            return;
        }

        storedData.remove(field);

        if (storedData.isEmpty()) {
            values.remove(key);
        }

        if (!values().containsKey(key)) {
            ttls().remove(key);
        }
    }

    public void clear() {
        for (Slice key : values().keySet()) {
            if (!isKeyOutdated(key)) {
                keyChangeNotifier.accept(key);
            }
        }
        values().clear();
        ttls().clear();
    }

    public RMDataStructure getValue(Slice key) {
        if (!verifyKey(key)) {
            return null;
        }
        return values().get(key);
    }

    private boolean verifyKey(Slice key) {
        Objects.requireNonNull(key);
        if (!values().containsKey(key)) {
            return false;
        }

        if (isKeyOutdated(key)) {
            delete(key);
            return false;
        }
        return true;
    }

    boolean isKeyOutdated(Slice key) {
        Long deadline = ttls().get(key);
        return deadline != null && deadline != -1 && deadline <= currentTimeMillis();
    }

    public Long getTTL(Slice key) {
        Objects.requireNonNull(key);
        Long deadline = ttls().get(key);
        if (deadline == null) {
            return null;
        }
        if (deadline == -1) {
            return deadline;
        }
        long now = currentTimeMillis();
        if (now < deadline) {
            return deadline - now;
        }
        delete(key);
        return null;
    }

    public long setTTL(Slice key, long ttl) {
        keyChangeNotifier.accept(key);
        return setDeadline(key, ttl + currentTimeMillis());
    }

    public void put(Slice key, RMDataStructure value, Long ttl) {
        keyChangeNotifier.accept(key);
        values().put(key, value);
        configureTTL(key, ttl);
    }

    // Put inside
    public void put(Slice key, Slice value, Long ttl) {
        keyChangeNotifier.accept(key);
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        values().put(key, value.extract());
        configureTTL(key, ttl);
    }

    // Put into inner RMHMap
    public void put(Slice key1, Slice key2, Slice value, Long ttl) {
        keyChangeNotifier.accept(key1);
        Objects.requireNonNull(key1);
        Objects.requireNonNull(key2);
        Objects.requireNonNull(value);
        RMHash mapByKey;

        if (!values.containsKey(key1)) {
            mapByKey = new RMHash();
            values.put(key1, mapByKey);
        } else {
            mapByKey = getRMHash(key1);
        }
        mapByKey.put(key2, value);
        configureTTL(key1, ttl);
    }

    private RMHash getRMHash(Slice key) {
        RMDataStructure valueByKey = values.get(key);
        if (!isSortedSetValue(valueByKey)) {
            valueByKey.raiseTypeCastException();
        }

        return (RMHash) valueByKey;
    }

    private void configureTTL(Slice key, Long ttl) {
        if (ttl == null) {
            setDeadline(key, -1L); // Override TTL in any case
        } else {
            if (ttl != -1) {
                setTTL(key, ttl);
            } else {
                setDeadline(key, -1L);
            }
        }
    }

    public long setDeadline(Slice key, long deadline) {
        Objects.requireNonNull(key);
        if (values().containsKey(key)) {
            ttls().put(key, deadline);
            return 1L;
        }
        return 0L;
    }

    public boolean exists(Slice slice) {
        return verifyKey(slice);
    }

    private boolean isSortedSetValue(RMDataStructure value) {
        return value instanceof RMHash;
    }

    public Slice type(Slice key) {
        //We also check for ttl here
        if (!verifyKey(key)) {
            return Slice.create("none");
        }
        RMDataStructure valueByKey = getValue(key);

        if (valueByKey == null) {
            return Slice.create("none");
        }
        return Slice.create(valueByKey.getTypeName());
    }

    public int size() {
        return values().size();
    }
}
