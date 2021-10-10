package com.github.fppt.jedismock.server;

import com.github.fppt.jedismock.exception.WrongValueTypeException;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.github.fppt.jedismock.Utils.deserializeObject;

public class RMMap implements RMDataStructure {
    private final Map<Slice, Double> storedMap;

    public RMMap(Slice data) {
        if (data == null) {
            storedMap = new LinkedHashMap<>();
            return;
        }
        try {
            storedMap = deserializeObject(data);
        } catch (WrongValueTypeException e) {
            throw new WrongValueTypeException("WRONGTYPE Failed to deserialize LinkedHashMap<Slice, Double> value");
        }
    }

    public Map<Slice, Double> getStoredMap() {
        return storedMap;
    }
}
