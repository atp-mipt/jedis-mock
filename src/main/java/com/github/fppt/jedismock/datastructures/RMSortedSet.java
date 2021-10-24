package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.WrongValueTypeException;

import java.util.LinkedHashMap;

import static com.github.fppt.jedismock.Utils.deserializeObject;

public class RMSortedSet implements RMDataStructure {
    protected LinkedHashMap<Slice, Double> storedData;

    public LinkedHashMap<Slice, Double> getStoredData() {
        return storedData;
    }

    public RMSortedSet(Slice data) {
        if (data == null) {
            storedData = new LinkedHashMap<>();
            return;
        }
        try {
            storedData = deserializeObject(data);
        } catch (WrongValueTypeException e) {
            throw new WrongValueTypeException("WRONGTYPE Failed to deserialize LinkedHashMap<Slice, Double> value");
        }
    }
}
