package com.github.fppt.jedismock.server;

import com.github.fppt.jedismock.exception.WrongValueTypeException;

import java.util.HashSet;
import java.util.Set;

import static com.github.fppt.jedismock.Utils.deserializeObject;

public class RMSet implements RMDataStructure {
    private final Set<Slice> storedSet;

    public RMSet(Slice data) {
        if (data == null) {
            storedSet = new HashSet<>();
            return;
        }
        try {
            storedSet = deserializeObject(data);
        } catch (WrongValueTypeException e) {
            throw new WrongValueTypeException("WRONGTYPE Failed to deserialize HashSet<Slice> value");
        }

    }

    public Set<Slice> getStoredSet() {
        return storedSet;
    }
}
