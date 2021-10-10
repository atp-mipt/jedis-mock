package com.github.fppt.jedismock.server;

import com.github.fppt.jedismock.exception.WrongValueTypeException;
import static com.github.fppt.jedismock.Utils.deserializeObject;

import java.util.LinkedList;
import java.util.List;

public class RMList implements RMDataStructure {
    private final List<Slice> storedList;

    public RMList(Slice data) {
        if (data == null) {
            storedList = new LinkedList<>();
            return;
        }
        try {
            storedList = deserializeObject(data);
        } catch (WrongValueTypeException e) {
            throw new WrongValueTypeException("WRONGTYPE Failed to deserialize LinkedList<Slice> value");
        }
    }

    public List<Slice> getStoredList() {
        return storedList;
    }
}
