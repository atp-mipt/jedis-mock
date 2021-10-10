package com.github.fppt.jedismock.server;

import com.github.fppt.jedismock.exception.WrongValueTypeException;
import static com.github.fppt.jedismock.Utils.deserializeObject;

import java.util.LinkedList;

public class RMLinkedList implements RMDataStructure {
    private final LinkedList<Slice> storedList;

    public RMLinkedList(Slice data) {
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

    public LinkedList<Slice> getStoredList() {
        return storedList;
    }
}
