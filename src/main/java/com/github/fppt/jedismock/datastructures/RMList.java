package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.WrongValueTypeException;

import static com.github.fppt.jedismock.Utils.deserializeObject;

import java.util.LinkedList;
import java.util.List;

public class RMList extends RMDataStructure<List<Slice>> {

    public RMList(Slice data) {
        if (data == null) {
            storedData = new LinkedList<>();
            return;
        }
        try {
            storedData = deserializeObject(data);
        } catch (WrongValueTypeException e) {
            throw new WrongValueTypeException("WRONGTYPE Failed to deserialize LinkedList<Slice> value");
        }
    }
}
