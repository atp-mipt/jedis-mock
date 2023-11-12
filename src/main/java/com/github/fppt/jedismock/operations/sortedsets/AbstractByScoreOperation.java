package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.exception.WrongValueTypeException;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

public abstract class AbstractByScoreOperation extends AbstractRedisOperation {

    public AbstractByScoreOperation(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    public static double toDouble(String value) {
        if ("+inf".equalsIgnoreCase(value)) {
            return Double.POSITIVE_INFINITY;
        }
        if ("-inf".equalsIgnoreCase(value)) {
            return Double.NEGATIVE_INFINITY;
        }

        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new WrongValueTypeException("*ERR*not*float*");
        }
    }

}
