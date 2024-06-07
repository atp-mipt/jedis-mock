package com.github.fppt.jedismock.operations.hashes;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToLong;

@RedisCommand("hincrby")
class HIncrBy extends AbstractRedisOperation {
    HIncrBy(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice hsetValue(Slice key, Slice field, Slice value) {
        long numericValue = convertToLong(String.valueOf(value));
        Slice foundValue = base().getRMHashValue(key, field);
        if (foundValue != null) {
            numericValue = Math.addExact(convertToLong(new String(foundValue.data())), numericValue);
        }
        base().putSlice(key, field, Slice.create(String.valueOf(numericValue)), -1L);
        return Response.integer(numericValue);
    }

    protected Slice response() {
        Slice key = params().get(0);
        Slice field = params().get(1);
        Slice value = params().get(2);
        return hsetValue(key, field, value);
    }
}
