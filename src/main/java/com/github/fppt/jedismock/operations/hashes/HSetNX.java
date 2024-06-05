package com.github.fppt.jedismock.operations.hashes;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("hsetnx")
class HSetNX extends HSet {
    HSetNX(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice hsetValue(Slice key, Slice field, Slice value){
        Slice foundValue = base().getRMHashValue(key, field);
        if(foundValue == null){
            base().putSlice(key, field, value, -1L);
        }
        return foundValue;
    }
}
