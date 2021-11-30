package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.Set;

@RedisCommand("srem")
class RO_srem extends AbstractRedisOperation {


    RO_srem(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        Slice key = params().get(0);
        RMSet setDBObj = getSetFromBaseOrCreateEmpty(key);
        Set<Slice> set = setDBObj.getStoredData();
        if(set == null || set.isEmpty()) return Response.integer(0);
        int count = 0;
        for (int i = 1; i < params().size(); i++) {
            if (set.remove(params().get(i))) {
                count++;
            }
        }
        return Response.integer(count);
    }
}