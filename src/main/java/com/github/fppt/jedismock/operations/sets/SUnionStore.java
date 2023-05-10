package com.github.fppt.jedismock.operations.sets;

import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;


import java.util.List;


@RedisCommand("sunionstore")
class SUnionStore extends AbstractRedisOperation {
    SUnionStore(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);
        SUnion sInter = new SUnion(base(), params().subList(1, params().size()));
        RMSet result = new RMSet(sInter.getUnion());
        base().putValue(key, result);

        return Response.integer(result.getStoredData().size());
    }
}
