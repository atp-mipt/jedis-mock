package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("type")
class RO_type extends AbstractRedisOperation {
    RO_type(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        return Response.bulkString(base().type(params().get(0)));
    }
}