package com.github.fppt.jedismock.operations.server;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.RMHash;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("debug")
class Debug extends AbstractRedisOperation {
    Debug(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    // handling DEBUG OBJECT command
    protected Slice debug_object(List<Slice> params){
        RMHash cls = base().getHash(params.get(1));
        return Slice.create(cls.getMeta());
    }

    protected Slice response() {
        if (params().get(0).toString().equals("object")){
            return Response.bulkString(debug_object(params()));
        }
        return Response.OK;
    }
}



