package com.github.fppt.jedismock.operations.object;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.RMHash;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("object")
class Object extends AbstractRedisOperation {
    Object(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    // handling OBJECT ENCODING command
    protected Slice object(List<Slice> params){
        RMHash cls = base().getHash(params.get(1));
        return Slice.create(cls.getEncoding());
    }

    protected Slice response() {
        if (params().get(0).toString().equals("encoding")){
            return Response.bulkString(object(params()));
        }
        return Response.OK;
    }
}



