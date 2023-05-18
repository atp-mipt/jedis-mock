package com.github.fppt.jedismock.operations.sets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.Arrays;
import java.util.List;

@RedisCommand("smove")
public class SMove extends AbstractRedisOperation {
    SMove(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice src = params().get(0);
        Slice dest = params().get(1);
        Slice member = params().get(2);
        final int result =
                new SRem(base(), Arrays.asList(src, member)).internalRemove();

        if (result > 0) {
            new SAdd(base(), Arrays.asList(dest, member)).execute();
        }
        return Response.integer(result);
    }
}
