package com.github.fppt.jedismock.operations.keys;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@RedisCommand("randomkey")
public class RandomKey extends AbstractRedisOperation {
    public RandomKey(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected int maxArgs() {
        return 0;
    }

    @Override
    protected Slice response() {
        if (base().size() == 0) {
            return Response.bulkString(Slice.empty());
        }

        Iterator<Slice> iter = base().keys().iterator();
        int iterCount = ThreadLocalRandom.current().nextInt(base().size());

        Slice key = iter.next();

        while (iterCount > 0) {
            key = iter.next();
            iterCount--;
        }

        return Response.bulkString(key);
    }
}
