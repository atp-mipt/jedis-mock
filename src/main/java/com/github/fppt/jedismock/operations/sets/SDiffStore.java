package com.github.fppt.jedismock.operations.sets;

import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;


import java.util.List;


@RedisCommand("sdiffstore")
class SDiffStore extends AbstractRedisOperation {
    SDiffStore(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);
        SDiff sDiff = new SDiff(base(), params().subList(1, params().size()));
        RMSet result = new RMSet(sDiff.getDifference());

        // delete dstkey if some params key dont exist
        if(params().size() > 1){
            RMSet obj = base().getSet(params().get(1));
            if(obj == null) {
                base().deleteValue(key);
                return Response.integer(0);
            }
        }
        base().putValue(key, result);

        return Response.integer(result.getStoredData().size());
    }
}
