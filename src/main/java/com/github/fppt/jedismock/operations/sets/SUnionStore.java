package com.github.fppt.jedismock.operations.sets;

import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;


import java.util.List;
import java.util.Set;


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

        // delete dstkey if params key dont exist or are empty
        boolean nonEmptyExists = false;
        for(int i = 1; i < params().size(); i++){
            Set<Slice> set = getSetFromBaseOrCreateEmpty(params().get(i)).getStoredData();
            if(set.size() > 0) {
                nonEmptyExists = true;
            }
        }
        if(!nonEmptyExists) {
            base().deleteValue(key);
            return Response.integer(0);
        }
        base().putValue(key, result);

        return Response.integer(result.getStoredData().size());
    }
}
