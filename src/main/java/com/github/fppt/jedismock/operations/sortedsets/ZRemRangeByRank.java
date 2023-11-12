package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("zremrangebyrank")
public class ZRemRangeByRank extends AbstractZRangeByIndex {

    ZRemRangeByRank(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        key = params().get(0);
        mapDBObj = getZSetFromBaseOrCreateEmpty(key);

        if (checkWrongIndex()) {
            return Response.integer(0);
        }

        return remRangeFromKey(getRange(Slice.create(String.valueOf(startIndex)), Slice.create(String.valueOf(endIndex))));
    }

}
