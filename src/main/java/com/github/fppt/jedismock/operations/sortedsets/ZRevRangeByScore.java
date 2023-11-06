package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("zrevrangebyscore")
public class ZRevRangeByScore extends AbstractZRangeByScore {

    ZRevRangeByScore(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        key = params().get(0);
        mapDBObj = getZSetFromBaseOrCreateEmpty(key);

        final Slice start = params().get(2);
        final Slice end = params().get(1);
        isRev = true;
        return getSliceFromRange(getRange(getStartBound(start), getEndBound(end)));
    }
}
