package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;

@RedisCommand("zrevrange")
class ZRevRange extends AbstractZRangeByIndex {

    ZRevRange(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        key = params().get(0);
        mapDBObj = getZSetFromBaseOrCreateEmpty(key);
        isRev = true;
        if (checkWrongIndex()) {
            return Response.array(new ArrayList<>());
        }

        NavigableSet<ZSetEntry> entries = getRange(
                getStartBound(Slice.create(String.valueOf(endIndex))),
                getEndBound(Slice.create(String.valueOf(startIndex))));

        return getSliceFromRange(entries);
    }
}
