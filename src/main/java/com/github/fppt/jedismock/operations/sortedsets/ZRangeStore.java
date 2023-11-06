package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.NavigableSet;

@RedisCommand("zrangestore")
class ZRangeStore extends AbstractZRangeByIndex {

    ZRangeStore(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice keyDest = params().get(0);
        params().remove(0);
        key = params().get(0);
        mapDBObj = getZSetFromBaseOrCreateEmpty(key);

        if (isByScore && !isRev) {
            ZRangeByScore zRangeByScore = new ZRangeByScore(base(), params());
            zRangeByScore.key = key;
            zRangeByScore.mapDBObj = mapDBObj;
            return saveToNewKey(keyDest, zRangeByScore.getRange(getStartBound(params().get(1)), getEndBound(params().get(2))));
        }
        if (isByScore) {
            ZRevRangeByScore zRevRangeByScore = new ZRevRangeByScore(base(), params());
            zRevRangeByScore.key = key;
            zRevRangeByScore.mapDBObj = mapDBObj;
            return saveToNewKey(keyDest, zRevRangeByScore.getRange(getStartBound(params().get(1)), getEndBound(params().get(2))));
        }
        if (isByLex && !isRev) {
            ZRangeByLex zRangeByLex = new ZRangeByLex(base(), params());
            zRangeByLex.key = key;
            zRangeByLex.mapDBObj = mapDBObj;
            return saveToNewKey(keyDest, zRangeByLex.getRange(getStartBound(params().get(1)), getEndBound(params().get(2))));
        }
        if (isByLex) {
            ZRevRangeByLex zRevRangeByLex = new ZRevRangeByLex(base(), params());
            zRevRangeByLex.key = key;
            zRevRangeByLex.mapDBObj = mapDBObj;
            return saveToNewKey(keyDest, zRevRangeByLex.getRange(getStartBound(params().get(1)), getEndBound(params().get(2))));
        }

        if (checkWrongIndex()) {
            return Response.integer(0);
        }

        NavigableSet<ZSetEntry> entries = getRange(getStartBound(Slice.create(String.valueOf(startIndex))), getStartBound(Slice.create(String.valueOf(endIndex))));
        if (entries.isEmpty()) {
            return Response.integer(0);
        }

        return saveToNewKey(keyDest, entries);
    }

    private Slice saveToNewKey(Slice keyDest, NavigableSet<ZSetEntry> entries) {
        RMZSet resultZSet = new RMZSet();
        for (ZSetEntry entry : entries) {
            resultZSet.put(entry.getValue(), entry.getScore());
        }
        base().putValue(keyDest, resultZSet);
        return Response.integer(resultZSet.size());
    }
}
