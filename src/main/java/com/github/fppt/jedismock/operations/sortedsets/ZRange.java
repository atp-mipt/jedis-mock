package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.exception.ArgumentException;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.NavigableSet;

@RedisCommand("zrange")
class ZRange extends AbstractZRangeByIndex {

    ZRange(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        key = params().get(0);
        mapDBObj = getZSetFromBaseOrCreateEmpty(key);

        if (isByScore && !isRev) {
            ZRangeByScore zRangeByScore = new ZRangeByScore(base(), params());
            return zRangeByScore.response();
        }
        if (isByScore) {
            ZRevRangeByScore zRevRangeByScore = new ZRevRangeByScore(base(), params());
            return zRevRangeByScore.response();
        }
        if (isByLex && withScores) {
            throw new ArgumentException("ERR syntax error, WITHSCORES not supported in combination with BYLEX");
        }
        if (isByLex && !isRev) {
            ZRangeByLex zRangeByLex = new ZRangeByLex(base(), params());
            return zRangeByLex.response();
        }
        if (isByLex) {
            ZRevRangeByLex zRevRangeByLex = new ZRevRangeByLex(base(), params());
            return zRevRangeByLex.response();
        }
        if (isLimit && count != -1) {
            throw new ArgumentException("ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX");
        }
        if (isRev) {
            ZRevRange zRevRange = new ZRevRange(base(), params());
            return zRevRange.response();
        }

        if (checkWrongIndex()) {
            return Response.EMPTY_ARRAY;
        }

        NavigableSet<ZSetEntry> entries = getRange(getStartBound(Slice.create(String.valueOf(startIndex))), getEndBound(Slice.create(String.valueOf(endIndex))));

        return getSliceFromRange(entries);
    }

}
