package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.datastructures.ZSetEntryBound;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("zlexcount")
class ZLexCount extends ZRangeByLex {

    ZLexCount(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);
        RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);

        String start = min();
        if (!validate(start)) {
            return buildErrorResponse("start");
        }

        String end = max();
        if (!validate(end)) {
            return buildErrorResponse("end");
        }

        if (mapDBObj.isEmpty()) {
            return Response.EMPTY_ARRAY;
        } else {
            //We assume that all the elements have the same score
            double score = mapDBObj.entries(false).first().getScore();
            return Response.integer(doProcess(mapDBObj, start, end, score).size());
        }
    }

    private boolean validate(String forValidate) {
        return NEGATIVELY_INFINITE.equals(forValidate) || POSITIVELY_INFINITE.equals(forValidate) ||
                startsWithAnyPrefix(forValidate);
    }

    @Override
    protected ZSetEntryBound buildStartEntryBound(double score, String start) {
        return buildEntryBound(score, start);
    }

    @Override
    protected ZSetEntryBound buildEndEntryBound(double score, String end) {
        return buildEntryBound(score, end);
    }

    protected ZSetEntryBound buildEntryBound(double score, String bound) {
        if (NEGATIVELY_INFINITE.equals(bound)) {
            return new ZSetEntryBound(score, ZSetEntry.MIN_VALUE, true);
        } else if (POSITIVELY_INFINITE.equals(bound)) {
            return new ZSetEntryBound(score, ZSetEntry.MAX_VALUE, true);
        } else {
            return new ZSetEntryBound(score, Slice.create(bound.substring(1)), bound.startsWith(INCLUSIVE_PREFIX));
        }
    }
}
