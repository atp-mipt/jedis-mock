package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.exception.ArgumentException;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;

@RedisCommand("zrangestore")
class ZRangeStore extends AbstractZRangeByIndex {

    ZRangeStore(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        if (withScores) {
            throw new ArgumentException("*syntax*");
        }
        Slice keyDest = params().get(0);
        params().remove(0);
        key = params().get(0);
        if (!base().exists(key)) {
            base().deleteValue(keyDest);
            return Response.integer(0);
        }
        mapDBObj = base().getZSet(key);
//        if(mapDBObj == null) {
//            throw new ArgumentException("*WRONGTYPE*");
//        }

        if (isByScore && !isRev) {
            ZRangeByScore zRangeByScore = new ZRangeByScore(base(), new ArrayList<>());
            zRangeByScore.key = key;
            zRangeByScore.mapDBObj = mapDBObj;
            return saveToNewKey(keyDest, zRangeByScore.getRange(zRangeByScore.getStartBound(params().get(1)), zRangeByScore.getEndBound(params().get(2))));
        }
        if (isByScore) {
            ZRevRangeByScore zRevRangeByScore = new ZRevRangeByScore(base(), new ArrayList<>());
            zRevRangeByScore.key = key;
            zRevRangeByScore.mapDBObj = mapDBObj;
            zRevRangeByScore.isRev = true;
            return saveToNewKey(keyDest, zRevRangeByScore.getRange(zRevRangeByScore.getStartBound(params().get(2)), zRevRangeByScore.getEndBound(params().get(1))));
        }
        if (isByLex && !isRev) {
            ZRangeByLex zRangeByLex = new ZRangeByLex(base(), new ArrayList<>());
            zRangeByLex.key = key;
            zRangeByLex.mapDBObj = mapDBObj;
            return saveToNewKey(keyDest, zRangeByLex.getRange(zRangeByLex.getStartBound(params().get(1)), zRangeByLex.getEndBound(params().get(2))));
        }
        if (isByLex) {
            ZRevRangeByLex zRevRangeByLex = new ZRevRangeByLex(base(), new ArrayList<>());
            zRevRangeByLex.key = key;
            zRevRangeByLex.mapDBObj = mapDBObj;
            zRevRangeByLex.isRev = true;
            return saveToNewKey(keyDest, zRevRangeByLex.getRange(zRevRangeByLex.getStartBound(params().get(2)), zRevRangeByLex.getEndBound(params().get(1))));
        }
        if (isLimit) {
            throw new ArgumentException("ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX");
        }
        if (checkWrongIndex()) {
            base().deleteValue(keyDest);
            return Response.integer(0);
        }

        NavigableSet<ZSetEntry> entries = getRange(getStartBound(Slice.create(String.valueOf(startIndex))), getStartBound(Slice.create(String.valueOf(endIndex))));

        return saveToNewKey(keyDest, entries);
    }

    private Slice saveToNewKey(Slice keyDest, NavigableSet<ZSetEntry> entries) {
        RMZSet resultZSet = new RMZSet();
        if (isLimit) {
            int tempOffset = 0;
            int tempCount = 0;
            for (ZSetEntry entry : entries) {
                if (tempOffset < offset) {
                    tempOffset++;
                    continue;
                }
                if (count == -1) {
                    resultZSet.put(entry.getValue(), entry.getScore());
                } else if (tempCount < count) {
                    resultZSet.put(entry.getValue(), entry.getScore());
                    tempCount++;
                } else {
                    break;
                }
            }
        } else {
            for (ZSetEntry entry : entries) {
                resultZSet.put(entry.getValue(), entry.getScore());
            }
        }
        base().deleteValue(keyDest);
        if (resultZSet.size() > 0) {
            base().putValue(keyDest, resultZSet);
        }
        return Response.integer(resultZSet.size());
    }
}
