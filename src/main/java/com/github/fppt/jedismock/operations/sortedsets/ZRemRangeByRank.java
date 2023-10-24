package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.stream.Collectors;

import static com.github.fppt.jedismock.Utils.convertToInteger;

@RedisCommand("zremrangebyrank")
public class ZRemRangeByRank extends AbstractByScoreOperation {
    private int startIndex = 0;
    private int endIndex = 0;

    ZRemRangeByRank(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        final Slice key = params().get(0);
        final RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);
        if (mapDBObj.isEmpty()) return Response.integer(0);

        if (!calculateIndexes(mapDBObj)) {
            return Response.integer(0);
        }

        List<ZSetEntry> subset = mapDBObj.entries(false).stream()
                .skip(startIndex)
                .limit(endIndex - startIndex + 1)
                .collect(Collectors.toList());

        for (ZSetEntry entry : subset) {
            mapDBObj.remove(entry.getValue());
        }
        if (mapDBObj.isEmpty()) {
            base().deleteValue(key);
        } else {
            base().putValue(key, mapDBObj);
        }
        return Response.integer(subset.size());
    }

    private boolean calculateIndexes(RMZSet map) {
        startIndex = convertToInteger(params().get(1).toString());
        endIndex = convertToInteger(params().get(2).toString());

        if (startIndex < 0) {
            startIndex = map.size() + startIndex;
            if (startIndex < 0) {
                startIndex = 0;
            }
        }

        if (endIndex < 0) {
            endIndex = map.size() + endIndex;
            if (endIndex < 0) {
                endIndex = -1;
            }
        }

        if (endIndex >= map.size()) {
            endIndex = map.size() - 1;
        }

        return startIndex <= map.size() && startIndex <= endIndex;
    }

}
