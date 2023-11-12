package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.datastructures.ZSetEntryBound;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.fppt.jedismock.Utils.convertToLong;
import static java.util.Collections.emptyNavigableSet;

abstract class AbstractZRange extends AbstractByScoreOperation {
    protected static final String EXCLUSIVE_PREFIX = "(";
    protected static final String LOWEST_POSSIBLE_SCORE = "-inf";
    protected static final String HIGHEST_POSSIBLE_SCORE = "+inf";

    protected static final String WITH_SCORES = "WITHSCORES";
    protected static final String IS_REV = "REV";
    protected static final String IS_BYSCORE = "BYSCORE";
    protected static final String IS_BYLEX = "BYLEX";
    protected static final String IS_LIMIT = "LIMIT";

    protected boolean withScores = false;
    protected boolean isRev = false;
    protected boolean isByScore = false;
    protected boolean isByLex = false;
    protected boolean isLimit = false;
    protected int startIndex;
    protected int endIndex;
    protected long offset = 0;
    protected long count = 0;
    protected Slice key;
    protected RMZSet mapDBObj;

    AbstractZRange(RedisBase base, List<Slice> params) {
        super(base, params);
        parseArgs();
    }

    abstract protected ZSetEntryBound getStartBound(Slice start);
    abstract protected ZSetEntryBound getEndBound(Slice end);

    protected NavigableSet<ZSetEntry> getRange(Slice start, Slice end) {
        if (mapDBObj.isEmpty()) {
            return emptyNavigableSet();
        }

        NavigableSet<ZSetEntry> subset =
                mapDBObj.subset(getStartBound(start), getEndBound(end));
        if (isRev) {
            subset = subset.descendingSet();
        }
        return subset;

    }

    protected Slice getSliceFromRange(NavigableSet<ZSetEntry> entries) {
        final List<Slice> list;
        if (isLimit) {
            if (count == -1) {
                if (withScores) {
                    list = entries.stream()
                            .skip(offset)
                            .flatMap(e -> Stream.of(e.getValue(),
                                    Slice.create(String.format("%.0f", e.getScore()))))
                            .map(Response::bulkString)
                            .collect(Collectors.toList());
                } else {
                    list = entries.stream()
                            .skip(offset)
                            .map(ZSetEntry::getValue)
                            .map(Response::bulkString)
                            .collect(Collectors.toList());
                }
            } else {
                if (withScores) {
                    list = entries.stream()
                            .skip(offset)
                            .limit(count)
                            .flatMap(e -> Stream.of(e.getValue(),
                                    Slice.create(String.format("%.0f", e.getScore()))))
                            .map(Response::bulkString)
                            .collect(Collectors.toList());
                } else {
                    list = entries.stream()
                            .skip(offset)
                            .limit(count)
                            .map(ZSetEntry::getValue)
                            .map(Response::bulkString)
                            .collect(Collectors.toList());
                }

            }
        } else {
            if (withScores) {
                list = entries.stream()
                        .flatMap(e -> Stream.of(e.getValue(),
                                Slice.create(String.format("%.0f", e.getScore()))))
                        .map(Response::bulkString)
                        .collect(Collectors.toList());
            } else {
                list = entries.stream()
                        .map(ZSetEntry::getValue)
                        .map(Response::bulkString)
                        .collect(Collectors.toList());
            }
        }

        return Response.array(list);
    }

    protected final void parseArgs() {
        for (Slice param : params()) {
            if (WITH_SCORES.equalsIgnoreCase(param.toString())) {
                withScores = true;
            }
            if (IS_REV.equalsIgnoreCase(param.toString())) {
                isRev = true;
            }
            if (IS_BYSCORE.equalsIgnoreCase(param.toString())) {
                isByScore = true;
            }
            if (IS_BYLEX.equalsIgnoreCase(param.toString())) {
                isByLex = true;
            }
            if (IS_LIMIT.equalsIgnoreCase(param.toString())) {
                isLimit = true;
                int index = params().indexOf(param);
                offset = convertToLong(params().get(++index).toString());
                count = convertToLong(params().get(++index).toString());
            }
        }
    }

    protected Slice remRangeFromKey(NavigableSet<ZSetEntry> entries) {
        int count = 0;
        for (ZSetEntry entry : new ArrayList<>(entries)) {
            mapDBObj.remove(entry.getValue());
            count++;
        }
        if (mapDBObj.isEmpty()) {
            base().deleteValue(key);
        } else {
            base().putValue(key, mapDBObj);
        }
        return Response.integer(count);
    }
}
