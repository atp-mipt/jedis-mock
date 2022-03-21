package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMHMap;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.fppt.jedismock.Utils.convertToInteger;
import static com.github.fppt.jedismock.Utils.convertToLong;

@RedisCommand("zrange")
class ZRange extends AbstractByScoreOperation {

    private static final String WITH_SCORES = "WITHSCORES";
    private static final String IS_REV = "REV";
    private static final String IS_BYSCORE = "BYSCORE";
    private static final String IS_LIMIT = "LIMIT";
    private static final Comparator<Map.Entry<Slice, Double>> zRangeComparator = Comparator
            .comparingDouble((ToDoubleFunction<Map.Entry<Slice, Double>>) Map.Entry::getValue)
            .thenComparing(Map.Entry::getKey);

    private boolean withScores = false;
    private boolean isRev = false;
    private boolean isByScore = false;
    private boolean isLimit = false;
    private long offset = 0;
    private long count = 0;
    private int start = 0;
    private int end = 0;

    ZRange(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);
        final RMHMap mapDBObj = getHMapFromBaseOrCreateEmpty(key);
        final Map<Slice, Double> map = mapDBObj.getStoredData();
        Predicate<Double> filterPredicate = getFilterPredicate(params().get(1).toString(), params().get(2).toString());

        initVars();
        calculateIndexes(map);

        boolean finalWithScores = withScores;
        boolean finalIsByScore = isByScore;

        List<Slice> values = map.entrySet().stream()
                .filter(e -> !finalIsByScore || filterPredicate.test(e.getValue()))
                .sorted(isRev ? zRangeComparator.reversed() : zRangeComparator)
                .skip(isLimit ? offset : (isByScore ? 0 : start))
                .limit(isLimit ? count : (isByScore ? map.entrySet().size() : end - start + 1))
                .flatMap(e -> finalWithScores
                        ? Stream.of(e.getKey(), Slice.create(e.getValue().toString()))
                        : Stream.of(e.getKey()))
                .map(Response::bulkString)
                .collect(Collectors.toList());

        return Response.array(values);
    }

    private void calculateIndexes(Map<Slice, Double> map) {
        if (!isByScore) {
            start = convertToInteger(params().get(1).toString());
            end = convertToInteger(params().get(2).toString());

            if (start < 0) {
                start = map.size() + start;
                if (start < 0) {
                    start = 0;
                }
            }

            if (end < 0) {
                end = map.size() + end;
                if (end < 0) {
                    end = -1;
                }
            }

            if (end >= map.size()) {
                end = map.size() - 1;
            }
        }
    }

    private void initVars() {
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
            if (IS_LIMIT.equalsIgnoreCase(param.toString())) {
                isLimit = true;
                int limitIndex = params().indexOf(param);
                offset = convertToLong(params().get(limitIndex + 1).toString());
                count = convertToLong(params().get(limitIndex + 2).toString());
            }
        }
    }
}
