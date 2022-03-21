package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMHMap;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.fppt.jedismock.Utils.convertToLong;

@RedisCommand("zrangebyscore")
public class ZRangeByScore extends AbstractByScoreOperation {
    private static final String IS_BYSCORE = "BYSCORE";
    private final ZRange zRange;

    public ZRangeByScore(RedisBase base, List<Slice> params) {
        super(base, params);
        List<Slice> updatedParams = new ArrayList<>(params);
        updatedParams.add(Slice.create(IS_BYSCORE));
        this.zRange = new ZRange(base, updatedParams);
    }

    @Override
    protected Slice response() {
        return zRange.response();
    }
}
