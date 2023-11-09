package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToLong;

@RedisCommand("zmpop")
public class ZMPop extends ZPop {
    protected static final String IS_MIN = "MIN";
    protected static final String IS_MAX = "MAX";
    protected static final String IS_COUNT = "COUNT";

    protected boolean isMinOrMax = false;
    private long count = 1;

    ZMPop(RedisBase base, List<Slice> params) {
        super(base, params, false);
    }

    @Override
    protected Slice response() {
        parseArgs();
        int numKeys = Integer.parseInt(params().get(0).toString());
        for (int i = 0; i < numKeys; i++) {
            Slice key = params().get(i + 1);
            RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);
            if (!mapDBObj.isEmpty()) {
                List<Slice> newParams = new ArrayList<>();
                newParams.add(key);
                newParams.add(Slice.create(String.valueOf(Math.min(count, mapDBObj.size()))));
                List<Slice> result = new ZPop(base(), newParams, isMinOrMax).pop();

                List<Slice> popedList = new ArrayList<>();
                for (int index = 0; index < result.size(); index += 2) {
                    Slice value = result.get(index);
                    Slice score = result.get(index + 1);
                    popedList.add(Response.array(Arrays.asList(value, score)));
                }
                Slice pop = Response.array(popedList);
                return Response.array(Arrays.asList(Response.bulkString(key), pop));
            }
        }
        return Response.NULL_ARRAY;
    }

    protected final void parseArgs() {
        for (Slice param : params()) {
            if (IS_MIN.equalsIgnoreCase(param.toString())) {
                isMinOrMax = false;
            }
            if (IS_MAX.equalsIgnoreCase(param.toString())) {
                isMinOrMax = true;
            }
            if (IS_COUNT.equalsIgnoreCase(param.toString())) {
                int index = params().indexOf(param);
                count = convertToLong(params().get(++index).toString());
            }
        }
    }
}
