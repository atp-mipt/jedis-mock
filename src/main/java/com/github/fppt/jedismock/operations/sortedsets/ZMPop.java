package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.ArgumentException;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@RedisCommand("zmpop")
public class ZMPop extends ZPop {
    private static final String IS_MIN = "MIN";
    private static final String IS_MAX = "MAX";
    private static final String IS_COUNT = "COUNT";

    private boolean isMin = false;
    private boolean isMax = false;
    private boolean isCount = false;
    private int numKeys = 1;
    private long count = 1;

    ZMPop(RedisBase base, List<Slice> params) {
        super(base, params, false);
//        this.numKeys = numKeys;
//        this.count = count;
    }

    @Override
    protected Slice response() {
        if (params().size() < 3) {
            throw new ArgumentException("ERR wrong number of arguments for 'zmpop' command");
        }
        parseArgs();
        return getResult();
    }

    protected Slice getResult() {
        for (int i = 0; i < numKeys; i++) {
            Slice key = params().get(i + 1);
            RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);
            if (!mapDBObj.isEmpty()) {
                List<Slice> newParams = new ArrayList<>();
                newParams.add(key);
                newParams.add(Slice.create(String.valueOf(Math.min(count, mapDBObj.size()))));
                List<Slice> result = new ZPop(base(), newParams, isMax).pop();

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

    protected final List<Slice> parseArgs() {
        List<Slice> temp = new ArrayList<>(params());
        for (Slice param : temp) {
            if (IS_MIN.equalsIgnoreCase(param.toString())) {
                if (isMax) {
                    throw new ArgumentException("ERR syntax error*");
                }
                isMin = true;
                params().remove(param);
            }
            if (IS_MAX.equalsIgnoreCase(param.toString())) {
                if (isMin) {
                    throw new ArgumentException("ERR syntax error*");
                }
                isMax = true;
                params().remove(param);
            }
            if (IS_COUNT.equalsIgnoreCase(param.toString())) {
                if (isCount) {
                    throw new ArgumentException("ERR syntax error*");
                }
                isCount = true;
                int index = params().indexOf(param);
                try {
                    count = Long.parseLong(params().get(index + 1).toString());
                } catch (IndexOutOfBoundsException e) {
                    throw new ArgumentException("ERR syntax error*");
                } catch (NumberFormatException e) {
                    throw new ArgumentException("ERR count*");
                }
                if (count < 1) {
                    throw new ArgumentException("ERR count*");
                }
                params().remove(index + 1);
                params().remove(param);
            }
        }
        try {
            numKeys = Integer.parseInt(params().get(0).toString());
        } catch (NumberFormatException e) {
            throw new ArgumentException("ERR numkeys*");
        }
        if (numKeys < 1) {
            throw new ArgumentException("ERR numkeys*");
        }
        if (!isMax && !isMin) {
            throw new ArgumentException("ERR syntax error*");
        }
        if (params().size() != numKeys + 1) {
            throw new ArgumentException("ERR syntax error*");
        }
        return params();
    }
}
