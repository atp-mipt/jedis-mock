package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.Utils;
import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.WrongValueTypeException;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@RedisCommand("sort")
public class Sort extends AbstractRedisOperation {
    private static final String LIMIT_PARAM = "LIMIT";
    private static final String ALPHA_PARAM = "ALPHA";
    private static final String STORE_PARAM = "STORE";
    private static final String DESC_PARAM = "DESC";

    private boolean sortNumerically = true;
    private Slice storeTo = null;
    private int offset = 0;
    private int count = Integer.MAX_VALUE;
    private int compareMultiplier = 1;

    public Sort(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);
        parseArgs();

        Slice[] items = getItems(key);

        try {
            Arrays.sort(items, this::compare);
        } catch (WrongValueTypeException e) {
            throw new WrongValueTypeException("ERR One or more scores can't be converted into double");
        }

        List<Slice> sorted = Arrays.stream(items)
                .skip(offset)
                .limit(count)
                .map(Response::bulkString)
                .collect(Collectors.toList());

        if (storeTo != null) {
            base().putValue(storeTo, new RMList(sorted));
            return Response.integer(sorted.size());
        }

        return Response.array(sorted);
    }

    private Slice[] getItems(Slice key) {
        return getListFromBaseOrCreateEmpty(key).getStoredData().toArray(new Slice[0]);
    }

    private int compare(Slice a, Slice b) {
        return (sortNumerically ?
                Double.compare(Utils.convertToDouble(a.toString()), Utils.convertToDouble(b.toString())) :
                a.compareTo(b)) * compareMultiplier;
    }

    private void parseArgs() {
        List<Slice> params = params();
        for (int i = 1; i < params.size(); ++i) {
            if (ALPHA_PARAM.equalsIgnoreCase(params.get(i).toString())) {
                sortNumerically = false;
            }

            if (STORE_PARAM.equalsIgnoreCase(params.get(i).toString())) {
                storeTo = params.get(i + 1);
                ++i;
            }

            if (LIMIT_PARAM.equalsIgnoreCase(params.get(i).toString())) {
                offset = Utils.convertToInteger(params.get(i + 1).toString());
                count = Utils.convertToInteger(params.get(i + 2).toString());
                i += 2;
            }

            if (DESC_PARAM.equalsIgnoreCase(params.get(i).toString())) {
                compareMultiplier = -1;
            }
        }
    }
}
