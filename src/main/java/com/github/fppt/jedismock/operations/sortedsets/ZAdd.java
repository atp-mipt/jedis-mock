package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.ArgumentException;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("zadd")
class ZAdd extends AbstractByScoreOperation {
    private static final String IS_XX = "XX";
    private static final String IS_NX = "NX";
    private static final String IS_LT = "LT";
    private static final String IS_GT = "GT";
    private static final String IS_CH = "CH";
    private static final String IS_INCR = "INCR";

    private boolean flagXX = false;
    private boolean flagNX = false;
    private boolean flagLT = false;
    private boolean flagGT = false;
    private boolean flagCH = false;
    private boolean flagIncr = false;

    ZAdd(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        int index = 1;
        while (parseParams(params().get(index))){
           index++;
        }

        if (flagNX && (flagGT || flagLT || flagXX)) {
            throw new ArgumentException("ERR syntax error");
        }

        return flagIncr ? incr(index) : adding(index);

    }

    private Slice incr(int index) {
        if (params().size() != index + 2) {
            throw new ArgumentException("ERR Recieved wrong number of arguments when executing command [ZAdd]");
        }
        List<Slice> newParams = new java.util.ArrayList<>();
        newParams.add(params().get(0));
        newParams.add(params().get(index));
        newParams.add(params().get(index + 1));
        ZIncrBy zIncrBy = new ZIncrBy(base(), newParams);
        return zIncrBy.response();
    }

    private Slice adding(int index) {
        Slice key = params().get(0);
        final RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);
        if (((params().size() - index) & 1) == 1) {
            throw new ArgumentException("ERR Recieved wrong number of arguments when executing command [ZAdd]");
        }
        if (flagXX && params().isEmpty()) {
            return Slice.empty();
        }
        int countAdd = 0;
        int countChange = 0;
        for (int i = index; i < params().size(); i += 2) {
            Slice score = params().get(i);
            Slice value = params().get(i + 1);

            double newScore = toDouble(score.toString());

            if (flagXX && mapDBObj.hasMember(value)) {
                Double oldScore = mapDBObj.getScore(value);
                if ((flagLT && oldScore > newScore) || (flagGT && oldScore < newScore) || (!flagLT && !flagGT && oldScore != newScore)) {
                    mapDBObj.put(value, newScore);
                    countChange++;
                }
            }
            if (flagNX && !mapDBObj.hasMember(value)) {
                mapDBObj.put(value, newScore);
                countAdd++;
            }
            if (!flagXX && !flagNX) {
                if (mapDBObj.hasMember(value)) {
                    Double oldScore = mapDBObj.getScore(value);
                    if ((flagLT && oldScore > newScore) || (flagGT && oldScore < newScore) || (!flagLT && !flagGT && oldScore != newScore)) {
                        mapDBObj.put(value, newScore);
                        countChange++;
                    }
                } else {
                    mapDBObj.put(value, newScore);
                    countAdd++;
                }
            }
        }
        if (countAdd + countChange > 0) {
            base().putValue(key, mapDBObj);
        }
        return flagCH ? Response.integer(countAdd + countChange) :
                        Response.integer(countAdd);
    }

    private boolean parseParams(Slice arg) {
        boolean result = false;
        if (IS_CH.equalsIgnoreCase(arg.toString())) {
            flagCH = true;
            result = true;
        }
        if (IS_INCR.equalsIgnoreCase(arg.toString())) {
            flagIncr = true;
            result = true;
        }
        if (IS_LT.equalsIgnoreCase(arg.toString())) {
            flagLT = true;
            result = true;
        }
        if (IS_GT.equalsIgnoreCase(arg.toString())) {
            flagGT = true;
            result = true;
        }
        if (IS_XX.equalsIgnoreCase(arg.toString())) {
            flagXX = true;
            result = true;
        }
        if (IS_NX.equalsIgnoreCase(arg.toString())) {
            flagNX = true;
            result = true;
        }
        return result;
    }

}
