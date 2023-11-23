package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.ArgumentException;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.ArrayList;
import java.util.List;

@RedisCommand("zadd")
class ZAdd extends AbstractByScoreOperation {
    private final Object lock;

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
    private int countAdd = 0;
    private int countChange = 0;

    ZAdd(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        this.lock = state.lock();
    }

    @Override
    protected Slice response() {
        parseParams();

        if (flagNX && (flagGT || flagLT || flagXX)) {
            throw new ArgumentException("ERR syntax error");
        }

        if (flagLT && flagGT) {
            throw new ArgumentException("ERR syntax error");
        }

        return flagIncr ? incr() : adding();
    }

    private Slice incr() {
        Slice key = params().get(0);
        final RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);
        if (params().size() != 3) {
            throw new ArgumentException("ERR*ERR*syntax*");
        }
        String increment = params().get(1).toString();
        Slice member = params().get(2);
        double score = (mapDBObj.getScore(member) == null) ? 0d :
                mapDBObj.getScore(member);

        double newScore = getSum(score, increment);

        if (newScore != score) {
            addOneElement(mapDBObj, member, newScore);
            if (countChange + countAdd > 0) {
                mapDBObj.put(member, newScore);
                base().putValue(key, mapDBObj);
                lock.notifyAll();
                return Response.bulkString(Slice.create(String.valueOf(newScore)));
            }
        }
        return Response.NULL;
    }

    private Slice adding() {
        Slice key = params().get(0);
        final RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);
        if (((params().size()) & 1) == 0) {
            throw new ArgumentException("ERR*ERR*syntax*");
        }
        if (flagXX && params().isEmpty()) {
            return Slice.empty();
        }

        for (int i = 1; i < params().size(); i += 2) {
            Slice score = params().get(i);
            Slice value = params().get(i + 1);

            double newScore = toDouble(score.toString());

            addOneElement(mapDBObj, value, newScore);
        }
        if (countAdd + countChange > 0) {
            base().putValue(key, mapDBObj);
            lock.notifyAll();
        }
        return flagCH ? Response.integer(countAdd + countChange) :
                        Response.integer(countAdd);
    }

    private void addOneElement(RMZSet mapDBObj, Slice value, double newScore) {
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

    private void parseParams() {
        List<Slice> temp = new ArrayList<>(params());
        for (Slice param : temp) {
            if (IS_CH.equalsIgnoreCase(param.toString())) {
                flagCH = true;
                params().remove(param);
            }
            if (IS_INCR.equalsIgnoreCase(param.toString())) {
                flagIncr = true;
                params().remove(param);
            }
            if (IS_LT.equalsIgnoreCase(param.toString())) {
                flagLT = true;
                params().remove(param);
            }
            if (IS_GT.equalsIgnoreCase(param.toString())) {
                flagGT = true;
                params().remove(param);
            }
            if (IS_XX.equalsIgnoreCase(param.toString())) {
                flagXX = true;
                params().remove(param);
            }
            if (IS_NX.equalsIgnoreCase(param.toString())) {
                flagNX = true;
                params().remove(param);
            }
        }
    }

}
