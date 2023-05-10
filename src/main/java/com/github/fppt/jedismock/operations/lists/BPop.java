package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.WrongValueTypeException;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.BlockingRedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.SliceParser;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToDouble;

abstract class BPop extends AbstractRedisOperation implements BlockingRedisOperation {

    private final boolean isTransactionModeOn;
    private double timeout;

    BPop(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        isTransactionModeOn = state.isTransactionModeOn();
    }

    abstract AbstractRedisOperation popper(List<Slice> params);

    protected Slice response() {
        List<Slice> keys = params().subList(0, params().size() - 1);

        Slice source = getKey(keys, false);

        if (isTransactionModeOn && source == null) {
            return Response.NULL;
        }

        Slice result = popper(Collections.singletonList(source)).execute();
        return Response.array(Arrays.asList(Response.bulkString(source), result));
    }

    private Slice getKey(List<Slice> list, boolean checkForType) {
        for (Slice key : list) {
            if (!base().exists(key)) {
                continue;
            }
            Slice result;
            try {
                result = new LLen(base(), Collections.singletonList(key)).execute();
            } catch (WrongValueTypeException e) {
                if (checkForType) {
                    throw e;
                }
                continue;
            }
            int length = SliceParser.consumeInteger(result.data());
            if (length > 0) {
                return key;
            }
        }
        return null;
    }

    @Override
    public boolean canBeExecuted() {
        int size = params().size();
        if (size < 2) {
            throw new IndexOutOfBoundsException("require at least 2 params");
        }
        timeout = convertToDouble(params().get(size - 1).toString());
        if (timeout < 0) {
            throw new IllegalArgumentException("ERR timeout is negative");
        }

        List<Slice> keys = params().subList(0, size - 1);

        return getKey(keys, true) != null || isTransactionModeOn;
    }

    @Override
    public Slice timeoutResponse() {
        return Response.NULL;
    }

    @Override
    public long getTimeoutNanos() {
        return timeout == 0 ? Long.MAX_VALUE : (long) (timeout * 1_000_000_000);
    }

    @Override
    public List<Slice> getBlockedOnKeys() {
        return params().subList(0, params().size() - 1);
    }

    @Override
    public boolean canBeExecutedOnKey(Slice key) {
        return getKey(Collections.singletonList(key), false) != null || isTransactionModeOn;
    }
}
