package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.BlockingRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.Collections;
import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToDouble;

@RedisCommand("brpoplpush")
class BRPopLPush extends RPopLPush implements BlockingRedisOperation {
    private double timeout;
    private final boolean isTransactionModeOn;

    BRPopLPush(OperationExecutorState state, List<Slice> params) {
        super(state, params);
        this.isTransactionModeOn = state.isTransactionModeOn();
    }

    private boolean isEmptyList(Slice source) {
        final RMList list = getListFromBaseOrCreateEmpty(source);
        return list.getStoredData().isEmpty();
    }

    @Override
    protected Slice response() {
        // If method executed not inside transaction, it means canBeExecuted was called and count != 0
        // if inside transaction - means we need to response with null if not able to rpoplpush
        if (isEmptyList(params().get(0))) {
            return Response.NULL;
        }
        return super.response();
    }

    @Override
    public boolean canBeExecuted() {
        Slice source = params().get(0);
        timeout = convertToDouble(params().get(2).toString());

        if (timeout < 0) {
            throw new IllegalArgumentException("ERR timeout is negative");
        }

        return isTransactionModeOn || !isEmptyList(source);
    }

    @Override
    public long getTimeoutNanos() {
        return timeout == 0 ? Long.MAX_VALUE : (long) (timeout * 1_000_000_000);
    }

    @Override
    public Slice timeoutResponse() {
        return Response.NULL;
    }

    @Override
    public List<Slice> getBlockedOnKeys() {
        return Collections.singletonList(params().get(0));
    }

    @Override
    public boolean canBeExecutedOnKey(Slice key) {
        return canBeExecuted();
    }
}
