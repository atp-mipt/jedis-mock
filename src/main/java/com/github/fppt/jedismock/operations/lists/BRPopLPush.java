package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.BlockingRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.SliceParser;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.Arrays;
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

    private long getCount(Slice source){
        Slice index = Slice.create("0");
        List<Slice> commands = Arrays.asList(source, index, index);
        Slice result = new LRange(base(), commands).execute();
        return SliceParser.consumeCount(result.data());
    }

    @Override
    protected Slice response() {
        long count = getCount(params().get(0));

        // If method executed not inside transaction, it means canBeExecuted was called and count != 0
        // if inside transaction - means we need to response with null if not able to rpoplpush
        if (count == 0) {
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

        return isTransactionModeOn || getCount(source) != 0;
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
