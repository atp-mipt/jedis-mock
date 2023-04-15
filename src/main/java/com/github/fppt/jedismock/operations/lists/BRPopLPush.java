package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.SliceParser;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.Arrays;
import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToDouble;
import static com.github.fppt.jedismock.operations.BlockingOperationsUtils.waitUntil;

@RedisCommand("brpoplpush")
class BRPopLPush extends RPopLPush {
    private Long count = 0L;
    private final Object lock;
    private final boolean isTransactionModeOn;

    BRPopLPush(OperationExecutorState state, List<Slice> params) {
        super(state, params);
        this.lock = state.lock();
        this.isTransactionModeOn = state.isTransactionModeOn();
    }

    protected void doOptionalWork(){
        Slice source = params().get(0);
        double timeout = convertToDouble(params().get(2).toString());

        if (timeout < 0) {
            throw new IllegalArgumentException("ERR timeout is negative");
        }

        count = getCount(source);

        if (isTransactionModeOn) {
            return;
        }

        try {
            count = count == null ? waitUntil(timeout, () -> getCount(source), lock) : count;
        } catch (InterruptedException e) {
            //wait interrupted prematurely
            Thread.currentThread().interrupt();
        }
    }

    protected Slice response() {
        return count != null ? super.response() : Response.NULL;
    }

    private Long getCount(Slice source){
        Slice index = Slice.create("0");
        List<Slice> commands = Arrays.asList(source, index, index);
        Slice result = new LRange(base(), commands).execute();
        long len = SliceParser.consumeCount(result.data());
        return len == 0 ? null : len;
    }
}
