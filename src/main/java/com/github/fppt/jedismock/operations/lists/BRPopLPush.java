package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.SliceParser;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.Arrays;
import java.util.List;

import static com.github.fppt.jedismock.Utils.toMillisTimeout;

@RedisCommand("brpoplpush")
class BRPopLPush extends RPopLPush {
    private long count = 0L;
    private final Object lock;
    private final boolean isInTransaction;

    BRPopLPush(OperationExecutorState state, List<Slice> params) {
        super(state, params);
        this.lock = state.lock();
        this.isInTransaction = state.isTransactionModeOn();
    }

    protected void doOptionalWork() {
        Slice source = params().get(0);
        long timeoutMillis = toMillisTimeout(params().get(2).toString());

        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("ERR timeout is negative");
        }

        long waitEnd = base().currentTime() + timeoutMillis;
        long waitTimeMillis;
        count = getCount(source);

        try {
            while (count == 0L &&
                  !isInTransaction &&
                  (waitTimeMillis = timeoutMillis == 0 ? 0 : waitEnd - base().currentTime()) >= 0) {
                lock.wait(waitTimeMillis, 1); // prevent 0 & 0 not to get stuck
                count = getCount(source);
            }
        } catch (InterruptedException e) {
            //wait interrupted prematurely
            Thread.currentThread().interrupt();
        }
    }

    protected Slice response() {
        if (count != 0) {
            return super.response();
        } else {
            return Response.NULL;
        }
    }

    private long getCount(Slice source) {
        Slice index = Slice.create("0");
        List<Slice> commands = Arrays.asList(source, index, index);
        Slice result = new LRange(base(), commands).execute();
        return SliceParser.consumeCount(result.data());
    }
}
