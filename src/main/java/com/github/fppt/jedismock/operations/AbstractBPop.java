package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.WrongValueTypeException;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.SliceParser;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.Collections;
import java.util.List;

import static com.github.fppt.jedismock.Utils.toMillisTimeout;

public abstract class AbstractBPop extends AbstractRedisOperation {

    private final Object lock;
    private final boolean isInTransaction;
    protected long timeoutMillis;

    protected List<Slice> keys;
    private final Object lock;
    private final boolean isInTransaction;

    protected AbstractBPop(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        this.lock = state.lock();
        this.isInTransaction = state.isTransactionModeOn();
    }

    @Override
    protected int minArgs(){
        return 2;
    }

    @Override
    protected void doOptionalWork() {
        timeoutMillis = toMillisTimeout(params().get(params().size() - 1).toString());
        keys = params().subList(0, params().size() - 1);
    }

    protected abstract Slice popper(List<Slice> params);

    protected abstract AbstractRedisOperation getSize(RedisBase base, List<Slice> params);

    protected Slice response() {
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("ERR timeout is negative");
        }

        Slice source = getKey(keys, true);

        long waitEnd = base().currentTime() + timeoutMillis;
        long waitTimeMillis;

        try {
            while (source == null &&
                   !isInTransaction &&
                   (waitTimeMillis = timeoutMillis == 0 ? 0 : waitEnd - base().currentTime()) >= 0) {
                lock.wait(waitTimeMillis, 1); // prevent 0 & 0 not to get stuck
                source = getKey(keys, false);
            }
        } catch (InterruptedException e) {
            //wait interrupted prematurely
            Thread.currentThread().interrupt();
            return Response.NULL;
        }

        if (source == null) {
            return Response.NULL_ARRAY;
        } else {
            return popper(Collections.singletonList(source));
        }
    }

    private Slice getKey(List<Slice> list, boolean checkForType) {
        for (Slice key : list) {
            if (!base().exists(key)) {
                continue;
            }
            Slice result;
            try {
                result = getSize(base(), Collections.singletonList(key)).execute();
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
}
