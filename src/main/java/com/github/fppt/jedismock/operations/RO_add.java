package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;

import static com.github.fppt.jedismock.Utils.serializeObject;

abstract class RO_add extends AbstractRedisOperation {
    private final Object lock;
    RO_add(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        this.lock = state.lock();
    }

    abstract void addSliceToList(List<Slice> list, Slice slice);

    Slice response() {
        Slice key = params().get(0);
        final RMList listDBObj = getListFromBase(key);
        final List<Slice> list = listDBObj.getStoredData();

        for (int i = 1; i < params().size(); i++) {
            addSliceToList(list, params().get(i));
        }

        try {
            base().putValue(key, listDBObj);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        //Notify all waiting operations
        lock.notifyAll();
        return Response.integer(list.size());
    }
}
