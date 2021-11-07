package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;

@RedisCommand("blpop")
class RO_blpop extends RO_bpop {
    RO_blpop(OperationExecutorState state, List<Slice> params) {
        super(state, params);
    }

    @Override
    AbstractRedisOperation popper(List<Slice> params) {
        return new RO_lpop(base(), params);
    }
}
