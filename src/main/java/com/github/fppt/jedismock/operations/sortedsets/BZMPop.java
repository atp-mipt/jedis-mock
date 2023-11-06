package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;


@RedisCommand("bzmpop")
public class BZMPop extends BZPop {

    BZMPop(OperationExecutorState state, List<Slice> params) {
        super(state, params);
        params.add(params.remove(0));
    }

    @Override
    protected Slice popper(List<Slice> params) {
        ZMPop zmPop = new ZMPop(base(), params());
        return zmPop.response();
    }
}
