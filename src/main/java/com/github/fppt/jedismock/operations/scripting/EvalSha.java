package com.github.fppt.jedismock.operations.scripting;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("evalsha")
public class EvalSha extends AbstractRedisOperation {

    public EvalSha(final RedisBase base, final List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        return null;
    }
}
