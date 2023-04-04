package com.github.fppt.jedismock.operations.scripting;


import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.CommandFactory;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RedisCallback {

    private final OperationExecutorState state;

    public RedisCallback(final OperationExecutorState state) {
        this.state = state;
    }

    public Slice call(final String operationName, final String rawArg1, final String rawArg2) {
        final List<Slice> args = Stream.of(rawArg1, rawArg2).map(Slice::create).collect(Collectors.toList());
        final RedisOperation operation = CommandFactory.buildOperation(operationName, true, state, args);
        if (operation != null) {
            return operation.execute();
        }
        final RedisOperation nonTransactionalOperation = CommandFactory.buildOperation(operationName, false, state, args);
        if (nonTransactionalOperation != null) {
            return nonTransactionalOperation.execute();
        }
        return null;
    }

}