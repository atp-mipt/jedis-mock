package com.github.fppt.jedismock.server;

import com.github.fppt.jedismock.commands.RedisCommand;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.server.MockExecutor;
import com.github.fppt.jedismock.operations.server.RedisCommandInterceptor;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;

/**
 * Created by Xiaolu on 2015/4/20.
 */
public class RedisOperationExecutor {
    private final OperationExecutorState state;
    private RedisCommandInterceptor mockedOperationsHandler = MockExecutor::proceed;

    public RedisOperationExecutor(OperationExecutorState state) {
        this.state = state;
    }

    public RedisOperationExecutor(OperationExecutorState state, RedisCommandInterceptor mockedOperationsHandler) {
        this.state = state;
        this.mockedOperationsHandler = mockedOperationsHandler;
    }

    public Slice execCommand(RedisCommand command) {
        if (!(command.parameters().size() > 0)) {
            throw new IllegalStateException();
        }
        List<Slice> params = command.parameters();
        List<Slice> commandParams = params.subList(1, params.size());
        String name = new String(params.get(0).data()).toLowerCase();

        return mockedOperationsHandler.execCommand(state, name, commandParams);
    }

    public void setMockedOperationsHandler(RedisCommandInterceptor mockedOperationsHandler) {
        this.mockedOperationsHandler = mockedOperationsHandler;
    }
}
