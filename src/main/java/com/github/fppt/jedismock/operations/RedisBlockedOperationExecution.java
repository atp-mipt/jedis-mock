package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.Slice;

import java.util.concurrent.CompletableFuture;

public class RedisBlockedOperationExecution {

    private final BlockingRedisOperation operation;
    private final CompletableFuture<Slice> promise;

    public RedisBlockedOperationExecution(BlockingRedisOperation operation, CompletableFuture<Slice> promise) {
        this.operation = operation;
        this.promise = promise;
    }

    public BlockingRedisOperation getOperation() {
        return operation;
    }

    public CompletableFuture<Slice> getPromise() {
        return promise;
    }
}
