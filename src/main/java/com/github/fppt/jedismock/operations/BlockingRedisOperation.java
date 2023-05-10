package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.Slice;

import java.util.List;

public interface BlockingRedisOperation extends RedisOperation {
    boolean canBeExecuted();

    boolean canBeExecutedOnKey(Slice key);

    long getTimeoutNanos();

    Slice timeoutResponse();

    List<Slice> getBlockedOnKeys();
}
