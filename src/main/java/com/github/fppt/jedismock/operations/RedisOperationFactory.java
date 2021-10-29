package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.Slice;

import java.util.List;
import java.util.Optional;

public interface RedisOperationFactory<T> {
    Optional<RedisOperation> buildOperation(String name, T base, List<Slice> params);
}
