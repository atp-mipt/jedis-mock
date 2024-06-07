package com.github.fppt.jedismock.operations.keys;

import com.github.fppt.jedismock.datastructures.RMDataStructure;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("move")
public class Move implements RedisOperation {
    private final OperationExecutorState state;
    private final List<Slice> params;

    public Move(OperationExecutorState state, List<Slice> params) {
        this.params = params;
        this.state = state;
    }

    RedisBase base() {
        return state.base();
    }

    @Override
    public Slice execute() {
        if (params.size() != 2) {
            return Response.error("ERR wrong number of arguments for 'move' command");
        }

        Slice key = params.get(0);
        int db;

        try {
            db = Integer.parseInt(params.get(1).toString());
        } catch (NumberFormatException e) {
            return Response.error("ERR value is not an integer or out of range");
        }

        if (!base().exists(key)) {
            return Response.integer(0);
        }

        RMDataStructure value = base().getValue(key);
        Long ttl = base().getTTL(key);
        int initialDatabase = state.getSelected();

        state.changeActiveRedisBase(db);

        if (base().exists(key)) {
            state.changeActiveRedisBase(initialDatabase);
            return Response.integer(0);
        }

        base().putValue(key, value, ttl);

        state.changeActiveRedisBase(initialDatabase);
        base().deleteValue(key);

        return Response.integer(1);
    }
}
