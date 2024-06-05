package com.github.fppt.jedismock.operations.keys;

import com.github.fppt.jedismock.datastructures.RMDataStructure;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

/**
 * COPY source destination [DB destination-db] [REPLACE]
 */
@RedisCommand(value = "copy", transactional = false)
public class Copy implements RedisOperation {
    private final OperationExecutorState state;
    private final List<Slice> params;

    public Copy(OperationExecutorState state, List<Slice> params) {
        this.params = params;
        this.state = state;
    }

    RedisBase base() {
        return state.base();
    }

    @Override
    public Slice execute() {
        if (params.size() < 2) {
            return Response.error("ERR wrong number of arguments for 'copy' command");
        }

        boolean replace = false;
        int initialDatabase = state.getSelected();

        /* Start parsing */
        Slice source = params.get(0);
        Slice destination = params.get(1);
        int selectedDatabase = 0;
        boolean useNonDefaultDatabase = false;

        if (params.size() > 2) {
            int replaceIndex = 2;

            // Check 'DB' presence
            if (params.get(2).toString().equalsIgnoreCase("db")) {
                if (params.size() == 3) { // If 'DB' presents than the value should follow it
                    return Response.error("ERR syntax error");
                }

                try {
                    selectedDatabase = Integer.parseInt(params.get(3).toString());
                    useNonDefaultDatabase = true;
                } catch (NumberFormatException e) {
                    return Response.error("ERR value is not an integer or out of range");
                }

                replaceIndex += 2;
            }

            // Check 'REPLACE' presence
            if (params.size() > replaceIndex + 1) {
                return Response.error("ERR syntax error");
            } else if (params.size() == replaceIndex + 1) {
                if (!params.get(replaceIndex).toString().equalsIgnoreCase("replace")) {
                    return Response.error("ERR syntax error");
                }

                replace = true;
            }
        }
        /* End parsing */

        RMDataStructure value = base().getValue(source);

        if (value == null) { // source doesn't exist
            return Response.integer(0);
        }

        Long ttl = base().getTTL(source);

        if (useNonDefaultDatabase) {
            state.changeActiveRedisBase(selectedDatabase);
        }

        boolean destExists = base().getValue(destination) != null;

        if (!replace && destExists) {
            state.changeActiveRedisBase(initialDatabase); // reset database
            return Response.integer(0);
        }


        base().deleteValue(destination);
        base().putValue(destination, value, ttl);

        state.changeActiveRedisBase(initialDatabase); // reset database

        return Response.integer(1);
    }
}
