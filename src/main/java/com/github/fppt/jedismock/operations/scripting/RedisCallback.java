package com.github.fppt.jedismock.operations.scripting;


import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.CommandFactory;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import org.luaj.vm2.LuaValue;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.util.RedisInputStream;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.fppt.jedismock.operations.scripting.Eval.embedLuaListToValue;

public class RedisCallback {

    public static final byte DOLLAR_BYTE = '$';
    public static final byte ASTERISK_BYTE = '*';
    public static final byte PLUS_BYTE = '+';
    public static final byte MINUS_BYTE = '-';
    public static final byte COLON_BYTE = ':';


    private final OperationExecutorState state;

    public RedisCallback(final OperationExecutorState state) {
        this.state = state;
    }

    // TODO: experimental, won't do refactoring until I receive approval
    private LuaValue execute(final String operationName, List<Slice> args) {
        final RedisOperation operation = CommandFactory.buildOperation(operationName.toLowerCase(), true, state, args);
        if (operation != null) {
            Slice result = operation.execute();
            if (result != null) {
                byte[] data = Response.clientResponse(operationName, result).data();
                System.out.printf("RedisCallback data: %s", new String(data));
                RedisInputStream is = Stream.of(data)
                        .map(ByteArrayInputStream::new)
                        .map(RedisInputStream::new)
                        .collect(Collectors.toList()).get(0);
                return process(is);
            } else {
                return LuaValue.NONE;
            }
        }
        throw new RuntimeException("Operation not implemented!");
    }

    private static LuaValue process(final RedisInputStream is) {
        byte b = is.readByte();
        switch (b) {
            case PLUS_BYTE:
                return LuaValue.valueOf(processStatusCodeReply(is));
            case DOLLAR_BYTE:
                return LuaValue.valueOf(processBulkReply(is));
            case ASTERISK_BYTE:
                return embedLuaListToValue(processMultiBulkReply(is));
            case COLON_BYTE:
                return LuaValue.valueOf(processInteger(is));
            case MINUS_BYTE:
//                        processError(is);
                return null;
            default:
                return LuaValue.NONE;
        }

    }

    private static byte[] processStatusCodeReply(RedisInputStream is) {
        return is.readLineBytes();
    }

    private static byte[] processBulkReply(RedisInputStream is) {
        int len = is.readIntCrLf();
        if (len == -1) {
            return null;
        } else {
            byte[] read = new byte[len];

            int size;
            for(int offset = 0; offset < len; offset += size) {
                size = is.read(read, offset, len - offset);
                if (size == -1) {
                    throw new RuntimeException("It seems like server has closed the connection."); // TODO: change this
                }
            }

            is.readByte();
            is.readByte();
            return read;
        }
    }

    private static Long processInteger(RedisInputStream is) {
        return is.readLongCrLf();
    }

    private static List<LuaValue> processMultiBulkReply(RedisInputStream is) {
        int num = is.readIntCrLf();
        if (num == -1) {
            return null;
        } else {
            List<LuaValue> ret = new ArrayList(num);

            for(int i = 0; i < num; ++i) {
                try {
                    ret.add(process(is));
                } catch (JedisDataException var5) {
//                    ret.add(var5);
                }
            }

            return ret;
        }
    }

    public LuaValue call(final String operationName) {
        return execute(operationName, Collections.emptyList());
    }
    public LuaValue call(final String operationName, final String rawArg) {
        return execute(operationName, Collections.singletonList(Slice.create(rawArg)));
    }
    public LuaValue call(final String operationName, final String rawArg1, final String rawArg2) {
        final List<Slice> args = Stream.of(rawArg1, rawArg2)
                .map(Slice::create).collect(Collectors.toList());
        return execute(operationName, args);
    }

}