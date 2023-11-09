package com.github.fppt.jedismock.operations.streams;

import com.github.fppt.jedismock.datastructures.RMStream;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.WrongStreamKeyException;
import com.github.fppt.jedismock.linkedMap.LinkedMap;
import com.github.fppt.jedismock.linkedMap.LinkedMapIterator;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.github.fppt.jedismock.datastructures.RMStream.checkParsedKey;
import static com.github.fppt.jedismock.datastructures.RMStream.compare;
import static java.lang.Long.compareUnsigned;
import static java.lang.Long.parseUnsignedLong;
import static java.lang.Long.toUnsignedString;

/**
 * XRANGE key start end [COUNT count]<br/>
 * Supported options: COUNT
 */
@RedisCommand("xrange")
public class XRange extends AbstractRedisOperation {
    XRange(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    /* TODO support singular number params */

    private static Slice incrementKey(String[] parsedKey) throws WrongStreamKeyException {
        if (compareUnsigned(parseUnsignedLong(parsedKey[1]), -1) < 0) {
            return Slice.create(parsedKey[0] + "-" + toUnsignedString(parseUnsignedLong(parsedKey[1]) + 1));
        }

        if (compareUnsigned(parseUnsignedLong(parsedKey[0]), -1) == 0) {
            throw new WrongStreamKeyException("ERR invalid start ID for the interval");
        }

        return Slice.create(toUnsignedString(parseUnsignedLong(parsedKey[0]) + 1) + "-0");
    }

    private static Slice decrementKey(String[] parsedKey) throws WrongStreamKeyException {
        if (compareUnsigned(parseUnsignedLong(parsedKey[1]), 0) > 0) {
            return Slice.create(parsedKey[0] + "-" + toUnsignedString(parseUnsignedLong(parsedKey[1]) - 1));
        }

        if (compareUnsigned(parseUnsignedLong(parsedKey[0]), 0) == 0) {
            throw new WrongStreamKeyException("ERR invalid end ID for the interval");
        }

        return Slice.create(toUnsignedString(parseUnsignedLong(parsedKey[0]) - 1) + "-" + toUnsignedString(-1));
    }

    private Slice preprocessExclusiveBorder(String[] parsedBorder, boolean isStart) throws WrongStreamKeyException {
        return isStart ? incrementKey(parsedBorder) : decrementKey(parsedBorder);
    }

    private Slice preprocessKey(Slice key, RMStream stream, boolean isStart) throws WrongStreamKeyException {
        if (key.toString().equals("-")) {
            return stream.getStoredData().getHead();
        } else if (key.toString().equals("+")) {
            return stream.getStoredData().getTail();
        }

        boolean exclusive = false;

        String[] parsedKey;

        if (key.toString().charAt(0) == '(') {
            exclusive = true;
            parsedKey = key.toString().substring(1).split("-");
        } else {
            parsedKey = key.toString().split("-");
        }

        checkParsedKey(parsedKey);

        if (exclusive) {
            return preprocessExclusiveBorder(parsedKey, isStart);
        }

        return key;
    }

    private Slice preprocessStartKey(Slice start, RMStream stream) throws WrongStreamKeyException {
        return preprocessKey(start, stream, true);
    }

    private Slice preprocessEndKey(Slice end, RMStream stream) throws WrongStreamKeyException {
        return preprocessKey(end, stream, false);
    }

    @Override
    protected Slice response() {
        if (params().size() < 3) {
            return Response.error("wrong number of arguments for 'xrange' command");
        }

        Slice id = params().get(0);
        RMStream setDBObj = getStreamFromBaseOrCreateEmpty(id);
        LinkedMap<Slice, LinkedMap<Slice, Slice>> map = setDBObj.getStoredData();

        /* Begin parsing arguments */

        Slice start = params().get(1);
        Slice end = params().get(2);
        int count = map.size();

        try {
            start = preprocessStartKey(start, setDBObj);
            end = preprocessEndKey(end, setDBObj);
        } catch (WrongStreamKeyException e) {
            return Response.error(e.getMessage());
        }

        if (params().size() > 3) {
            if (params().size() != 5) {
                return Response.error("ERR syntax error");
            }

            if (!params().get(3).toString().equalsIgnoreCase("count")) {
                return Response.error("ERR syntax error");
            }

            try {
                count = Integer.parseInt(params().get(4).toString()); // TODO non-positive count returns (nil)
            } catch (NumberFormatException e) {
                return Response.error("ERR value is not an integer or out of range");
            }
        }

        /* End parsing arguments */

        if (map.size() == 0) {
            return Response.array(Collections.emptyList());
        }

        if (compare(start, map.getTail()) > 0) {
            return Response.array(Collections.emptyList());
        }

        if (!map.contains(start)) {
            Slice currKey = start;
            LinkedMapIterator<Slice, LinkedMap<Slice, Slice>> it = map.iterator();

            while (it.hasNext()) {
                currKey = it.next().getKey();
                if (compare(currKey, start) > 0) {
                    break;
                }
            }

            start = currKey;
        }


        LinkedMapIterator<Slice, LinkedMap<Slice, Slice>> it = map.iterator(start);

        List<Slice> output = new ArrayList<>();

        if (compare(start, end) > 0) {
            return Response.array(output);
        }

        int i = 1;

        while (it.hasNext()) {
            if (i++ > count) {
                break;
            }

            List<Slice> s = new ArrayList<>();
            Map.Entry<Slice, LinkedMap<Slice, Slice>> entry = it.next();

            if (compare(entry.getKey(), end) > 0) {
                break;
            }


            entry.getValue().forEach((key, value) -> {
                s.add(Response.bulkString(key));
                s.add(Response.bulkString(value));
            });

            output.add(Response.array(List.of(Response.bulkString(entry.getKey()), Response.array(s))));
        }

        return Response.array(output);
    }
}
