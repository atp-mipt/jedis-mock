package com.github.fppt.jedismock.operations.streams;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.streams.LinkedMap;
import com.github.fppt.jedismock.datastructures.streams.LinkedMapIterator;
import com.github.fppt.jedismock.datastructures.streams.RMStream;
import com.github.fppt.jedismock.datastructures.streams.StreamId;
import com.github.fppt.jedismock.exception.WrongStreamKeyException;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Ranges extends AbstractRedisOperation {
    protected LinkedMapIterator<StreamId, LinkedMap<Slice, Slice>> it;
    /**
     * Multiplier for comparison:<br>
     * 1 - 'xrange'<br>
     * -1 - 'xrevrange'
     */
    protected int multiplier = 1;

    Ranges(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected StreamId preprocessExclusiveBorder(StreamId key, boolean isStart) throws WrongStreamKeyException {
        if (isStart) {
            return key.increment();
        }

        return key.decrement();
    }

    protected StreamId preprocessKey(Slice key, RMStream stream, boolean isStart) throws WrongStreamKeyException {
        String rawKey = key.toString();

        if (rawKey.equals("-")) {
            return stream.getStoredData().getHead();
        } else if (rawKey.equals("+")) {
            return stream.getStoredData().getTail();
        }

        boolean exclusive = false;
        StreamId id;

        if (rawKey.charAt(0) == '(') {
            exclusive = true;
            id = new StreamId(rawKey.substring(1));
        } else {
            id = new StreamId(key);
        }

        if (exclusive) {
            id = preprocessExclusiveBorder(id, isStart);
        }

        return id;
    }

    protected Slice range() {
        RMStream stream = getStreamFromBaseOrCreateEmpty(params().get(0));
        LinkedMap<StreamId, LinkedMap<Slice, Slice>> map = stream.getStoredData();

        /* Begin parsing arguments */

        StreamId start;
        StreamId end;
        int count = map.size();

        try {
            start = preprocessKey(params().get(1), stream, true);
            end = preprocessKey(params().get(2), stream, false);
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

        if (map.size() == 0) { // empty map case
            return Response.array(Collections.emptyList());
        }

        if (multiplier == 1) {
            if (start.compareTo(map.getTail()) > 0) {
                return Response.array(Collections.emptyList());
            }
        } else {
            if (start.compareTo(map.getHead()) < 0) {
                return Response.array(Collections.emptyList());
            }
        }

        it.findFirstSuitable(start);

        List<Slice> output = new ArrayList<>();

        int i = 1;

        while (it.hasNext()) {
            if (i++ > count) {
                break;
            }

            List<Slice> s = new ArrayList<>();
            Map.Entry<StreamId, LinkedMap<Slice, Slice>> entry = it.next();

            if (multiplier * entry.getKey().compareTo(end) > 0) {
                break;
            }


            entry.getValue().forEach((key, value) -> {
                s.add(Response.bulkString(key));
                s.add(Response.bulkString(value));
            });

            output.add(Response.array(List.of(Response.bulkString(entry.getKey().toSlice()), Response.array(s))));
        }

        return Response.array(output);
    }

    @Override
    protected Slice response() {
        return null;
    }
}
