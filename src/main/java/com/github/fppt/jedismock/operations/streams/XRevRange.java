package com.github.fppt.jedismock.operations.streams;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.streams.LinkedMap;
import com.github.fppt.jedismock.datastructures.streams.StreamId;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("xrevrange")
public class XRevRange extends Ranges {
    public XRevRange(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        if (params().size() < 3) {
            return Response.invalidArgumentsCountError("xrevrange");
        }

        Slice key = params().get(0);
        LinkedMap<StreamId, LinkedMap<Slice, Slice>> map = getStreamFromBaseOrCreateEmpty(key).getStoredData();
        it = map.reverseIterator();
        multiplier = -1;

        return range();
    }
}
