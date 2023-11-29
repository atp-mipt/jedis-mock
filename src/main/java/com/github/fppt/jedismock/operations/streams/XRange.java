package com.github.fppt.jedismock.operations.streams;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.streams.LinkedMap;
import com.github.fppt.jedismock.datastructures.streams.StreamId;
import com.github.fppt.jedismock.exception.WrongStreamKeyException;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.Collections;
import java.util.List;

/**
 * XRANGE key start end [COUNT count]<br>
 * Supported options: COUNT
 */
@RedisCommand("xrange")
public class XRange extends Ranges {
    XRange(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        if (params().size() < 3) {
            return Response.invalidArgumentsCountError("xrange");
        }

        Slice key = params().get(0);
        LinkedMap<StreamId, LinkedMap<Slice, Slice>> map = getStreamFromBaseOrCreateEmpty(key).getStoredData();
        it = map.iterator();

        return range();
    }
}
