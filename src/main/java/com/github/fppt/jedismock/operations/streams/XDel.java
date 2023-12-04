package com.github.fppt.jedismock.operations.streams;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.streams.LinkedMap;
import com.github.fppt.jedismock.datastructures.streams.StreamId;
import com.github.fppt.jedismock.exception.WrongStreamKeyException;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.List;

/**
 * XDEL key id [id ...]<br>
 */
@RedisCommand("xdel")
public class XDel extends AbstractRedisOperation {
    XDel(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        if (params().size() < 2) {
            return Response.invalidArgumentsCountError("xdel");
        }

        Slice key = params().get(0);
        LinkedMap<StreamId, LinkedMap<Slice, Slice>> map = getStreamFromBaseOrCreateEmpty(key).getStoredData();

        int deletedElementsCount = 0;
        List<StreamId> idsToBeDeleted = new ArrayList<>();

        for (int i = 1; i < params().size(); ++i) {
            try {
                idsToBeDeleted.add(new StreamId(params().get(i)));
            } catch (WrongStreamKeyException e) {
                return Response.error(e.getMessage());
            }
        }

        for (StreamId id : idsToBeDeleted) {
            if (map.remove(id) != null) {
                ++deletedElementsCount;
            }
        }

        return Response.integer(deletedElementsCount);
    }
}
