package com.github.fppt.jedismock.operations.streams;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.streams.LinkedMap;
import com.github.fppt.jedismock.datastructures.streams.StreamId;
import com.github.fppt.jedismock.exception.WrongStreamKeyException;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

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
            return Response.error("wrong number of arguments for 'xdel' command");
        }

        Slice key = params().get(0);
        LinkedMap<StreamId, LinkedMap<Slice, Slice>> map = getStreamFromBaseOrCreateEmpty(key).getStoredData();

        int deletedElementsCount = 0;
        StreamId idToBeRemoved;

        for (int i = 1; i < params().size(); ++i) {
            try { /* FIXME all preceding nodes are to be removed */
                idToBeRemoved = new StreamId(params().get(i));
            } catch (WrongStreamKeyException e) {
                return Response.error(e.getMessage());
            }

            if (map.remove(idToBeRemoved) != null) {
                ++deletedElementsCount;
            }
        }


        return Response.integer(deletedElementsCount);
    }
}
