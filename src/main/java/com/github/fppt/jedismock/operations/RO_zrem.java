package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMHMap;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.Map;

import static com.github.fppt.jedismock.Utils.serializeObject;

@TxOperation("zrem")
class RO_zrem extends AbstractRedisOperation {

    RO_zrem(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        Slice key = params().get(0);
        final RMHMap mapDBObj = getHMapFromBase(key);
        final Map<Slice, Double> map = mapDBObj.getStoredData();
        if(map == null || map.isEmpty()) return Response.integer(0);
        int count = 0;
        for (int i = 1; i < params().size(); i++) {
            if (map.remove(params().get(i)) != null) {
                count++;
            }
        }
        try {
            base().putSlice(key, serializeObject(map));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        return Response.integer(count);
    }
}
