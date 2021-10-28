package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.Utils;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

class RO_set extends AbstractRedisOperation {
    RO_set(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        Slice key = params().get(0);
        Slice value = params().get(1);

        if (nx()) {
            Slice old = base().getValue(key);
            if (old == null) {
                base().putValue(key, value, ttl());
                return Response.OK;
            } else {
                return Response.NULL;
            }
        } else if (xx()) {
            Slice old = base().getValue(key);
            if (old == null) {
                return Response.NULL;
            } else {
                base().putValue(key, value, ttl());
                return Response.OK;
            }
        } else {
            base().putValue(key, value, ttl());
            return Response.OK;
        }
    }

    private boolean nx() {
        return params().stream().map(Slice::toString).anyMatch("nx"::equalsIgnoreCase);
    }

    private boolean xx() {
        return params().stream().map(Slice::toString).anyMatch("xx"::equalsIgnoreCase);
    }

    private Long ttl() {
        String previous = null;
        for (Slice param : params()) {
            if ("ex".equalsIgnoreCase(previous)) {
                return 1000 * Utils.convertToLong(param.toString());
            } else if ("px".equalsIgnoreCase(previous)) {
                return Utils.convertToLong(param.toString());
            }
            previous = param.toString();
        }
        return null;
    }

}
