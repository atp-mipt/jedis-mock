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

    private static final Slice NX = Slice.create("nx");
    private static final Slice XX = Slice.create("xx");
    private static final Slice EX = Slice.create("ex");
    private static final Slice PX = Slice.create("px");

    private boolean nx() {
        return params().contains(NX);
    }

    private boolean xx() {
        return params().contains(XX);
    }

    private Long ttl() {
        Slice previous = null;
        for (Slice param : params()) {
            if (EX.equals(previous)) {
                return 1000 * Utils.convertToLong(new String(param.data()));
            } else if (PX.equals(previous)) {
                return Utils.convertToLong(new String(param.data()));
            }
            previous = param;
        }
        return null;
    }

}
