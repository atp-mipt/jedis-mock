package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.RMLinkedList;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.RedisBase;
import com.google.common.collect.Lists;

import java.util.LinkedList;
import java.util.List;

class RO_llen extends AbstractRedisOperation {
    RO_llen(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        Slice key = params().get(0);
        RMLinkedList listDBObj = getLinkedListFromBase(key);
        LinkedList<Slice> list = listDBObj.getStoredList();
        return Response.integer(list.size());
    }
}
