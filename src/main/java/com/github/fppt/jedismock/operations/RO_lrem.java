package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import org.apache.commons.collections4.iterators.ReverseListIterator;

import java.util.Iterator;
import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToInteger;

@RedisCommand("lrem")
class RO_lrem extends AbstractRedisOperation {
    private final int directedNumRemove;
    private final Slice target;

    private boolean isDeletingElement(Slice element, int numRemoved) {
        return element.equals(target) && (directedNumRemove == 0 || numRemoved < Math.abs(directedNumRemove));
    }

    RO_lrem(RedisBase base, List<Slice> params) {
        super(base, params);
        directedNumRemove = convertToInteger(new String(params().get(1).data()));
        target = params().get(2);
    }

    Slice response(){
        Slice key = params().get(0);
        RMList listObj = base().getList(key);
        if(listObj == null){
            return Response.integer(0);
        }

        List<Slice> list = listObj.getStoredData();

        //Determine the directionality of the deletions
        int numRemoved = 0;
        Iterator<Slice> iterator;
        if(directedNumRemove < 0){
            iterator = new ReverseListIterator<>(list);
        } else {
            iterator = list.listIterator();
        }

        while (iterator.hasNext()) {
            Slice element = iterator.next();
            if(isDeletingElement(element, numRemoved)) {
                iterator.remove();
                numRemoved++;
            }
        }
        return Response.integer(numRemoved);
    }
}
