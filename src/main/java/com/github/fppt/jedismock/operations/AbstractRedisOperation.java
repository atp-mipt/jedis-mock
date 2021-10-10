package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.RMLinkedList;
import com.github.fppt.jedismock.server.RMMap;
import com.github.fppt.jedismock.server.RMSet;
import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

import static com.github.fppt.jedismock.Utils.deserializeObject;

abstract class AbstractRedisOperation implements RedisOperation {
    private final RedisBase base;
    private final List<Slice> params;

    AbstractRedisOperation(RedisBase base, List<Slice> params) {
        this.base = base;
        this.params = params;
    }

    void doOptionalWork(){
        //Place Holder For Ops which need to so some operational work
    }

    abstract Slice response();

    RedisBase base(){
        return base;
    }

    List<Slice> params(){
        return params;
    }

    public RMLinkedList getLinkedListFromBase(Slice key) {
        Slice data = base().getValue(key);
        return new RMLinkedList(data);
    }

    public RMSet getSetFromBase(Slice key) {
        Slice data = base().getValue(key);
        return new RMSet(data);
    }

    public RMMap getMapFromBase(Slice key) {
        Slice data = base.getValue(key);
        return new RMMap(data);
    }

    <V> V getDataFromBase(Slice key, V defaultResponse){
        Slice data = base().getValue(key);
        if (data != null) {
            return deserializeObject(data);
        } else {
            return defaultResponse;
        }
    }

    @Override
    public Slice execute(){
        try {
            doOptionalWork();
            synchronized (base) {
                return response();
            }
        } catch (IndexOutOfBoundsException e){
            throw new IllegalArgumentException("Invalid number of arguments when executing command [" + getClass().getSimpleName() + "]", e);
        }
    }
}
