package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.ArgumentException;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.github.fppt.jedismock.Utils.convertToDouble;

@RedisCommand("bzmpop")
public class BZMPop extends BZPop {
    private Slice numKeys;

    BZMPop(OperationExecutorState state, List<Slice> params) {
        super(state, params);

    }

    @Override
    protected void doOptionalWork(){
        if (params().size() < 4) {
            throw new ArgumentException("ERR wrong number of arguments for 'bzmpop' command");
        }
        timeoutNanos = (long) (convertToDouble(params().get(0).toString()) * 1_000_000_000L);
        params().remove(0);
        ZMPop zmPop = new ZMPop(base(), new ArrayList<>(params()));
        numKeys = params().remove(0);
        keys = zmPop.parseArgs();
        keys.remove(0);
        System.out.println(keys.stream().map(Slice::toString).collect(Collectors.toList()));
    }

    @Override
    protected Slice popper(List<Slice> params) {
        List<Slice> newParams = new ArrayList<>(params());
        newParams.add(0, numKeys);
        ZMPop zmPop = new ZMPop(base(), newParams);
        return zmPop.response();
    }

}
