package com.github.fppt.jedismock.operations.streams;

import com.github.fppt.jedismock.datastructures.streams.RMStream;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.WrongStreamKeyException;
import com.github.fppt.jedismock.datastructures.streams.LinkedMap;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

import static com.github.fppt.jedismock.datastructures.streams.RMStream.checkKey;

/**
 * XADD key [NOMKSTREAM] [(MAXLEN | MINID) [= | ~] threshold
 *   [LIMIT count]] (* | id) field value [field value ...]<br/>
 * Supported options: NOMKSTREAM<br/>
 * Unsupported options (TODO): MAXLEN, MINID, LIMIT
 */
@RedisCommand("xadd")
public class XAdd extends AbstractRedisOperation {
    public static final String ARG_NUMBER_ERROR = "wrong number of arguments for 'xadd' command";

    XAdd(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        if (params().size() < 3) {
            return Response.error(ARG_NUMBER_ERROR);
        }

        Slice key = params().get(0);
        Slice id = params().get(1);;
        int valueInd = 2;

        if (params().get(1).toString().equalsIgnoreCase("nomkstream")) {
            if (!base().exists(key)) {
                return Response.bulkString(Slice.empty());
            }

            if (params().size() % 2 != 1) {
                return Response.error(ARG_NUMBER_ERROR);
            }

            valueInd = 3;
            id = params().get(2);
        } else {
            if (params().size() % 2 != 0) {
                return Response.error("wrong number of arguments for 'xadd' command");
            }
        }

        RMStream setDBObj = getStreamFromBaseOrCreateEmpty(key);
        LinkedMap<Slice, LinkedMap<Slice, Slice>> map = setDBObj.getStoredData();

        LinkedMap<Slice, Slice> entryValue = new LinkedMap<>();

        try {
            id = setDBObj.compareWithTopKey(checkKey(setDBObj.replaceAsterisk(id)));
        } catch (WrongStreamKeyException e) {
            return Response.error(e.getMessage());
        }

        for (int i = valueInd; i < params().size(); i += 2) {
            entryValue.append(params().get(i), params().get(i + 1));
        }

        map.append(id, entryValue);
        setDBObj.updateLastId(id);

        base().putValue(key, setDBObj);

        return Response.bulkString(id);
    }
}
