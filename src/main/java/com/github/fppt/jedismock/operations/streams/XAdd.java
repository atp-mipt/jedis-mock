package com.github.fppt.jedismock.operations.streams;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.streams.LinkedMap;
import com.github.fppt.jedismock.datastructures.streams.RMStream;
import com.github.fppt.jedismock.datastructures.streams.StreamId;
import com.github.fppt.jedismock.exception.WrongStreamKeyException;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.LIMIT_OPTION_ERROR;
import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.NOT_AN_INTEGER_ERROR;
import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.SYNTAX_ERROR;
import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.TOP_ERROR;
import static com.github.fppt.jedismock.operations.streams.XTrim.trimID;
import static com.github.fppt.jedismock.operations.streams.XTrim.trimLen;

/**
 * XADD key [NOMKSTREAM] [(MAXLEN | MINID) [= | ~] threshold
 *   [LIMIT count]] (* | id) field value [field value ...]<br>
 * Supported options: all, except '~'<br>
 * About trim options see {@link XTrim}
 */
@RedisCommand("xadd")
public class XAdd extends AbstractRedisOperation {
    XAdd(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    public StreamId compareWithTopKey(StreamId key) throws WrongStreamKeyException {
        RMStream stream = getStreamFromBaseOrCreateEmpty(params().get(0));
        if (key.compareTo(stream.getLastId()) <= 0) {
            throw new WrongStreamKeyException(TOP_ERROR);
        }

        return key;
    }

    @Override
    protected Slice response() {
        if (params().size() < 3) {
            return Response.invalidArgumentsCountError("xadd");
        }

        Slice key = params().get(0);

        RMStream stream = getStreamFromBaseOrCreateEmpty(key);
        LinkedMap<StreamId, LinkedMap<Slice, Slice>> map = stream.getStoredData();

        int idInd = 1; // the first field index

        /* Parsing NOMSTREAM option */
        if (params().get(1).toString().equalsIgnoreCase("nomkstream")) {
            if (!base().exists(key)) {
                return Response.bulkString(Slice.empty());
            }

            if (params().size() % 2 != 1) {
                return Response.invalidArgumentsCountError("xadd");
            }

            idInd++; // incrementing position
        }

        /*  Begin trim options parsing */

        String criterion = "";
        int thresholdPosition = idInd + 1;
        int limit = map.size() + 1;

        if (params().get(idInd).toString().equalsIgnoreCase("maxlen")
                || params().get(idInd).toString().equalsIgnoreCase("minid")) {

            criterion = params().get(idInd++).toString(); // (MAXLEN|MINID) option

            boolean aproxTrim = false;
            switch (params().get(idInd).toString()) {
                case "~":
                    aproxTrim = true;
                case "=":
                    ++thresholdPosition;
                    ++idInd; // '~' or '='
                default:
                    ++idInd; // threshold value
            }


            if (params().get(idInd).toString().equalsIgnoreCase("limit")) {
                try {
                    limit = Integer.parseInt(params().get(++idInd).toString());
                } catch (NumberFormatException e) {
                    return Response.error(NOT_AN_INTEGER_ERROR);
                }

                if (!aproxTrim) {
                    return Response.error(LIMIT_OPTION_ERROR);
                }

                ++idInd;
            }
        }

        /*  End trim options parsing */

        Slice id = params().get(idInd++);

        LinkedMap<Slice, Slice> entryValue = new LinkedMap<>();

        StreamId nodeId;
        try {
           nodeId = compareWithTopKey(new StreamId(stream.replaceAsterisk(id)).compareToZero());
        } catch (WrongStreamKeyException e) {
            return Response.error(e.getMessage());
        }

        for (int i = idInd; i < params().size(); i += 2) {
            entryValue.append(params().get(i), params().get(i + 1));
        }

        map.append(nodeId, entryValue);
        stream.updateLastId(nodeId);

        base().putValue(key, stream);


        switch (criterion.toUpperCase()) {
            case "MAXLEN":
                /* Parsing threshold value */
                try {
                    int threshold = Integer.parseInt(params().get(thresholdPosition).toString());
                    trimLen(map, threshold, limit);
                } catch (NumberFormatException e) {
                    return Response.error(SYNTAX_ERROR);
                }
                break;

            case "MINID":
                try {
                    StreamId minId = new StreamId(params().get(thresholdPosition));
                    trimID(map, minId, limit);
                } catch (WrongStreamKeyException e) {
                    return Response.error(e.getMessage());
                }
                break;
            default:
                // ignored
        }

        return Response.bulkString(nodeId.toSlice());
    }
}
