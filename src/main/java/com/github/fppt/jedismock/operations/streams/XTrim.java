package com.github.fppt.jedismock.operations.streams;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.streams.LinkedMap;
import com.github.fppt.jedismock.exception.WrongStreamKeyException;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

import static com.github.fppt.jedismock.datastructures.streams.RMStream.compare;
import static com.github.fppt.jedismock.datastructures.streams.RMStream.checkKey;

/**
 * XTRIM key (MAXLEN | MINID) [= | ~] threshold [LIMIT count]<br>
 * Supported options: MINID, MAXLEN, LIMIT, =<br>
 * Unsupported options: "~" - due to our implementation works as = option
 */
@RedisCommand("xtrim")
public class XTrim extends AbstractRedisOperation {
    public XTrim(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    int trimLen(LinkedMap<?, ?> map, int threshold, int limit) {
        int numberOfEvictedNodes = 0;
        while (map.size() > threshold && numberOfEvictedNodes < limit) {
            ++numberOfEvictedNodes;
            map.removeHead();
        }

        return numberOfEvictedNodes;
    }

    int trimID(LinkedMap<Slice, ?> map, Slice threshold, int limit) {
        int numberOfEvictedNodes = 0;
        while (compare(map.getHead(), threshold) < 0 && numberOfEvictedNodes < limit) {
            ++numberOfEvictedNodes;
            map.removeHead();
        }

        return numberOfEvictedNodes;
    }

    @Override
    protected Slice response() {
        if (params().size() < 3) {
            return Response.error("ERR wrong number of arguments for 'xtrim' command");
        }

//        System.out.println(params());

        /* Begin parsing arguments */

        Slice key = params().get(0);
        LinkedMap<Slice, LinkedMap<Slice, Slice>> map = getStreamFromBaseOrCreateEmpty(key).getStoredData();

        String criterion = params().get(1).toString(); // (MAXLEN|MINID) option
        int thresholdPosition = 2;

        /* Checking for "=", "~" options */
        if (params().get(2).toString().equals("~") || params().get(2).toString().equals("=")) {
            ++thresholdPosition;
        }

        boolean aproxTrim = false;
        if (params().get(2).toString().equals("~")) {
            aproxTrim = true;
        }


//        System.out.println("threshold: " + thresholdPosition);

        int limit = map.size() + 1;

        if (params().size() > thresholdPosition + 3) {
            return Response.error("syntax error");
        }

        if (params().size() > thresholdPosition + 1) {
            if (params().get(thresholdPosition + 1).toString().equalsIgnoreCase("limit")) {
                try {
                    limit = Integer.parseInt(params().get(thresholdPosition + 2).toString());
                } catch (NumberFormatException e) {
                    return Response.error("ERR value is not an integer or out of range");
                }

                if (!aproxTrim) {
                    return Response.error("ERR syntax error, LIMIT cannot be used without the special ~ option");
                }

            } else {
                return Response.error("ERR syntax error");
            }
        }

//        System.out.println("limit: " + thresholdPosition);
//        System.out.println(criterion.toUpperCase());

        switch (criterion.toUpperCase()) {
            case "MAXLEN":
                /* Parsing threshold value */
                try {
                    int threshold = Integer.parseInt(params().get(thresholdPosition).toString());
                    return Response.integer(trimLen(map, threshold, limit));
                } catch (NumberFormatException e) {
                    return Response.error("ERR syntax error");
                }

            case "MINID":
                try {
                    return Response.integer(trimID(map, checkKey(params().get(thresholdPosition)), limit));
                } catch (WrongStreamKeyException e) {
                    return Response.error(e.getMessage());
                }
            default:
                return Response.error("ERR syntax error");
        }
    }
}
