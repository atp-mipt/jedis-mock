package com.github.fppt.jedismock.operations.streams;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.streams.LinkedMap;
import com.github.fppt.jedismock.datastructures.streams.LinkedMapIterator;
import com.github.fppt.jedismock.datastructures.streams.StreamId;
import com.github.fppt.jedismock.exception.WrongStreamKeyException;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.NEGATIVE_TIMEOUT_ERROR;
import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.SYNTAX_ERROR;
import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.XREAD_ARGS_ERROR;

/**
 * XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id
 *   [id ...]<br>
 * Supported options: COUNT<br>
 * Unsupported options (TODO): BLOCK
 */
@RedisCommand("xread")
public class XRead extends AbstractRedisOperation {
    private final Object lock;
    private final boolean isInTransaction;
    public XRead(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        lock = state.lock();
        isInTransaction = state.isTransactionModeOn();
    }

    @Override
    protected Slice response() {
        if (params().size() < 3) {
            return Response.invalidArgumentsCountError("xread");
        }

        int streamInd = 0;
        int count;
        long blockTimeNanosec = 0;

        if (params().get(streamInd).toString().equalsIgnoreCase("count")) {
            count = Integer.parseInt(params().get(++streamInd).toString());
            ++streamInd;
        } else {
            count = Integer.MAX_VALUE;
        }

        if (params().get(streamInd).toString().equalsIgnoreCase("block")) {
            blockTimeNanosec = Long.parseLong(params().get(++streamInd).toString()) * 1_000_000;

            if (blockTimeNanosec < 0) {
                return Response.error(NEGATIVE_TIMEOUT_ERROR);
            }
            ++streamInd;

            // TODO currently ignored
        }

        if (!params().get(streamInd++).toString().equalsIgnoreCase("streams")) {
            return Response.error(SYNTAX_ERROR);
        }

        if ((params().size() - streamInd) % 2 != 0) {
            return Response.error(XREAD_ARGS_ERROR);
        }

        int streamsCount = (params().size() - streamInd) / 2;

        LinkedMap<Slice, StreamId> mapKeyToBeginEntryId = new LinkedMap<>();

        /* Mapping all stream ids */
        for (int i = 0; i < streamsCount; ++i) {
            Slice key = params().get(streamInd + i);
            Slice id = params().get(streamInd + streamsCount + i);

            /* check whether stream exists */
            if (!base().exists(key)) {
                continue;
            }

            try {
                mapKeyToBeginEntryId.append(
                        key,
                        id.toString().equalsIgnoreCase("$")
                                ? getStreamFromBaseOrCreateEmpty(key).getStoredData().getTail()
                                : new StreamId(id)
                );
            } catch (WrongStreamKeyException e) {
                return Response.error(e.getMessage());
            }
        }

        List<Slice> output = new ArrayList<>();

        /* Blocking */
        long waitEnd = System.nanoTime() + blockTimeNanosec;
        long waitTimeNanos;
        if (blockTimeNanosec != 0) {
            try {
                while (!isInTransaction && (waitTimeNanos = waitEnd - System.nanoTime()) >= 0) {
                    lock.wait(waitTimeNanos / 1_000_000, (int) (waitTimeNanos % 1_000_000));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Response.NULL;
            }
        }

        /* Response */
        mapKeyToBeginEntryId.forEach((key, id) -> {
            LinkedMap<StreamId, LinkedMap<Slice, Slice>> map = getStreamFromBaseOrCreateEmpty(key).getStoredData();
            LinkedMapIterator<StreamId, LinkedMap<Slice, Slice>> it = map.iterator();

            if (id.compareTo(map.getTail()) >= 0) {
                return;
            }

            try {
                id.increment();
            } catch (WrongStreamKeyException e) {
                return; // impossible as 0xFFFFFFFFFFFFFFFF is greater or equal to all keys
            }

            it.findFirstSuitable(id);

            List<Slice> data = new ArrayList<>();
            int addedEntries = 1;

            while (it.hasNext() && addedEntries++ <= count) {
                Map.Entry<StreamId, LinkedMap<Slice, Slice>> entry = it.next();

                List<Slice> values = new ArrayList<>();

                entry.getValue().forEach((k, v) -> {
                    values.add(Response.bulkString(k));
                    values.add(Response.bulkString(v));
                });

                data.add(Response.array(List.of(
                        Response.bulkString(entry.getKey().toSlice()),
                        Response.array(values)
                )));
            }

            output.add(Response.array(List.of(
                    Response.bulkString(key),
                    Response.array(data)
            )));
        });

        return Response.array(output);
    }
}
