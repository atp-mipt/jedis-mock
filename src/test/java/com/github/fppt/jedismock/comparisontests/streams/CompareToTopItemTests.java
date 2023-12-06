package com.github.fppt.jedismock.comparisontests.streams;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.streams.RMStream;
import com.github.fppt.jedismock.datastructures.streams.StreamId;
import com.github.fppt.jedismock.exception.WrongStreamKeyException;
import com.github.fppt.jedismock.operations.streams.XAdd;
import com.github.fppt.jedismock.storage.RedisBase;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CompareToTopItemTests {
    @Test
    void compareTopIdTest() {
        RMStream stream = new RMStream();

        RedisBase mock = Mockito.mock(RedisBase.class);
        Mockito.when(mock.getStream(Mockito.any())).thenReturn(stream);

        XAdd xAdd = new XAdd(mock, Collections.singletonList(Slice.create("mock")));

        stream.updateLastId(new StreamId(3, 3));

        assertThrows(
                WrongStreamKeyException.class,
                () -> xAdd.compareWithTopKey(new StreamId(3, 3)),
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        );

        assertThrows(
                WrongStreamKeyException.class,
                () -> xAdd.compareWithTopKey(new StreamId(3, 2)),
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        );

        assertThrows(
                WrongStreamKeyException.class,
                () -> xAdd.compareWithTopKey(new StreamId(2, -1)),
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        );

        StreamId key = new StreamId(3, 4);
        assertDoesNotThrow(() -> assertSame(key, xAdd.compareWithTopKey(key)));
        StreamId newKey = new StreamId(4, 0);
        assertDoesNotThrow(() -> assertSame(newKey, xAdd.compareWithTopKey(newKey)));
    }

    @Test
    void fuzzingTest() {
        RMStream stream = new RMStream();

        RedisBase mock = Mockito.mock(RedisBase.class);
        Mockito.when(mock.getStream(Mockito.any())).thenReturn(stream);

        XAdd xAdd = new XAdd(mock, Collections.singletonList(Slice.create("mock")));

        for (int i = 0; i < 1000; ++i) {
            long first = (long) (Math.random() * (Long.MAX_VALUE - 1) + 1) * (Math.random() >= 0.5 ? 1 : -1);
            long second = (long) (Math.random() * (Long.MAX_VALUE - 1) + 1) * (Math.random() >= 0.5 ? 1 : -1);

            StreamId id = new StreamId(first, second);

            stream.updateLastId(id);

            StreamId key = new StreamId(first, second + 1);
            assertDoesNotThrow(() -> assertSame(key, xAdd.compareWithTopKey(key)));

            StreamId newKey = new StreamId(first + 1, 0);
            assertDoesNotThrow(() -> assertSame(newKey, xAdd.compareWithTopKey(newKey)));

            assertThrows(
                    WrongStreamKeyException.class,
                    () -> xAdd.compareWithTopKey(new StreamId(first, second - 1)),
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item"
            );

            assertThrows(
                    WrongStreamKeyException.class,
                    () -> xAdd.compareWithTopKey(new StreamId(first - 1, -1)),
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item"
            );
        }
    }
}
