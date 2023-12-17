package com.github.fppt.jedismock.datastructures.streams;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.WrongStreamKeyException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import static java.lang.Long.toUnsignedString;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RMStreamTests {
    RMStream stream;

    @BeforeEach
    void setup() {
        stream = new RMStream();
    }

    @Test
    void updateLastIdTest() {
        stream.updateLastId(new StreamId(1, 1));
        assertEquals(new StreamId(1, 1), stream.getLastId());
    }

    @Test
    void whenLongMaxIsLastId_ensureThrowsExceptionOnAsteriskInput() {
        stream.updateLastId(new StreamId(-1, -1));
        assertThrows(
                WrongStreamKeyException.class,
                () -> stream.replaceAsterisk(Slice.create("*")),
                "ERR The stream has exhausted the last possible ID, unable to add more items"
        );
    }

    @Test
    void whenLongMaxIsSecondPartLastId_ensureIncrementsCorrectlyOnAsteriskInput() {
        stream.updateLastId(new StreamId(3, -1));
        assertDoesNotThrow(() -> assertEquals(Slice.create("4-0"), stream.replaceAsterisk(Slice.create("*"))));
    }

    @Test
    void whenInvokeReplaceAsterisk_ensureWorksFineWithOnAsteriskInput() {
        assertDoesNotThrow(() -> assertEquals(Slice.create("0-1"), stream.replaceAsterisk(Slice.create("*"))));

        stream.updateLastId(new StreamId(1, 1));
        assertDoesNotThrow(() -> assertEquals(Slice.create("1-2"), stream.replaceAsterisk(Slice.create("*"))));
    }

    @Test
    void whenLongMaxIsSecondPartLastId_ensureThrowsExceptionOnNumberWithAsteriskInput() {
        stream.updateLastId(new StreamId(3, -1));
        assertThrows(
                WrongStreamKeyException.class,
                () -> stream.replaceAsterisk(Slice.create("3-*")),
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        );
    }

    @Test
    void whenGivenNumberIsSmallerThanFirstPartLastId_ensureThrowsExceptionOnNumberWithAsteriskInput() {
        stream.updateLastId(new StreamId(1, 0));
        assertThrows(
                WrongStreamKeyException.class,
                () -> stream.replaceAsterisk(Slice.create("0-*")),
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        );
    }

    @Test
    void whenNotANumberWasGivenWithAsterisk_ensureThrowsException() {
        assertThrows(
                WrongStreamKeyException.class,
                () -> stream.replaceAsterisk(Slice.create("e-*")),
                "ERR Invalid stream ID specified as stream command argument"
        );
    }

    @Test
    void whenInvokeReplaceAsterisk_ensureWorksFineWithOnNumberWithAsteriskInput() {
        assertDoesNotThrow(() -> assertEquals(Slice.create("1-0"), stream.replaceAsterisk(Slice.create("1-*"))));

        stream.updateLastId(new StreamId(1, 3));
        assertDoesNotThrow(() -> assertEquals(Slice.create("1-4"), stream.replaceAsterisk(Slice.create("1-*"))));
    }

    @Test
    void whenKeyIsCorrect_ensureReplaceAsteriskDoesNotChangeIt() {
        Slice id = Slice.create("1-1");
        assertDoesNotThrow(() -> assertSame(id, stream.replaceAsterisk(id))); // Reference equality
    }

    @Test
    void whenKeyIsIncorrect_ensureReplaceAsteriskDoesNotChangeIt() {
        Slice id = Slice.create("qwerty");
        assertDoesNotThrow(() -> assertSame(id, stream.replaceAsterisk(id))); // Reference equality
    }

    @Test
    void stressAsteriskTest() {
        for (int i = 0; i < 1000; ++i) {
            if (Math.random() >= 0.9) {
                long second = (long) (Math.random() * Long.MAX_VALUE) * (Math.random() >= 0.5 ? 1 : -1);
                long first = (long) (Math.random() * Long.MAX_VALUE) * (Math.random() >= 0.5 ? 1 : -1);

                stream.updateLastId(new StreamId(first, second));

                assertDoesNotThrow(
                        () -> assertEquals(
                                Slice.create(toUnsignedString(first) + "-" + toUnsignedString(second + 1)),
                                stream.replaceAsterisk(Slice.create(toUnsignedString(first) + "-*"))
                        )
                );

                assertDoesNotThrow(
                        () -> assertEquals(
                                Slice.create(toUnsignedString(first) + "-" + toUnsignedString(second + 1)),
                                stream.replaceAsterisk(Slice.create("*"))
                        )
                );
            } else {
                long second = -1;
                long first = (long) (Math.random() * Long.MAX_VALUE) * (Math.random() >= 0.5 ? 1 : -1);

                stream.updateLastId(new StreamId(first, second));

                assertThrows(
                        WrongStreamKeyException.class,
                        () -> stream.replaceAsterisk(Slice.create(toUnsignedString(first) + "-*")),
                        "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                );

                assertDoesNotThrow(
                        () -> assertEquals(
                                Slice.create(toUnsignedString(first + 1) + "-0"),
                                stream.replaceAsterisk(Slice.create("*"))
                        )
                );
            }
        }
    }
}
