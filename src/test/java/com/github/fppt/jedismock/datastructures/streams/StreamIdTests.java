package com.github.fppt.jedismock.datastructures.streams;

import com.github.fppt.jedismock.exception.WrongStreamKeyException;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

import static java.lang.Long.toUnsignedString;
import static org.junit.jupiter.api.Assertions.*;


public class StreamIdTests {
    @Test
    void equalsHashCodeTest(){
        EqualsVerifier.forClass(StreamId.class).verify();
    }

    @Test
    void zeroComparisonWithZeroTest() {
        StreamId zero = new StreamId(0, 0);
        assertThrows(
                WrongStreamKeyException.class,
                zero::compareToZero,
                "ERR The ID specified in XADD must be greater than 0-0"
        );
    }

    @Test
    void zeroComparisonWithPositiveKeysTest() {
        StreamId other = new StreamId(0, 1);
        assertDoesNotThrow(() -> assertSame(other, other.compareToZero()));

        StreamId newOther = new StreamId(1, 0);
        assertDoesNotThrow(() -> assertSame(newOther, newOther.compareToZero()));

        StreamId newestOther = new StreamId(1, 1);
        assertDoesNotThrow(() -> assertSame(newestOther, newestOther.compareToZero()));
    }

    @Test
    void zeroComparisonWithNegativeKeysTest() {
        StreamId other = new StreamId(0, -1);
        assertDoesNotThrow(() -> assertSame(other, other.compareToZero()));

        StreamId newOther = new StreamId(-1, 0);
        assertDoesNotThrow(() -> assertSame(newOther, newOther.compareToZero()));

        StreamId newestOther = new StreamId(-1, -1);
        assertDoesNotThrow(() -> assertSame(newestOther, newestOther.compareToZero()));
    }

    @Test
    void incrementMaxKeyTest() {
        StreamId maxKey = new StreamId(-1, -1);
        assertThrows(
                WrongStreamKeyException.class,
                maxKey::increment,
                "ERR invalid start ID for the interval"
        );
    }

    @Test
    void incrementTest() {
        assertDoesNotThrow(() -> assertEquals(
                new StreamId(1234567890, 1234567891),
                new StreamId(1234567890, 1234567890).increment()
        ));

        assertDoesNotThrow(() -> assertEquals(
                new StreamId(1, 0),
                new StreamId(0, -1).increment()
        ));

        assertDoesNotThrow(() -> assertEquals(
                new StreamId(-1234567891, -1234567890),
                new StreamId(-1234567891, -1234567891).increment()
        ));
    }

    @Test
    void decrementMinKeyTest() {
        StreamId minKey = new StreamId(0, 0);
        assertThrows(
                WrongStreamKeyException.class,
                minKey::decrement,
                "ERR invalid end ID for the interval"
        );
    }

    @Test
    void decrementTest() {
        assertDoesNotThrow(() -> assertEquals(
                new StreamId(1234567890, 1234567890),
                new StreamId(1234567890, 1234567891).decrement()
        ));

        assertDoesNotThrow(() -> assertEquals(
                new StreamId(0, -1),
                new StreamId(1, 0).decrement()
        ));

        assertDoesNotThrow(() -> assertEquals(
                new StreamId(-1234567891, -1234567891),
                new StreamId(-1234567891, -1234567890).decrement()
        ));
    }

    @Test
    void incrementDecrementFuzzingTest() {
        for (int i = 0; i < 1000; ++i) {
            if (Math.random() >= 0.9) {
                long first = (long) (Math.random() * Long.MAX_VALUE) * (Math.random() >= 0.5 ? 1 : -1);
                long second = (long) (Math.random() * (Long.MAX_VALUE - 1) + 1) * (Math.random() >= 0.5 ? 1 : -1);

                assertDoesNotThrow(
                        () -> assertEquals(
                                new StreamId(first, second + 1),
                                new StreamId(first, second).increment()
                        )
                );

                assertDoesNotThrow(
                        () -> assertEquals(
                                new StreamId(first, second - 1),
                                new StreamId(first, second).decrement()
                        )
                );
            } else {
                long first = (long) (Math.random() * (Long.MAX_VALUE - 1) + 1) * (Math.random() >= 0.5 ? 1 : -1);

                assertDoesNotThrow(
                        () -> assertEquals(
                                new StreamId(first + 1, 0),
                                new StreamId(first, -1).increment()
                        )
                );

                assertDoesNotThrow(
                        () -> assertEquals(
                                new StreamId(first - 1, -1),
                                new StreamId(first, 0).decrement()
                        )
                );
            }
        }
    }

    @Test
    void toStringFuzzingTest() {
        assertEquals(
                "18446744072474983726-18446744072474983726",
                new StreamId(-1234567890, -1234567890).toString()
        );

        assertEquals(
                "0-0",
                new StreamId().toString()
        );

        assertEquals(
                "18446744073709551615-1234567890",
                new StreamId(-1, 1234567890).toString()
        );

        assertEquals(
                "1234567890-18446744073709551615",
                new StreamId(1234567890, -1).toString()
        );
    }

    @Test
    void comparisonTest() {
        StreamId zero = new StreamId();
        assertEquals(0, zero.compareTo(new StreamId()));

        for (int i = 0; i < 1000; ++i) {
            long first = (long) (Math.random() * Long.MAX_VALUE) * (Math.random() >= 0.5 ? 1 : -1);
            long second = (long) (Math.random() * (Long.MAX_VALUE - 1) + 1) * (Math.random() >= 0.5 ? 1 : -1);

            assertTrue(
                    zero.compareTo(new StreamId(first, second)) < 0,
                    "0-0 >= " + new StreamId(first, second)
            );
        }

        StreamId ones = new StreamId(1, 1);
        assertTrue(ones.compareTo(new StreamId(0, -1)) > 0);
        assertTrue(ones.compareTo(new StreamId(1, 0)) > 0);
        assertEquals(0, ones.compareTo(new StreamId(1, 1)));
        assertTrue(ones.compareTo(new StreamId(1, 2)) < 0);
        assertTrue(ones.compareTo(new StreamId(2, 0)) < 0);

        for (int i = 0; i < 1000; ++i) {
            long first = (long) (Math.random() * (Long.MAX_VALUE - 1) + 1) * (Math.random() >= 0.5 ? 1 : -1);
            long second = (long) (Math.random() * (Long.MAX_VALUE - 1) + 1) * (Math.random() >= 0.5 ? 1 : -1);

            StreamId id = new StreamId(first, second);

            assertTrue(
                    id.compareTo(new StreamId(first, second + 1)) < 0,
                    "Id " + id + " >= " + new StreamId(first, second + 1)
            );
            assertTrue(
                    id.compareTo(new StreamId(first + 1, 0)) < 0,
                    "Id " + id + " >= " + new StreamId(first + 1, 0)
            );
            assertTrue(
                    id.compareTo(new StreamId(first, second - 1)) > 0,
                    "Id " + id + " <= " + new StreamId(first, second - 1)
            );
            assertTrue(
                    id.compareTo(new StreamId(first - 1, -1)) > 0,
                    "Id " + id + " <= " + new StreamId(first - 1, -1)
            );
        }
    }

    @Test
    void constructorInvalidIdsTest() {
        assertThrows(
                WrongStreamKeyException.class,
                () -> new StreamId("a"),
                "ERR Invalid stream ID specified as stream command argument"
        );

        assertThrows(
                WrongStreamKeyException.class,
                () -> new StreamId("0-0-0"),
                "ERR Invalid stream ID specified as stream command argument"
        );

        assertThrows(
                WrongStreamKeyException.class,
                () -> new StreamId("a-0"),
                "ERR Invalid stream ID specified as stream command argument"
        );

        assertThrows(
                WrongStreamKeyException.class,
                () -> new StreamId("0-a"),
                "ERR Invalid stream ID specified as stream command argument"
        );
    }

    @Test
    void constructorFuzzingTest() {
        for (int i = 0; i < 1000; ++i) {
            long first = (long) (Math.random() * (Long.MAX_VALUE - 1) + 1) * (Math.random() >= 0.5 ? 1 : -1);
            long second = (long) (Math.random() * (Long.MAX_VALUE - 1) + 1) * (Math.random() >= 0.5 ? 1 : -1);

            assertDoesNotThrow(() -> assertEquals(
                    new StreamId(first, second),
                    new StreamId(toUnsignedString(first) + "-" + toUnsignedString(second))
            ));
        }

        for (int i = 0; i < 1000; ++i) {
            long first = (long) (Math.random() * (Long.MAX_VALUE - 1) + 1) * (Math.random() >= 0.5 ? 1 : -1);

            assertDoesNotThrow(() -> assertEquals(
                    new StreamId(first, 0),
                    new StreamId(toUnsignedString(first))
            ));
        }
    }
}
