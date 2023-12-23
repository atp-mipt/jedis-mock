package com.github.fppt.jedismock.datastructures.streams;

import com.github.fppt.jedismock.exception.WrongStreamKeyException;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

import static java.lang.Long.toUnsignedString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class StreamIdTests {
    @Test
    void equalsHashCodeTest(){
        EqualsVerifier.forClass(StreamId.class).verify();
    }

    @Test
    void zeroComparisonWithZeroTest() {
        StreamId zero = new StreamId(0, 0);
        assertThat(zero.isZero()).isTrue();
    }

    @Test
    void zeroComparisonWithPositiveKeysTest() {
        StreamId other = new StreamId(0, 1);
        assertThat(other.isZero()).isFalse();

        StreamId newOther = new StreamId(1, 0);
        assertThat(newOther.isZero()).isFalse();

        StreamId newestOther = new StreamId(1, 1);
        assertThat(newestOther.isZero()).isFalse();
    }

    @Test
    void zeroComparisonWithNegativeKeysTest() {
        StreamId other = new StreamId(0, -1);
        assertThat(other.isZero()).isFalse();

        StreamId newOther = new StreamId(-1, 0);
        assertThat(newOther.isZero()).isFalse();

        StreamId newestOther = new StreamId(-1, -1);
        assertThat(newestOther.isZero()).isFalse();
    }

    @Test
    void incrementMaxKeyTest() {
        StreamId maxKey = new StreamId(-1, -1);
        assertThatThrownBy(
                maxKey::increment
        )
                .isInstanceOf(WrongStreamKeyException.class)
                .hasMessage("ERR invalid start ID for the interval");
    }

    @Test
    void incrementTest() {
        assertThatCode(() -> assertThat(
                new StreamId(1234567890, 1234567890).increment()
        ).isEqualTo(
                new StreamId(1234567890, 1234567891)
        )).doesNotThrowAnyException();

        assertThatCode(() -> assertThat(
                new StreamId(0, -1).increment()
        ).isEqualTo(
                new StreamId(1, 0)
        )).doesNotThrowAnyException();

        assertThatCode(() -> assertThat(
                new StreamId(-1234567891, -1234567891).increment()
        ).isEqualTo(
                new StreamId(-1234567891, -1234567890)
        )).doesNotThrowAnyException();
    }

    @Test
    void decrementMinKeyTest() {
        StreamId minKey = new StreamId(0, 0);
        assertThatThrownBy(
                minKey::decrement
        )
                .isInstanceOf(WrongStreamKeyException.class)
                .hasMessage("ERR invalid end ID for the interval");
    }

    @Test
    void decrementTest() {
        assertThatCode(() -> assertThat(
                new StreamId(1234567890, 1234567891).decrement()
        ).isEqualTo(
                new StreamId(1234567890, 1234567890)
        )).doesNotThrowAnyException();

        assertThatCode(() -> assertThat(
                new StreamId(1, 0).decrement()
        ).isEqualTo(
                new StreamId(0, -1)
        )).doesNotThrowAnyException();

        assertThatCode(() -> assertThat(
                new StreamId(-1234567891, -1234567890).decrement()
        ).isEqualTo(
                new StreamId(-1234567891, -1234567891)
        )).doesNotThrowAnyException();
    }

    @Test
    void incrementDecrementStressTest() {
        for (int i = 0; i < 1000; ++i) {
            if (Math.random() >= 0.9) {
                long first = (long) (Math.random() * Long.MAX_VALUE) * (Math.random() >= 0.5 ? 1 : -1);
                long second = (long) (Math.random() * (Long.MAX_VALUE - 1) + 1) * (Math.random() >= 0.5 ? 1 : -1);

                assertThatCode(
                        () -> assertThat(
                                new StreamId(first, second).increment()
                        ).isEqualTo(
                                new StreamId(first, second + 1)
                        )
                ).doesNotThrowAnyException();

                assertThatCode(
                        () -> assertThat(
                                new StreamId(first, second).decrement()
                        ).isEqualTo(
                                new StreamId(first, second - 1)
                        )
                ).doesNotThrowAnyException();
            } else {
                long first = (long) (Math.random() * (Long.MAX_VALUE - 1) + 1) * (Math.random() >= 0.5 ? 1 : -1);

                assertThatCode(
                        () -> assertThat(
                                new StreamId(first, -1).increment()
                        ).isEqualTo(
                                new StreamId(first + 1, 0)
                        )
                ).doesNotThrowAnyException();

                assertThatCode(
                        () -> assertThat(
                                new StreamId(first, 0).decrement()
                        ).isEqualTo(
                                new StreamId(first - 1, -1)
                        )
                ).doesNotThrowAnyException();
            }
        }
    }

    @Test
    void toStringStressTest() {
        assertThat(
                new StreamId(-1234567890, -1234567890).toString()
        ).isEqualTo("18446744072474983726-18446744072474983726");

        assertThat(
                new StreamId().toString()
        ).isEqualTo("0-0");

        assertThat(
                new StreamId(-1, 1234567890).toString()
        ).isEqualTo("18446744073709551615-1234567890");

        assertThat(
                new StreamId(1234567890, -1).toString()
        ).isEqualTo("1234567890-18446744073709551615");
    }

    @Test
    void comparisonTest() {
        StreamId zero = new StreamId();
        assertThat(zero.compareTo(new StreamId())).isEqualTo(0);

        for (int i = 0; i < 1000; ++i) {
            long first = (long) (Math.random() * Long.MAX_VALUE) * (Math.random() >= 0.5 ? 1 : -1);
            long second = (long) (Math.random() * (Long.MAX_VALUE - 1) + 1) * (Math.random() >= 0.5 ? 1 : -1);

            assertThat(
                    zero.compareTo(new StreamId(first, second)) < 0
            ).isTrue();
        }

        StreamId ones = new StreamId(1, 1);
        assertThat(ones.compareTo(new StreamId(0, -1)) > 0).isTrue();
        assertThat(ones.compareTo(new StreamId(1, 0)) > 0).isTrue();
        assertThat(ones.compareTo(new StreamId(1, 1))).isEqualTo(0);
        assertThat(ones.compareTo(new StreamId(1, 2)) < 0).isTrue();
        assertThat(ones.compareTo(new StreamId(2, 0)) < 0).isTrue();

        for (int i = 0; i < 1000; ++i) {
            long first = (long) (Math.random() * (Long.MAX_VALUE - 1) + 1) * (Math.random() >= 0.5 ? 1 : -1);
            long second = (long) (Math.random() * (Long.MAX_VALUE - 1) + 1) * (Math.random() >= 0.5 ? 1 : -1);

            StreamId id = new StreamId(first, second);

            assertThat(
                    id.compareTo(new StreamId(first, second + 1)) < 0
            ).isTrue();
            assertThat(
                    id.compareTo(new StreamId(first + 1, 0)) < 0
            ).isTrue();
            assertThat(
                    id.compareTo(new StreamId(first, second - 1)) > 0
            ).isTrue();
            assertThat(
                    id.compareTo(new StreamId(first - 1, -1)) > 0
            ).isTrue();
        }
    }

    @Test
    void constructorInvalidIdsTest() {
        assertThatThrownBy(
                () -> new StreamId("a")
        )
                .isInstanceOf(WrongStreamKeyException.class)
                .hasMessage("ERR Invalid stream ID specified as stream command argument");

        assertThatThrownBy(
                () -> new StreamId("0-0-0")
        )
                .isInstanceOf(WrongStreamKeyException.class)
                .hasMessage("ERR Invalid stream ID specified as stream command argument");

        assertThatThrownBy(
                () -> new StreamId("a-0")
        )
                .isInstanceOf(WrongStreamKeyException.class)
                .hasMessage("ERR Invalid stream ID specified as stream command argument");

        assertThatThrownBy(
                () -> new StreamId("0-a")
        )
                .isInstanceOf(WrongStreamKeyException.class)
                .hasMessage("ERR Invalid stream ID specified as stream command argument");
    }

    @Test
    void constructorStressTest() {
        for (int i = 0; i < 1000; ++i) {
            long first = (long) (Math.random() * (Long.MAX_VALUE - 1) + 1) * (Math.random() >= 0.5 ? 1 : -1);
            long second = (long) (Math.random() * (Long.MAX_VALUE - 1) + 1) * (Math.random() >= 0.5 ? 1 : -1);

            assertThatCode(() -> assertThat(
                    new StreamId(toUnsignedString(first) + "-" + toUnsignedString(second))
            ).isEqualTo(
                    new StreamId(first, second)
            )).doesNotThrowAnyException();
        }

        for (int i = 0; i < 1000; ++i) {
            long first = (long) (Math.random() * (Long.MAX_VALUE - 1) + 1) * (Math.random() >= 0.5 ? 1 : -1);

            assertThatCode(() -> assertThat(
                    new StreamId(toUnsignedString(first))
            ).isEqualTo(
                    new StreamId(first, 0)
            )).doesNotThrowAnyException();
        }
    }
}
