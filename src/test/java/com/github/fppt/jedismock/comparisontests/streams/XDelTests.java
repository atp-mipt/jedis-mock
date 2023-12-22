package com.github.fppt.jedismock.comparisontests.streams;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.XAddParams;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class XDelTests {
    @BeforeEach
    void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    void whenTopEntryIsDeleted_ensureTopIdDoesNotDecrease(Jedis jedis) {
        jedis.xadd("s", XAddParams.xAddParams().id("0-1"), ImmutableMap.of("a", "b"));

        assertThat(jedis.xdel("s", new StreamEntryID(0, 1))).isEqualTo(1);

        assertThatThrownBy(
                () -> jedis.xadd("s", XAddParams.xAddParams().id("0-1"), ImmutableMap.of("a", "b"))
        )
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR The ID specified in XADD is equal or smaller than the target stream top item");
    }

    @TestTemplate
    void whenTopEntryIsDeleted_ensureNextEntryIdIsIncremental(Jedis jedis) {
        jedis.xadd("s", XAddParams.xAddParams().id("2-1"), ImmutableMap.of("a", "b"));

        assertThat(jedis.xdel("s", new StreamEntryID(2, 1))).isEqualTo(1);

        assertThat(
                jedis.xadd("s", XAddParams.xAddParams().id("2-*"), ImmutableMap.of("a", "b"))
        ).isEqualTo(new StreamEntryID(2, 2));
    }

    @TestTemplate
    void whenAllElementsAreRemoved_ensureStreamIsNotAlsoRemoved(Jedis jedis) {
        jedis.xadd("s", XAddParams.xAddParams().id("0-1"), ImmutableMap.of("a", "b"));
        jedis.xadd("s", XAddParams.xAddParams().id("0-2"), ImmutableMap.of("a", "b"));
        jedis.xadd("s", XAddParams.xAddParams().id("1-0"), ImmutableMap.of("a", "b"));
        jedis.xadd("s", XAddParams.xAddParams().id("1-1"), ImmutableMap.of("a", "b"));

        assertThat(
                jedis.xdel(
                        "s",
                        new StreamEntryID(0, 1),
                        new StreamEntryID(0, 2),
                        new StreamEntryID(1, 0),
                        new StreamEntryID(1, 1)
                )
        ).isEqualTo(4);

        assertThat(jedis.exists("s")).isTrue();
    }

    @TestTemplate
    void stressTest(Jedis jedis) {
        StreamEntryID[] ids = new StreamEntryID[1000];

        for (int i = 0; i < 1000; ++i) {
            ids[i] = new StreamEntryID(i + 1);
            jedis.xadd("s", ids[i], ImmutableMap.of("a",  "b"));
        }

        assertThat(jedis.xdel("s", ids)).isEqualTo(1000);
        assertThat(jedis.xlen("s")).isEqualTo(0);
    }
}
