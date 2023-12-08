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

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class XAddTests {
    @BeforeEach
    void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    void whenAllOptionsUsed_ensureWorksCorrect(Jedis jedis) {
        jedis.xadd(
            "s",
            XAddParams.xAddParams().noMkStream().approximateTrimming().maxLen(3).limit(3).id(1),
            ImmutableMap.of("a", "b")
        );

        assertFalse(jedis.exists("s"));

        jedis.xadd("s", XAddParams.xAddParams().id(1), ImmutableMap.of("a", "b"));

        jedis.xadd(
                "s",
                XAddParams.xAddParams().noMkStream().approximateTrimming().minId("0-0").limit(3).id(2),
                ImmutableMap.of("a", "b")
        );

        assertTrue(jedis.exists("s"));
        assertEquals(2, jedis.xlen("s"));
    }

    @TestTemplate
    void whenLimitOptionIsUsedWithoutTilda_ensureThrowsException(Jedis jedis) {
        jedis.xadd(
                "s",
                XAddParams.xAddParams().noMkStream().approximateTrimming().maxLen(3).limit(3).id(1),
                ImmutableMap.of("a", "b")
        );

        assertFalse(jedis.exists("s"));

        jedis.xadd("s", XAddParams.xAddParams().id(1), ImmutableMap.of("a", "b"));

        jedis.xadd(
                "s",
                XAddParams.xAddParams().noMkStream().approximateTrimming().minId("0-0").limit(3).id(2),
                ImmutableMap.of("a", "b")
        );

        assertTrue(jedis.exists("s"));
        assertEquals(2, jedis.xlen("s"));
    }

    @TestTemplate
    void whenLimitIsProvided_ensureDoesNotExceed(Jedis jedis) {
        jedis.xadd("s", new StreamEntryID(0, 1), ImmutableMap.of("a", "b"));
        jedis.xadd("s", new StreamEntryID(0, 2), ImmutableMap.of("a", "b"));
        jedis.xadd("s", new StreamEntryID(1, 0), ImmutableMap.of("a", "b"));
        jedis.xadd("s", new StreamEntryID(1, 1), ImmutableMap.of("a", "b"));

        jedis.xadd(
                "s",
                XAddParams.xAddParams().approximateTrimming().minId("2").limit(2),
                ImmutableMap.of("a", "b")
        );

        long len = jedis.xlen("s");
        assertTrue(
                len >= 3,
                "Expected " + len + " >= 3"
        );
    }

    @TestTemplate
    void whenAddedIdIsLowerThanMinId_ensureStreamIsEmpty(Jedis jedis) {
        jedis.xadd("s", new StreamEntryID(0, 1), ImmutableMap.of("a", "b"));
        jedis.xadd("s", new StreamEntryID(0, 2), ImmutableMap.of("a", "b"));
        jedis.xadd("s", new StreamEntryID(1, 0), ImmutableMap.of("a", "b"));
        jedis.xadd("s", new StreamEntryID(1, 1), ImmutableMap.of("a", "b"));

        jedis.xadd(
                "s",
                XAddParams.xAddParams().exactTrimming().minId("2").id("1-2"),
                ImmutableMap.of("a", "b")
        );

        assertEquals(0, jedis.xlen("s"));
    }

    @TestTemplate
    void whenNomkstream_ensureReturnsNull(Jedis jedis) {
        assertNull(jedis.xadd(
                "s",
                XAddParams.xAddParams().noMkStream().id("*"),
                ImmutableMap.of("a", "b")
        ));

        assertNull(jedis.xadd(
                "s",
                XAddParams.xAddParams().minId("1").noMkStream().id("*"),
                ImmutableMap.of("a", "b")
        ));

        assertNull(jedis.xadd(
                "s",
                XAddParams.xAddParams().maxLen(1).noMkStream().id("*"),
                ImmutableMap.of("a", "b")
        ));

        assertNull(jedis.xadd(
                "s",
                XAddParams.xAddParams().minId("1").exactTrimming().noMkStream().id("*"),
                ImmutableMap.of("a", "b")
        ));

        assertNull(jedis.xadd(
                "s",
                XAddParams.xAddParams().maxLen(1).exactTrimming().noMkStream().id("*"),
                ImmutableMap.of("a", "b")
        ));

        assertNull(jedis.xadd(
                "s",
                XAddParams.xAddParams().maxLen(1).approximateTrimming().noMkStream().id("*"),
                ImmutableMap.of("a", "b")
        ));

        assertNull(jedis.xadd(
                "s",
                XAddParams.xAddParams().minId("1").approximateTrimming().noMkStream().id("*"),
                ImmutableMap.of("a", "b")
        ));

        assertNull(jedis.xadd(
                "s",
                XAddParams.xAddParams().minId("1").approximateTrimming().limit(3).noMkStream().id("*"),
                ImmutableMap.of("a", "b")
        ));

        assertNull(jedis.xadd(
                "s",
                XAddParams.xAddParams().maxLen(1).approximateTrimming().limit(3).noMkStream().id("*"),
                ImmutableMap.of("a", "b")
        ));
    }

    @TestTemplate
    void whenZeroId_ensureThrowsException(Jedis jedis) {
        assertThrows(
                JedisDataException.class,
                () -> jedis.xadd("s", XAddParams.xAddParams().id("0"), ImmutableMap.of("a", "b")),
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        );
    }
}
