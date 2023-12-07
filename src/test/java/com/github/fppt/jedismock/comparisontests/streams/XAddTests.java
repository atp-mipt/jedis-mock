package com.github.fppt.jedismock.comparisontests.streams;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
            Map.of("a", "b")
        );

        assertFalse(jedis.exists("s"));

        jedis.xadd("s", XAddParams.xAddParams().id(1), Map.of("a", "b"));

        jedis.xadd(
                "s",
                XAddParams.xAddParams().noMkStream().approximateTrimming().minId("0-0").limit(3).id(2),
                Map.of("a", "b")
        );

        assertTrue(jedis.exists("s"));
        assertEquals(2, jedis.xlen("s"));
    }

    @TestTemplate
    void whenLimitOptionIsUsedWithoutTilda_ensureThrowsException(Jedis jedis) {
        jedis.xadd(
                "s",
                XAddParams.xAddParams().noMkStream().approximateTrimming().maxLen(3).limit(3).id(1),
                Map.of("a", "b")
        );

        assertFalse(jedis.exists("s"));

        jedis.xadd("s", XAddParams.xAddParams().id(1), Map.of("a", "b"));

        jedis.xadd(
                "s",
                XAddParams.xAddParams().noMkStream().approximateTrimming().minId("0-0").limit(3).id(2),
                Map.of("a", "b")
        );

        assertTrue(jedis.exists("s"));
        assertEquals(2, jedis.xlen("s"));
    }

    @TestTemplate
    void whenLimitIsProvided_ensureDoesNotExceed(Jedis jedis) {
        jedis.xadd("s", new StreamEntryID(0, 1), Map.of("a", "b"));
        jedis.xadd("s", new StreamEntryID(0, 2), Map.of("a", "b"));
        jedis.xadd("s", new StreamEntryID(1, 0), Map.of("a", "b"));
        jedis.xadd("s", new StreamEntryID(1, 1), Map.of("a", "b"));

        jedis.xadd(
                "s",
                XAddParams.xAddParams().approximateTrimming().minId("2").limit(2),
                Map.of("a", "b")
        );

        long len = jedis.xlen("s");
        assertTrue(
                len >= 3,
                "Expected " + len + " >= 3"
        );
    }

    @TestTemplate
    void whenAddedIdIsLowerThanMinId_ensureStreamIsEmpty(Jedis jedis) {
        jedis.xadd("s", new StreamEntryID(0, 1), Map.of("a", "b"));
        jedis.xadd("s", new StreamEntryID(0, 2), Map.of("a", "b"));
        jedis.xadd("s", new StreamEntryID(1, 0), Map.of("a", "b"));
        jedis.xadd("s", new StreamEntryID(1, 1), Map.of("a", "b"));

        jedis.xadd(
                "s",
                XAddParams.xAddParams().exactTrimming().minId("2").id("1-2"),
                Map.of("a", "b")
        );

        assertEquals(0, jedis.xlen("s"));
    }

    @TestTemplate
    void whenNomkstream_ensureReturnsNull(Jedis jedis) {
        assertNull(jedis.xadd(
                "s",
                XAddParams.xAddParams().noMkStream().id("*"),
                Map.of("a", "b")
        ));

        assertNull(jedis.xadd(
                "s",
                XAddParams.xAddParams().minId("1").noMkStream().id("*"),
                Map.of("a", "b")
        ));

        assertNull(jedis.xadd(
                "s",
                XAddParams.xAddParams().maxLen(1).noMkStream().id("*"),
                Map.of("a", "b")
        ));

        assertNull(jedis.xadd(
                "s",
                XAddParams.xAddParams().minId("1").exactTrimming().noMkStream().id("*"),
                Map.of("a", "b")
        ));

        assertNull(jedis.xadd(
                "s",
                XAddParams.xAddParams().maxLen(1).exactTrimming().noMkStream().id("*"),
                Map.of("a", "b")
        ));

        assertNull(jedis.xadd(
                "s",
                XAddParams.xAddParams().maxLen(1).approximateTrimming().noMkStream().id("*"),
                Map.of("a", "b")
        ));

        assertNull(jedis.xadd(
                "s",
                XAddParams.xAddParams().minId("1").approximateTrimming().noMkStream().id("*"),
                Map.of("a", "b")
        ));

        assertNull(jedis.xadd(
                "s",
                XAddParams.xAddParams().minId("1").approximateTrimming().limit(3).noMkStream().id("*"),
                Map.of("a", "b")
        ));

        assertNull(jedis.xadd(
                "s",
                XAddParams.xAddParams().maxLen(1).approximateTrimming().limit(3).noMkStream().id("*"),
                Map.of("a", "b")
        ));
    }
}
