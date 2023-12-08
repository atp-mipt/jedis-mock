package com.github.fppt.jedismock.comparisontests.streams;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.XAddParams;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(ComparisonBase.class)
public class XDelTests {
    @BeforeEach
    void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    void whenTopEntryIsDeleted_ensureTopIdDoesNotDecrease(Jedis jedis) {
        jedis.xadd("s", XAddParams.xAddParams().id("0-1"), Map.of("a", "b"));

        assertEquals(1, jedis.xdel("s", new StreamEntryID(0, 1)));

        assertThrows(
                JedisDataException.class,
                () -> jedis.xadd("s", XAddParams.xAddParams().id("0-1"), Map.of("a", "b")),
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
        );
    }

    @TestTemplate
    void whenTopEntryIsDeleted_ensureNextEntryIdIsIncremental(Jedis jedis) {
        jedis.xadd("s", XAddParams.xAddParams().id("2-1"), Map.of("a", "b"));

        assertEquals(1, jedis.xdel("s", new StreamEntryID(2, 1)));

        assertEquals(
                new StreamEntryID(2, 2),
                jedis.xadd("s", XAddParams.xAddParams().id("2-*"), Map.of("a", "b"))
        );
    }

    @TestTemplate
    void whenAllElementsAreRemoved_ensureStreamIsNotAlsoRemoved(Jedis jedis) {
        jedis.xadd("s", XAddParams.xAddParams().id("0-1"), Map.of("a", "b"));
        jedis.xadd("s", XAddParams.xAddParams().id("0-2"), Map.of("a", "b"));
        jedis.xadd("s", XAddParams.xAddParams().id("1-0"), Map.of("a", "b"));
        jedis.xadd("s", XAddParams.xAddParams().id("1-1"), Map.of("a", "b"));

        assertEquals(
                4,
                jedis.xdel(
                        "s",
                        new StreamEntryID(0, 1),
                        new StreamEntryID(0, 2),
                        new StreamEntryID(1, 0),
                        new StreamEntryID(1, 1)
                )
        );

        assertTrue(jedis.exists("s"));
    }

    @TestTemplate
    void stressTest(Jedis jedis) {
        StreamEntryID[] ids = new StreamEntryID[1000];

        for (int i = 0; i < 1000; ++i) {
            ids[i] = new StreamEntryID(i + 1);
            jedis.xadd("s", ids[i], Map.of("a",  "b"));
        }

        assertEquals(1000, jedis.xdel("s", ids));
        assertEquals(0, jedis.xlen("s"));
    }
}
