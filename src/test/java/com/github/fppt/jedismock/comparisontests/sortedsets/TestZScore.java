package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class TestZScore {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void testZScoreNotExistKey(Jedis jedis) {
        assertNull(jedis.zscore(ZSET_KEY, "a"));
    }

    @TestTemplate
    public void testZScoreNotExistMember(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 2, "b");
        assertNull(jedis.zscore(ZSET_KEY, "a"));
    }

    @TestTemplate
    public void testZScoreOK(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 2, "a");
        assertEquals(2, jedis.zscore(ZSET_KEY, "a"));
    }
}
