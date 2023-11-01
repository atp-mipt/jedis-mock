package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ZRangeParams;
import redis.clients.jedis.resps.Tuple;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static redis.clients.jedis.Protocol.Keyword.BYLEX;
import static redis.clients.jedis.Protocol.Keyword.BYSCORE;

@ExtendWith(ComparisonBase.class)
public class TestZRangeStore {

    private static final String ZSET_KEY = "myzset";
    private static final String ZSET_KEY_OUT = "out";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();
        jedis.zadd(ZSET_KEY, 1, "a");
        jedis.zadd(ZSET_KEY, 2, "b");
        jedis.zadd(ZSET_KEY, 3, "c");
        jedis.zadd(ZSET_KEY, 4, "d");
    }

    @TestTemplate
    public void testZRangeStoreBaseOK(Jedis jedis) {
        assertEquals(4, jedis.zrangestore(ZSET_KEY_OUT, ZSET_KEY, new ZRangeParams(0, -1)));
        List<Tuple> result = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        List<Tuple> expected = Arrays.asList(
                new Tuple("a", 1.0),
                new Tuple("b", 2.0),
                new Tuple("c", 3.0),
                new Tuple("d", 4.0));
        assertEquals(expected, result);
    }

    @TestTemplate
    public void testZRangeStoreRange(Jedis jedis) {
        assertEquals(2, jedis.zrangestore(ZSET_KEY_OUT, ZSET_KEY, new ZRangeParams(1, 2)));
        List<Tuple> result = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        List<Tuple> expected = Arrays.asList(
                new Tuple("b", 2.0),
                new Tuple("c", 3.0));
        assertEquals(expected, result);
    }
    @TestTemplate
    public void testZRangeStoreByLex(Jedis jedis) {
        assertEquals(2, jedis.zrangestore(ZSET_KEY_OUT, ZSET_KEY, new ZRangeParams(BYLEX, "[b", "[c")));
        List<Tuple> result = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        List<Tuple> expected = Arrays.asList(
                new Tuple("b", 2.0),
                new Tuple("c", 3.0));
        assertEquals(expected, result);
    }
    @TestTemplate
    public void testZRangeStoreByScore(Jedis jedis) {
        assertEquals(2, jedis.zrangestore(ZSET_KEY_OUT, ZSET_KEY, new ZRangeParams(BYSCORE, "1", "2")));
        List<Tuple> result = jedis.zrangeWithScores(ZSET_KEY_OUT, 0, -1);
        List<Tuple> expected = Arrays.asList(
                new Tuple("a", 1.0),
                new Tuple("b", 2.0));
        assertEquals(expected, result);
    }
}
