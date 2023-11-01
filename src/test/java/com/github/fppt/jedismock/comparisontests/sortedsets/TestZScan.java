package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.resps.Tuple;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class TestZScan {

    private static final String ZSET_KEY_1 = "myzset";
//    private static final String ZSET_KEY_2 = "ztmp";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();
        jedis.zadd(ZSET_KEY_1, 1, "b");
        jedis.zadd(ZSET_KEY_1, 12, "bq");
        jedis.zadd(ZSET_KEY_1, 13, "bw");
        jedis.zadd(ZSET_KEY_1, 14, "be");
        jedis.zadd(ZSET_KEY_1, 15, "br");
        jedis.zadd(ZSET_KEY_1, 16, "bt");
        jedis.zadd(ZSET_KEY_1, 17, "by");
//        jedis.zadd(ZSET_KEY_1, 18, "bu");
//        jedis.zadd(ZSET_KEY_1, 19, "bi");
//        jedis.zadd(ZSET_KEY_1, 10, "bo");
//        jedis.zadd(ZSET_KEY_1, 21, "bp");
//        jedis.zadd(ZSET_KEY_1, 31, "qc");
//        jedis.zadd(ZSET_KEY_1, 41, "wc");
//        jedis.zadd(ZSET_KEY_1, 51, "ec");
//        jedis.zadd(ZSET_KEY_1, 61, "rc");
//        jedis.zadd(ZSET_KEY_1, 71, "tc");
//        jedis.zadd(ZSET_KEY_1, 81, "yc");
//        jedis.zadd(ZSET_KEY_1, 91, "uc");
    }

    @TestTemplate
    public void testZScan(Jedis jedis) {
        ScanResult<Tuple> result = jedis.zscan(ZSET_KEY_1, "0", new ScanParams().match("*e*"));
        ScanResult<Tuple> expected = new ScanResult<>("0",
                Collections.singletonList(new Tuple("be", 14.0)));
//        ScanResult<Tuple> expected = new ScanResult<Tuple>("0",
//                Arrays.asList(new Tuple("be", 14.0),
//                        new Tuple("ec", 51.0)));
        //        assertEquals(expected.getCursor(), result.getCursor());
        assertEquals(expected.getResult().size(), result.getResult().size());
        assertEquals(expected.getResult(), result.getResult());
    }
}
