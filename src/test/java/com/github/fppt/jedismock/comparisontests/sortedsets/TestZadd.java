package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import com.github.fppt.jedismock.exception.ArgumentException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.CommandArguments;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ZAddParams;
import redis.clients.jedis.resps.Tuple;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class TestZadd {
    static class ZaddParamsExt extends ZAddParams {
        public ZAddParams incr() {
            addParam("incr");
            return this;
        }

        public void addParams(CommandArguments args) {
            if (this.contains("incr")) {
                args.add("incr");
            }
        }
    }

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void zaddAddsKey(Jedis jedis) {
        String key = "mykey";
        double score = 10;
        String value = "myvalue";

        long result = jedis.zadd(key, score, value);

        assertEquals(1L, result);

        List<String> results = jedis.zrange(key, 0, -1);

        assertEquals(1, results.size());
        assertEquals(value, results.get(0));
    }

    @TestTemplate
    public void zaddAddsKeys(Jedis jedis) {
        String key = "mykey";
        Map<String, Double> members = new HashMap<>();
        members.put("myvalue1", 10d);
        members.put("myvalue2", 20d);

        long result = jedis.zadd(key, members);

        assertEquals(2L, result);

        List<String> results = jedis.zrange(key, 0, -1);

        assertEquals(2, results.size());
        assertEquals("myvalue1", results.get(0));
        assertEquals("myvalue2", results.get(1));
    }

    @TestTemplate
    public void testZaddNonUTF8binary(Jedis jedis) {
        byte[] msg = new byte[]{(byte) 0xbe};
        jedis.zadd("foo".getBytes(), 42, msg);
        byte[] newMsg = jedis.zrange("foo".getBytes(), 0, 0).get(0);
        assertArrayEquals(msg, newMsg);
    }

    @TestTemplate
    public void testBasicZAddAndScoreUpdate(Jedis jedis) {
        String key = "mykey";

        jedis.zadd(key, 10d, "x");
        jedis.zadd(key, 20d, "y");
        jedis.zadd(key, 30d, "z");
        assertEquals(Arrays.asList("x", "y", "z"), jedis.zrange(key, 0, -1));

        jedis.zadd(key, 1d, "y");
        assertEquals(Arrays.asList("y", "x", "z"), jedis.zrange(key, 0, -1));
    }

    @TestTemplate
    public void testZAddKeys(Jedis jedis) {

        String key = "mykey";
        Map<String, Double> members = new HashMap<>();
        members.put("a", 10d);
        members.put("b", 20d);
        members.put("c", 30d);

        long result = jedis.zadd(key, members);
        List<Tuple> results = jedis.zrangeWithScores(key, 0, -1);

        assertEquals(result, results.size());
        assertEquals(new Tuple("a", 10.0), results.get(0));
        assertEquals(new Tuple("b", 20.0), results.get(1));
        assertEquals(new Tuple("c", 30.0), results.get(2));
    }

    @TestTemplate
    public void testZAddXXWithoutKey(Jedis jedis) {
        String key = "mykey";

        long result = jedis.zadd(key, 10, "x", new ZAddParams().xx());

        assertEquals(0, result);
        assertEquals("none", jedis.type(key));
    }

    @TestTemplate
    public void testZAddXXToExistKey(Jedis jedis) {
        String key = "mykey";

        jedis.zadd(key, 10, "x");
        long result = jedis.zadd(key, 20, "y", new ZAddParams().xx());

        assertEquals(0, result);
        assertEquals(1, jedis.zcard(key));
    }

    @TestTemplate
    public void testZAddGetNumberAddedElements(Jedis jedis) {
        String key = "mykey";

        jedis.zadd(key, 10, "x");
        Map<String, Double> members = new HashMap<>();
        members.put("x", 10d);
        members.put("y", 20d);
        members.put("z", 30d);
        long result = jedis.zadd(key, members);

        assertEquals(2, result);
    }

    @TestTemplate
    public void testZAddXXUpdateExistingScores(Jedis jedis) {

        String key = "mykey";
        Map<String, Double> members1 = new HashMap<>();
        members1.put("x", 10d);
        members1.put("y", 20d);
        members1.put("z", 30d);
        jedis.zadd(key, members1);

        Map<String, Double> members2 = new HashMap<>();
        members2.put("foo", 5d);
        members2.put("x", 11d);
        members2.put("y", 21d);
        members2.put("zap", 40d);
        jedis.zadd(key, members2, new ZAddParams().xx());

        assertEquals(3, jedis.zcard(key));
        assertEquals(11, jedis.zscore(key, "x"));
        assertEquals(21, jedis.zscore(key, "y"));
    }

    @TestTemplate
    public void testZAddXXandNX(Jedis jedis) {

        String key = "mykey";
        assertThrows(RuntimeException.class,
                () -> jedis.zadd(key, 10, "x", new ZAddParams().xx().nx()));
    }

    @TestTemplate
    public void testZAddNXWithNonExistingKey(Jedis jedis) {
        String key = "mykey";

        Map<String, Double> members = new HashMap<>();
        members.put("x", 10d);
        members.put("y", 20d);
        members.put("z", 30d);

        jedis.zadd(key, members, new ZAddParams().nx());

        assertEquals(3, jedis.zcard(key));
    }

    @TestTemplate
    public void testZAddNXOnlyAddNewElements(Jedis jedis) {

        String key = "mykey";
        Map<String, Double> members1 = new HashMap<>();
        members1.put("x", 10d);
        members1.put("y", 20d);
        members1.put("z", 30d);
        jedis.zadd(key, members1);
        Map<String, Double> members2 = new HashMap<>();
        members2.put("x", 11d);
        members2.put("y", 21d);
        members2.put("a", 100d);
        members2.put("b", 200d);

        long result = jedis.zadd(key, members2, new ZAddParams().nx());

        assertEquals(2, result);
        assertEquals(10, jedis.zscore(key, "x"));
        assertEquals(20, jedis.zscore(key, "y"));
        assertEquals(100, jedis.zscore(key, "a"));
        assertEquals(200, jedis.zscore(key, "b"));
    }

//    @TestTemplate
//    public void testZAddIncrLikeIncrBy(Jedis jedis) {
//
//        String key = "mykey";
//        Map<String, Double> members = new HashMap<>();
//        members.put("x", 10d);
//        members.put("y", 20d);
//        members.put("z", 30d);
//        jedis.zadd(key, members);
//        jedis.zadd(key, 15, "x", new ZaddParamsExt().incr());
//
//        assertEquals(25, jedis.zscore(key, "x"));
//    }

    @TestTemplate
    public void testZAddIncrMoreArgs(Jedis jedis) {

        String key = "mykey";
        Map<String, Double> members1 = new HashMap<>();
        members1.put("x", 10d);
        members1.put("y", 20d);
        members1.put("z", 30d);
        jedis.zadd(key, members1);
        Map<String, Double> members2 = new HashMap<>();
        members2.put("x", 15d);
        members2.put("y", 10d);
        assertThrows(RuntimeException.class,
                () -> jedis.zadd(key, members2, new ZaddParamsExt().incr()));
    }

    @TestTemplate
    public void testZAddCHGetNumberChangedElements(Jedis jedis) {

        String key = "mykey";
        Map<String, Double> members1 = new HashMap<>();
        members1.put("x", 10d);
        members1.put("y", 20d);
        members1.put("z", 30d);
        jedis.zadd(key, members1);

        Map<String, Double> members2 = new HashMap<>();
        members2.put("x", 11d);
        members2.put("y", 21d);
        members2.put("z", 30d);
        assertEquals(0, jedis.zadd(key, members2));

        Map<String, Double> members3 = new HashMap<>();
        members3.put("x", 12d);
        members3.put("y", 22d);
        members3.put("z", 30d);
        assertEquals(2, jedis.zadd(key, members3, new ZAddParams().ch()));
    }

}
