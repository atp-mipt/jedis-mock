package com.github.fppt.jedismock.comparisontests.hashes;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class TestHDel {
    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();
    }

    @TestTemplate
    void hdelWithManyArguments(Jedis jedis) {
        Map<String, String> hash = new HashMap<>();
        hash.put("key1", "1");
        hash.put("key2", "2");
        jedis.hset("foo", hash);
        final Long res = jedis.hdel("foo", "key1", "key2");
        assertEquals(2, res);
    }
}
