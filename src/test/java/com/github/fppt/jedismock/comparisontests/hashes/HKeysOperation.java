package com.github.fppt.jedismock.comparisontests.hashes;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class HKeysOperation {
    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    void hkeysUnknownKey(Jedis jedis) {
        Set<String> res = jedis.hkeys("foo");
        assertEquals(new HashSet<String>(), res);
    }
}
