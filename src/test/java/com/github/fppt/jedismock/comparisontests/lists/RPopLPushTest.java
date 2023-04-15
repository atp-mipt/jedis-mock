package com.github.fppt.jedismock.comparisontests.lists;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(ComparisonBase.class)
public class RPopLPushTest {
    private static final String rpoplpush_from_key = "rpoplpush_from_key";
    private static final String rpoplpush_to_key = "rpoplpush_to_key";


    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
        jedis.rpush(rpoplpush_from_key, "1", "2", "3", "foo", "bar");
    }

    @TestTemplate
    public void whenUsingRPopLPush_EnsureDoesntPopOnWrongType(Jedis jedis) {
        jedis.set(rpoplpush_to_key, "not_list");
        JedisDataException exception = Assertions.assertThrows(JedisDataException.class, () -> jedis.rpoplpush(rpoplpush_from_key, rpoplpush_to_key));
        assertEquals("WRONGTYPE Operation against a key holding the wrong kind of value", exception.getMessage());

        assertEquals(5, jedis.llen(rpoplpush_from_key));
    }
}
