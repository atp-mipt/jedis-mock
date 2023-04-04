package com.github.fppt.jedismock.comparisontests.scripting;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(ComparisonBase.class)
public class EvalTest {

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void evalTest(Jedis jedis) {
        Object eval_return = jedis.eval("return 'Hello, scripting!'", 0);
        assertEquals(String.class, eval_return.getClass());
        assertEquals("Hello, scripting!", eval_return);
    }

    @TestTemplate
    public void evalParametrizedTest(Jedis jedis) {
        Object eval_return = jedis.eval("return ARGV[1]",0, "Hello");
        assertEquals(String.class, eval_return.getClass());
        assertEquals("Hello", eval_return);
    }

    @TestTemplate
    public void evalNumberTest(Jedis jedis) {
        Object eval_return = jedis.eval("return 0",0);
        assertEquals(Long.class, eval_return.getClass());
        assertEquals(0L, eval_return);
    }

    @TestTemplate
    public void evalTableOfStringsTest(Jedis jedis) {
        Object eval_return = jedis.eval("return { 'test' }", 0);
        assertEquals(ArrayList.class, eval_return.getClass());
        assertEquals(Collections.singletonList("test"), eval_return);
    }

    @TestTemplate
    public void evalTableOfLongTest(Jedis jedis) {
        Object eval_return = jedis.eval("return { 1, 2, 3 }", 0);
        assertEquals(ArrayList.class, eval_return.getClass());
        assertEquals(Long.class, ((List<?>)eval_return).get(0).getClass());
        assertEquals(Arrays.asList(1L,2L,3L), eval_return);
    }

    @TestTemplate
    public void evalParametrizedReturnMultipleKeysArgsTest(Jedis jedis) {
        Object eval_return = jedis.eval(
                "return { KEYS[1], KEYS[2], ARGV[1], ARGV[2], ARGV[3] }",
                2, "key1", "key2",
                "arg1", "arg2", "arg3"
        );
        assertEquals(ArrayList.class, eval_return.getClass());
        assertEquals(Arrays.asList("key1", "key2", "arg1", "arg2", "arg3"), eval_return);
    }

    @TestTemplate
    public void evalParametrizedReturnMultipleKeysArgsNumbersTest(Jedis jedis) {
        Object eval_return = jedis.eval(
                "return { KEYS[1], KEYS[2], ARGV[1], ARGV[2], ARGV[3] }",
                2, "key1", "key2",
                "arg1", "arg2", "arg3"
        );
        assertEquals(ArrayList.class, eval_return.getClass());
        assertEquals(Arrays.asList("key1", "key2", 1L, 2L, 3L), eval_return);
    }

    @TestTemplate
    public void evalRedisSetTest(Jedis jedis) {
        assertEquals("OK", jedis.eval("return redis.call('SET', 'test', 'hello')", 0));
    }

}
