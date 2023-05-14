package com.github.fppt.jedismock.comparisontests.scripting;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(ComparisonBase.class)
class ScriptLoadTest {

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void loadTest(Jedis jedis) {
        String sha = jedis.scriptLoad("return 'Hello'");
        Object response = jedis.evalsha(sha);
        assertEquals(String.class, response.getClass());
        assertEquals("Hello", response);
    }

    @TestTemplate
    public void loadParametrizedTest(Jedis jedis) {
        String sha = jedis.scriptLoad("return ARGV[1]");
        String supposedReturn = "Hello, scripting!";
        Object response = jedis.evalsha(sha, 0, supposedReturn);
        assertEquals(String.class, response.getClass());
        assertEquals(supposedReturn, response);
    }
}