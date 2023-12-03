package com.github.fppt.jedismock.comparisontests.scripting;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import com.github.fppt.jedismock.operations.scripting.Script;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(ComparisonBase.class)
class EvalShaTest {

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void evalShaWorksLowercase(Jedis jedis) {
        String script =
                "redis.call('SADD', 'foo', 'bar')\n" +
                        "return 'Hello, scripting! (lowercase SHA)'";
        Object evalResult = jedis.eval(script, 0);
        String sha = Script.getScriptSHA(script).toLowerCase();
        assertThat(jedis.scriptExists(sha)).isTrue();
        assertEquals(evalResult, jedis.evalsha(sha, 0));
    }

    @TestTemplate
    public void evalShaWorksUppercase(Jedis jedis) {
        String script =
                "redis.call('SADD', 'foo', 'bar')\n" +
                        "return 'Hello, scripting! (uppercase SHA)'";
        Object evalResult = jedis.eval(script, 0);
        String sha = Script.getScriptSHA(script).toUpperCase();
        assertThat(jedis.scriptExists(sha)).isTrue();
        assertEquals(evalResult, jedis.evalsha(sha, 0));
    }


    @TestTemplate
    public void evalShaWithScriptLoadingWorks(Jedis jedis) {
        String script = "return 'Hello, ' .. ARGV[1] .. '!'";
        String sha = jedis.scriptLoad(script);
        assertEquals("Hello, world!", jedis.evalsha(sha, 0, "world"));
    }

    @TestTemplate
    public void evalShaNotFoundExceptionIsCorrect(Jedis jedis) {
        assertThatThrownBy(() -> jedis.evalsha("abc", 0))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("NOSCRIPT No matching script. Please use EVAL.");
    }
}
