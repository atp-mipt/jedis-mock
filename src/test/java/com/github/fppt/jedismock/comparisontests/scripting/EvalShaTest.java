package com.github.fppt.jedismock.comparisontests.scripting;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import com.github.fppt.jedismock.operations.scripting.Script;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
class EvalShaTest {

    @BeforeEach
    public void setUp(Jedis jedis) throws NoSuchAlgorithmException {
        jedis.flushAll();
    }

    @TestTemplate
    public void evalShaWorks(Jedis jedis) throws NoSuchAlgorithmException {
        String script = "return 'Hello, scripting!'";
        Object evalResult = jedis.eval(script, 0);
        MessageDigest sha1 = MessageDigest.getInstance("SHA1");
        StringBuilder hex = new StringBuilder();
        byte[] digest = sha1.digest(script.getBytes());
        for (byte value : digest) {
            hex.append(Integer.toString((value & 0xff) + 0x100, 16).substring(1));
        }
        String result = hex.toString();
        assertEquals(evalResult ,jedis.evalsha(result, 0));
    }


    @TestTemplate
    public void evalShaWithScriptLoadingWorks(Jedis jedis) {
        String script = "return 'Hello, scripting!'";
        Object evalResult = jedis.eval(script, 0);
        String sha = Script.getScriptSHA(script);
        assertEquals(evalResult, jedis.evalsha(sha, 0));
    }

    @TestTemplate
    public void evalShaNotFoundExceptionIsCorrect(Jedis jedis) {
        RuntimeException e = assertThrows(RuntimeException.class, () -> jedis.evalsha("", 0));
        assertEquals("NOSCRIPT No matching script. Please use EVAL.", e.getMessage());
    }
}
