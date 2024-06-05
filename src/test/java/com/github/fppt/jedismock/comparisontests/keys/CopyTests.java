package com.github.fppt.jedismock.comparisontests.keys;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class CopyTests {
    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void whenGettingKeys_EnsureCorrectKeysAreReturned(Jedis jedis) {
        String first = "first";
        String second = "second";

        String firstValue = "abracadabra";
        String secondValue = "abc";

        jedis.select(11);

        jedis.set(first, firstValue);
        jedis.select(10);
        jedis.set(second, secondValue);

        jedis.select(11);

        assertThat(jedis.get(first)).isEqualTo(firstValue);
        assertThat(jedis.copy(first, second, 10, false)).isFalse();
        assertThat(jedis.get(first)).isEqualTo(firstValue);

        jedis.select(10);

        assertThat(jedis.get(second)).isEqualTo(secondValue);


    }
}
