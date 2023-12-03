package com.github.fppt.jedismock;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RList;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.io.IOException;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestRedissonConnection {
    private static RedisServer redisServer;
    private static RedissonClient client;

    @BeforeAll
    static void setUp() throws IOException {
        redisServer = RedisServer.newRedisServer();
        redisServer.start();
        Config config = new Config();
        config.useSingleServer().setAddress(
                String.format("redis://%s:%d", redisServer.getHost(), redisServer.getBindPort()));
        client = Redisson.create(config);
    }

    @AfterAll
    static void tearDown() throws IOException {
        redisServer.stop();
    }

    @Test
    public void testStringMap() {
        RMap<String, String> map = client.getMap("stringMap");
        assertThat(map.put("foo", "bar")).isNull();
        assertEquals("bar", map.get("foo"));
        assertEquals("bar", map.put("foo", "baz"));
    }

    @Test
    public void testIntegerMap() {
        RMap<String, Integer> map = client.getMap("intMap");
        assertThat(map.put("foo", 42)).isNull();
        assertEquals(42, map.get("foo"));
        assertEquals(42, map.put("foo", 43));
    }

    @Test
    public void testIntList() {
        RList<Integer> list = client.getList("intList");
        assertThat(list.add(11)).isTrue();
        assertThat(list.add(15)).isTrue();
        assertEquals(2, list.size());
        assertEquals(Arrays.asList(11, 15), list.readAll());
    }

    @Test
    public void testLock() {
        String key = "an-example-key";
        RLock rLock = client.getLock(key);
        rLock.lock();
        assertThat(rLock.isLocked()).isTrue();
        rLock.unlock();
        assertThat(rLock.isLocked()).isFalse();
    }
}
