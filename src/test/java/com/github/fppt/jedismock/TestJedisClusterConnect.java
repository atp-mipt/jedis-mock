package com.github.fppt.jedismock;

import com.github.fppt.jedismock.server.ServiceOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.exceptions.JedisDataException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestJedisClusterConnect {

    RedisServer server;

    @BeforeEach
    void setup() throws IOException {
        server = RedisServer
                .newRedisServer()
                .setOptions(ServiceOptions.defaultOptions().withClusterModeEnabled())
                .start();
    }

    @AfterEach
    void tearDown() throws IOException {
        server.stop();
    }

    @Test
    void jedisClusterClientCanConnectAndWork() {
        Set<HostAndPort> jedisClusterNodes = new HashSet<>();
        jedisClusterNodes.add(
                new HostAndPort(server.getHost(), server.getBindPort()));

        String[] planets = new String[]{"Mars", "Jupyter", "Venus", "Earth", "Mercury", "Saturn"};
        try (JedisCluster jedis = new JedisCluster(jedisClusterNodes)) {
            jedis.sadd("planets", planets);
            assertThat(jedis.smembers("planets")).containsExactlyInAnyOrder(planets);
            assertThat(jedis.getClusterNodes()).hasSize(1);
        }
    }

    @Test
    void selectOperationDoesNotWorkInClusterMode() {
        try (Jedis jedis = new Jedis(new HostAndPort(server.getHost(), server.getBindPort()))) {
            assertThatThrownBy(() -> jedis.select(1))
                    .isInstanceOf(JedisDataException.class)
                    .hasMessage("ERR SELECT is not allowed in cluster mode");
        }
    }
}
