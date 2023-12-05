package com.github.fppt.jedismock;

import com.github.fppt.jedismock.server.ServiceOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestLettuceConnection {

    @Test
    void lettuceClientCanConnectAndWork() throws Exception {
        RedisServer server = RedisServer.newRedisServer();
        server.start();
        try {
            RedisClient redisClient = RedisClient
                    .create(String.format("redis://%s:%s",
                            server.getHost(), server.getBindPort()));
            StatefulRedisConnection<String, String> connection = redisClient.connect();
            RedisCommands<String, String> syncCommands = connection.sync();
            syncCommands.set("key", "Hello, Redis!");
            String val = syncCommands.get("key");
            connection.close();
            redisClient.shutdown();

            assertThat(val).isEqualTo("Hello, Redis!");
        } finally {
            server.stop();
        }
    }

    @Test
    void lettuceClusterClientCanConnectAndWork() throws Exception {
        RedisServer server = RedisServer.newRedisServer()
                .setOptions(ServiceOptions.defaultOptions().withClusterModeEnabled());
        server.start();
        try {
            RedisClusterClient redisClient = RedisClusterClient
                    .create(String.format("redis://%s:%s",
                            server.getHost(), server.getBindPort()));
            StatefulRedisClusterConnection<String, String> connection = redisClient.connect();


            RedisClusterCommands<String, String> syncCommands = connection.sync();
            syncCommands.set("key", "Hello, Redis cluster!");
            String val = syncCommands.get("key");
            assertThat(syncCommands.clusterMyId()).isNotEmpty();
            assertThat(syncCommands.clusterSlots()).hasSize(1);
            connection.close();
            redisClient.shutdown();
            assertThat(val).isEqualTo("Hello, Redis cluster!");
        } finally {
            server.stop();
        }
    }
}
