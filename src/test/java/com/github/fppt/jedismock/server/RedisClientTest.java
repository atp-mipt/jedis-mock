package com.github.fppt.jedismock.server;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.time.Clock;
import java.util.Collections;

class RedisClientTest {
    Socket socket;
    RedisClient redisClient;

    @BeforeEach
    void init() throws IOException {
        socket = Mockito.mock(Socket.class);
        Mockito.when(socket.getInputStream()).thenReturn(Mockito.mock(InputStream.class));
        Mockito.when(socket.getOutputStream()).thenReturn(Mockito.mock(OutputStream.class));
        redisClient = new RedisClient(
                Collections.emptyMap(),
                socket,
                ServiceOptions.defaultOptions(), c -> {},
                Clock.systemDefaultZone()
        );
    }

    @Test
    void testClosedSocket() {
        Mockito.when(socket.isClosed()).thenReturn(true);
        redisClient.run();
    }
}