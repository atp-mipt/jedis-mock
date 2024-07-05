package com.github.fppt.jedismock;

import com.github.fppt.jedismock.operations.CommandFactory;
import com.github.fppt.jedismock.server.RedisService;
import com.github.fppt.jedismock.server.ServiceOptions;
import com.github.fppt.jedismock.storage.RedisBase;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by Xiaolu on 2015/4/18.
 */
public class RedisServer {
    private Clock internalClock;
    private int bindPort;
    private InetAddress bindAddress;
    private final Map<Integer, RedisBase> redisBases;
    private final ExecutorService threadPool;
    private RedisService service;
    private ServiceOptions options = ServiceOptions.defaultOptions();
    private Future<Void> serviceFinalization;

    public RedisServer() {
        this(0);
    }

    public RedisServer(int port) {
        this(port, null);
    }

    public RedisServer(int port, InetAddress address) {
        this(port, address, Clock.systemDefaultZone());
    }

    public RedisServer(int port, InetAddress address, Clock internalClock) {
        this.bindPort = port;
        this.bindAddress = address;
        this.redisBases = new HashMap<>();
        this.threadPool = Executors.newSingleThreadExecutor();
        CommandFactory.initialize();
        this.internalClock = internalClock;
    }

    public static RedisServer newRedisServer() {
        return new RedisServer();
    }

    public static RedisServer newRedisServer(int port) {
        return new RedisServer(port);
    }

    public static RedisServer newRedisServer(int port, InetAddress address) {
        return new RedisServer(port, address);
    }

    public RedisServer setAddress(InetAddress address) {
        Objects.requireNonNull(address);
        this.bindAddress = address;
        return this;
    }

    public RedisServer setOptions(ServiceOptions options) {
        Objects.requireNonNull(options);
        this.options = options;
        return this;
    }

    public RedisServer setPort(int port) {
        this.bindPort = port;
        return this;
    }

    public RedisServer setInternalClock(Clock internalClock) {
        Objects.requireNonNull(internalClock);
        this.internalClock = internalClock;
        return this;
    }

    public String getHost() {
        Objects.requireNonNull(service);
        return service.getServer().getInetAddress().getHostAddress();
    }

    public int getBindPort() {
        Objects.requireNonNull(service);
        return service.getServer().getLocalPort();
    }

    public Clock getInternalClock() {
        return internalClock;
    }

    public RedisServer start() throws IOException {
        if (service != null) {
            throw new IllegalStateException();
        }

        this.service = new RedisService(bindPort, bindAddress, redisBases, options, internalClock);
        serviceFinalization = threadPool.submit(service);
        return this;
    }

    public void stop() throws IOException {
        Objects.requireNonNull(service);
        service.stop();
        service = null;
        try {
            serviceFinalization.get();
        } catch (ExecutionException ignored) {
            //Do nothing: it's a normal behaviour when the service was stopped
        } catch (InterruptedException e) {
            System.err.println("Jedis-mock interrupted while stopping");
            Thread.currentThread().interrupt();
        }
    }

}
