package com.github.fppt.jedismock;

import com.github.fppt.jedismock.datastructures.streams.StreamId;
import com.github.fppt.jedismock.server.ServiceOptions;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.params.XTrimParams;


import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Main {
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        RedisServer server = RedisServer
                .newRedisServer()
                //This is a special type of interceptor
//                .setOptions(ServiceOptions.executeOnly(3))
                .start();
        try (Jedis jedis = new Jedis(server.getHost(),
                server.getBindPort(), 30000); Jedis other = new Jedis(server.getHost(),
        server.getBindPort(), 30000)) {
//            jedis.xadd("e", XAddParams.xAddParams().id("1"), ImmutableMap.of("a", "b"));
            System.out.println(jedis.xadd(
                    "s",
                    XAddParams.xAddParams().noMkStream().approximateTrimming().maxLen(3).limit(3).id(1),
                    new HashMap<>() {{ put("a", "b"); }}
            ));

//            System.out.println(jedis.xadd("e", XAddParams.xAddParams().id("*"), ImmutableMap.of("a", "b")));
//            System.out.println(jedis.xadd("e", XAddParams.xAddParams().id("1"), ImmutableMap.of("a", "b")));
//            System.out.println(jedis.xadd("e", XAddParams.xAddParams().id("2"), ImmutableMap.of("a", "b")));
//            System.out.println(jedis.xadd("e", XAddParams.xAddParams().id("3"), ImmutableMap.of("a", "b")));
//            System.out.println(jedis.xadd("e", XAddParams.xAddParams().id("4"), ImmutableMap.of("a", "b")));
//            System.out.println(jedis.xadd("e", XAddParams.xAddParams().id("5"), ImmutableMap.of("a", "b")));
//            System.out.println(jedis.xrange("e", "-", "+", -1));
//            System.out.println(jedis.xadd("w", XAddParams.xAddParams().id("1"), ImmutableMap.of("a", "b")));
//            System.out.println(jedis.xadd("w", XAddParams.xAddParams().id("2"), ImmutableMap.of("a", "b")));
//            System.out.println(jedis.xadd("w", XAddParams.xAddParams().id("3"), ImmutableMap.of("a", "b")));
//            System.out.println(jedis.xadd("w", XAddParams.xAddParams().id("4"), ImmutableMap.of("a", "b")));

//            System.out.println(jedis.xrange("e", "(1-0", "+"));
//
//            ExecutorService executorService = Executors.newSingleThreadExecutor();
//
//            Future<?> future = executorService.submit(() -> {
//                System.out.println(
//                        other.xread(
//                                XReadParams.xReadParams().block(5000),
//                                ImmutableMap.of(
//                                        "w", StreamEntryID.LAST_ENTRY
//                                )
//                        )
//                );
//            });

//            System.out.println(jedis.xadd("w", XAddParams.xAddParams().id("5"), ImmutableMap.of("a", "b", "c", "d")));
//            System.out.println(jedis.del("w"));
//            Thread.sleep(1000);
//            System.out.println(jedis.xadd("w", XAddParams.xAddParams().id("6"), ImmutableMap.of("a", "b", "c", "d")));
//            System.out.println(jedis.xdel("w", new StreamEntryID(6)));
//            future.get();
//
//            future = executorService.submit(() -> {
//                System.out.println(
//                        other.xread(
//                                XReadParams.xReadParams().block(0),
//                                ImmutableMap.of(
//                                        "w", new StreamEntryID(5)
//                                )
//                        )
//                );
//            });
//
//            System.out.println(jedis.xadd("w", XAddParams.xAddParams().id("7"), ImmutableMap.of("a", "b", "c", "d")));
//            future.get();




        }
    }
}
