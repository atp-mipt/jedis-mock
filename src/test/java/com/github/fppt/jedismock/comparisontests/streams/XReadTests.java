package com.github.fppt.jedismock.comparisontests.streams;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class XReadTests {
    private ExecutorService blockingThread;
    private Jedis blockedClient;

    @BeforeEach
    public void setUp(Jedis jedis, HostAndPort hostAndPort) {
        jedis.flushAll();
        blockedClient = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
        blockingThread = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    public void tearDown() {
        blockedClient.close();
        blockingThread.shutdownNow();
    }

    @TestTemplate
    void blockingXREADforStreamThatRanDry(Jedis jedis) throws ExecutionException, InterruptedException {
        jedis.xadd("s", XAddParams.xAddParams().id("666"), ImmutableMap.of("a", "b"));
        jedis.xdel("s", new StreamEntryID(666));

        jedis.xread(XReadParams.xReadParams().block(10),ImmutableMap.of("s", new StreamEntryID(665)));

        assertThatThrownBy(
                () -> jedis.xadd("s", XAddParams.xAddParams().id("665"), ImmutableMap.of("a", "b"))
        )
                .isInstanceOf(JedisDataException.class)
                .hasMessageMatching("ERR.*equal.*smaller.*");

        Future<?> future = blockingThread.submit(() -> {
            List<Map.Entry<String, List<StreamEntry>>> data = blockedClient.xread(
                    XReadParams.xReadParams().block(0),
                    ImmutableMap.of("s", new StreamEntryID(665))
            );

            assertThat(data)
                    .hasSize(1)
                    .first()
                    .extracting(Map.Entry::getValue)
                    .asList()
                    .hasSize(1)
                    .first()
                    .usingRecursiveComparison()
                    .isEqualTo(
                            new StreamEntry(
                                    new StreamEntryID(667),
                                    ImmutableMap.of("a", "b")
                            )
                    );
        });

        jedis.xadd("s", XAddParams.xAddParams().id("667"), ImmutableMap.of("a", "b"));

        future.get();
    }

    @TestTemplate
    @Disabled
    void xaddWithDelAndLpushShouldNotAwakeClient(Jedis jedis) throws ExecutionException, InterruptedException {
        Future<?> future = blockingThread.submit(() -> {
            List<Map.Entry<String, List<StreamEntry>>> data = blockedClient.xread(
                    XReadParams.xReadParams().block(20000),
                    ImmutableMap.of("s", StreamEntryID.LAST_ENTRY)
            );

            assertThat(data)
                    .hasSize(1)
                    .first()
                    .extracting(Map.Entry::getValue)
                    .asList()
                    .hasSize(1)
                    .first()
                    .usingRecursiveComparison()
                    .isEqualTo(
                            new StreamEntry(
                                    new StreamEntryID(12),
                                    ImmutableMap.of("new", "123")
                            )
                    );
        });

        jedis.xadd("s", XAddParams.xAddParams().id("11"), ImmutableMap.of("old", "123"));
        jedis.del("s");
        jedis.lpush("s", "foo", "bar");
        jedis.del("s");
        jedis.xadd("s", XAddParams.xAddParams().id("12"), ImmutableMap.of("new", "123"));

        future.get();
    }
}
