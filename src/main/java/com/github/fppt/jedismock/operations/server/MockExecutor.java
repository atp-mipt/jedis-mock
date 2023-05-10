package com.github.fppt.jedismock.operations.server;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.BlockingRedisOperation;
import com.github.fppt.jedismock.operations.CommandFactory;
import com.github.fppt.jedismock.operations.RedisBlockedOperationExecution;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.RedisOperationExecutor;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MockExecutor {

    private static List<ReadyKey> readyKeys = new ArrayList<>();
    private final static ScheduledExecutorService timeoutsExecutorService = Executors.newSingleThreadScheduledExecutor();

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RedisOperationExecutor.class);

    /**
     * Proceed with execution, mocking the Redis behaviour.
     * @param state Executor state, holding the shared database and connection-specific state.
     * @param name Command name.
     * @param commandParams Command parameters.
     */
    public static Slice proceed(OperationExecutorState state, String name, List<Slice> commandParams) {
        CompletableFuture<Slice> promise = new CompletableFuture<>();
        synchronized (state.lock()) {
            processOperations(state, name, commandParams, promise);
            processReadyKeys();
        }
        return promise.join();
    }

    /**
     * Break the connection (imitate Redis shutdown).
     * @param state  state Executor state
     */
    public static Slice breakConnection(OperationExecutorState state) {
        state.owner().close();
        return Response.SKIP;
    }

    public static void processReadyKeys() {
        while (!readyKeys.isEmpty()) {
            List<ReadyKey> readyKeysOld = readyKeys;
            readyKeys = new ArrayList<>();

            for (ReadyKey readyKey: readyKeysOld) {
                Queue<RedisBlockedOperationExecution> unblockedOperations = readyKey.getBase().getBlockedOperationsOnKey(readyKey.getKey());

                while (!unblockedOperations.isEmpty()) {
                    RedisBlockedOperationExecution execution = unblockedOperations.peek();
                    if (!execution.getOperation().canBeExecutedOnKey(readyKey.getKey())) {
                        break;
                    }

                    execution = unblockedOperations.poll();

                    if (execution.getPromise().isDone()) {
                        continue;
                    }

                    try {
                        execution.getPromise().complete(execution.getOperation().execute());
                    } catch (Exception e) {
                        execution.getPromise().complete(Response.error(e.getMessage()));
                    }
                }
            }
        }
    }

    public static void processOperations(OperationExecutorState state, String name, List<Slice> commandParams, CompletableFuture<Slice> promise) {
        try {
            //Checking if we are affecting the server or client state.
            //This is done outside the context of a transaction which is why it's a separate check
            RedisOperation operation = CommandFactory.buildOperation(name, false, state, commandParams);

            if (operation != null) {
                promise.complete(operation.execute());
                return;
            }

            //Checking if we are mutating the transaction or the redisBases
            operation = CommandFactory.buildOperation(name, true, state, commandParams);
            if (operation == null) {
                promise.complete(Response.error(String.format("Unsupported operation: %s", name)));
                return;
            }

            if (state.isTransactionModeOn()) {
                state.tx().add(operation);
                promise.complete(Response.clientResponse(name, Response.OK));
                return;
            }

            if (operation instanceof BlockingRedisOperation && !((BlockingRedisOperation) operation).canBeExecuted()) {
                BlockingRedisOperation blockingOperation = (BlockingRedisOperation) operation;
                timeoutsExecutorService.schedule(
                        () -> {
                            synchronized (state.lock()) {
                                promise.complete(blockingOperation.timeoutResponse());
                            }
                        },
                        blockingOperation.getTimeoutNanos(),
                        TimeUnit.NANOSECONDS
                );
                state.base().addBlockedOperation(blockingOperation.getBlockedOnKeys(), blockingOperation, promise);
                return;
            }
            promise.complete(Response.clientResponse(name, operation.execute()));
        } catch (Exception e) {
            LOG.error("Malformed request", e);
            promise.complete(Response.error(e.getMessage()));
        }
    }

    public static void addReadyKey(ReadyKey readyKey) {
        readyKeys.add(readyKey);
    }

    public static class ReadyKey {
        private final Slice key;
        private final RedisBase base;

        public ReadyKey(Slice key, RedisBase base) {
            this.key = key;
            this.base = base;
        }

        public Slice getKey() {
            return key;
        }

        public RedisBase getBase() {
            return base;
        }
    }
}
