package com.github.fppt.jedismock.operations;

import java.util.function.Supplier;

public class BlockingOperationsUtils {
    private static final double EPS = 1e-9;

    /**
     * Functions waits until one of two things happen: eiter valueSupplier returns non-null value or the timeout expired
     * and returns result. If valueSupplier supplied non-null value, then this value is return. If timeout expired null value
     * is returned.
     * @param timeout - time to wait in seconds
     * @param valueSupplier - supplier calculating value
     * @param lock - lock to release and wait on
     * @return null, if timeout is expired, value of type T if valueSupplier suplied non-null value
     * @param <T> - Type of returning value
     * @throws InterruptedException - if wait was interrupted
     */
    public static <T> T waitUntil(double timeout, Supplier<T> valueSupplier, Object lock) throws InterruptedException {
        T result = null;

        long waitEnd = System.nanoTime() + (long)(timeout * 1_000_000_000);

        while (result == null) {
            long waitTime = timeout < EPS ? Long.MAX_VALUE : (waitEnd - System.nanoTime()) / 1_000_000;

            if (waitTime <= 0) {
                return null;
            }

            lock.wait(waitTime);
            result = valueSupplier.get();
        }

        return result;
    }
}
