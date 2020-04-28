package com.findinpath.connect.nestedset.jdbc.sink;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class AsyncSquashingExecutorTest {

    private AsyncSquashingExecutor squashingExecutor;

    @BeforeEach
    public void setup() {
        squashingExecutor = new AsyncSquashingExecutor();
    }

    @AfterEach
    public void tearDown() {
        squashingExecutor.stop();
    }

    @Test
    public void demo() {
        AtomicBoolean eventDispatchedToConsumer = new AtomicBoolean(false);
        Runnable command = () -> eventDispatchedToConsumer.set(true);
        try {
            squashingExecutor.execute(command);
            await().atMost(1, TimeUnit.SECONDS).until(eventDispatchedToConsumer::get);
        } finally {
            squashingExecutor.stop();
        }
    }

    @Test
    public void multiThreading() throws Exception {
        AtomicInteger eventsDispatchedToConsumer = new AtomicInteger(0);
        Runnable command = () -> {
            eventsDispatchedToConsumer.incrementAndGet();
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        };

        int nThreads = 20;
        ExecutorService executorService = Executors.newFixedThreadPool(nThreads);

        try {
            IntStream.range(0, nThreads)
                    .parallel()
                    .forEach(i -> executorService.execute(() -> squashingExecutor.execute(command)));

            // wait until multiple handling cycles of the consumer would have completed.
            Thread.sleep(700);

            int expectedAmountOfEventsDispatchedToConsumer = 2;
            assertThat(eventsDispatchedToConsumer.get(), equalTo(expectedAmountOfEventsDispatchedToConsumer));
        }finally {
            executorService.shutdown();
        }
    }
}
