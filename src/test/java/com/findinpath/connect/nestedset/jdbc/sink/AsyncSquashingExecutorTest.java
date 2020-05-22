/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
        } finally {
            executorService.shutdown();
        }
    }
}
