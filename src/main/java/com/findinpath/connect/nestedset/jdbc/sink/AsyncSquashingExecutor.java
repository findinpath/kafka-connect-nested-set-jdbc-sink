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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class wraps logic for it transmitting the information
 * that a {@link #execute(Runnable)} method call has been performed towards the
 * consumer in an asynchronous fashion.
 * <p>
 * The class makes use of an {@link Executor} for dispatching the calls asynchronously towards
 * consumer.
 * When using an executor having <code>3</code> threads there can be achieved the squashing of the
 * {@link #enqueue(Runnable)} ()} calls. In case that a synchronization call is already enqueued for processing and
 * another {@link  #enqueue(Runnable)} is being performed, the new call will not be enqueued anymore
 * leading to a faster method call exit. As mentioned before at least <code>3</code> threads would be enough because:
 * <ul>
 *     <li>one of the threads would be busy doing the consumption of the {@link  #enqueue(Runnable)} calls</li>
 *     <li>another thread would be busy trying to acquire the lock for doing consumption of the {@link  #enqueue(Runnable)} call</li>
 *     <li>this thread would verify if there is a {@link  #enqueue(Runnable)} method call already enqueued, and if so it would complete the method call.</li>
 * </ul>
 */
public class AsyncSquashingExecutor implements Executor {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncSquashingExecutor.class);

    /**
     * The executor used by this listener for notifying
     * asynchronously the downstream services about updates in the nested set log
     */
    private final ExecutorService executorService;

    private AtomicBoolean notificationEnqueued = new AtomicBoolean(false);
    private ReentrantReadWriteLock notificationLock = new ReentrantReadWriteLock();


    /**
     * Constructor of the class.
     */
    public AsyncSquashingExecutor() {
        this.executorService = Executors.newFixedThreadPool(3);
    }

    @Override
    public void execute(Runnable command) {
        LOGGER.info("Enqueuing a command call");
        executorService.execute(() -> enqueue(command));
    }

    public void stop() {
        try {
            if (!executorService.awaitTermination(800, TimeUnit.MILLISECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }

    private void enqueue(Runnable command) {
        boolean isNotificationEnqueued = notificationEnqueued.compareAndSet(false, true);

        if (isNotificationEnqueued) {
            try {
                LOGGER.debug("Trying to acquire the lock");
                while (!notificationLock.writeLock().tryLock()) {
                }
                notificationEnqueued.set(false);

                LOGGER.debug("Executing the command call");
                command.run();

            } finally {
                notificationLock.writeLock().unlock();
            }
        } else {
            LOGGER.debug("Command call squashed");
        }

    }
}
