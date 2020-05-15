package com.findinpath.connect.nestedset.jdbc.testcontainers;

import org.slf4j.Logger;
import org.testcontainers.containers.output.OutputFrame;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.function.Consumer;

final class Utils {
    public static final String CONFLUENT_PLATFORM_VERSION = "5.5.0";

    private Utils() {
    }

    /**
     * Retrieves a random port that is currently not in use on this machine.
     *
     * @return a free port
     */
    static int getRandomFreePort() {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Add logs consumer for the testcontainers.
     * Increase the log level when debugging exceptions that may occur at
     * the initialization of the containers.
     *
     * @param log
     * @return
     */
    public static Consumer<OutputFrame> containerLogsConsumer(Logger log) {
        return (OutputFrame outputFrame) -> log.trace(outputFrame.getUtf8String());
    }
}
