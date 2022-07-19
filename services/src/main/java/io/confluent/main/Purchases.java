/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.main;

import io.confluent.services.MetadataService;
import io.confluent.services.PurchasesService;
import io.confluent.streams.PurchasesTopology;
import io.javalin.Javalin;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Purchases {

    private static final Logger LOGGER = LoggerFactory.getLogger(Purchases.class);
    private static final String STRING_SERDE_NAME = Serdes.String().getClass().getName();

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("Usage <streamsPropertiesFile> <appPropertiesFile>");
            System.exit(1);
        }

        final Properties streamsConfig = loadConfig(args[0]);
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, STRING_SERDE_NAME);
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, STRING_SERDE_NAME);

        final Properties appConfig = loadConfig(args[1]);
        final var applicationPort = Integer.parseInt(appConfig.getProperty("application.port"));

        LOGGER.info("Starting the application..");
        final Topology topology = new PurchasesTopology().buildTopology();
        final KafkaStreams streams = new KafkaStreams(topology, streamsConfig);

        final Javalin server = Javalin.create();
        final var metadataService = new MetadataService(streams, server);
        final var purchasesService = new PurchasesService(streams, server, "localhost", applicationPort);

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("demo-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            metadataService.bind();
            purchasesService.bind();

            server.start(applicationPort);

            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    public static Properties loadConfig(final String configFile) throws IOException {

        final ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        final Properties cfg = new Properties();

        try (InputStream inputStream = classloader.getResourceAsStream(configFile)) {
            cfg.load(inputStream);
        }

        return cfg;
    }
}
