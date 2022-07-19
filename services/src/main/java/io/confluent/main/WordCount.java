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

import io.confluent.interceptors.FailProofInterceptor;
import io.confluent.resilience.ReplaceThreadExceptionHandler;
import io.confluent.services.MetadataService;
import io.confluent.services.WordCountService;
import io.confluent.streams.WordCountTopology;
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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * In this example, we implement a simple WordCount program using the high-level Streams DSL
 * that reads from a source topic "demo-plaintext-input", where the values of messages represent lines of text,
 * split each text line into words and then compute the word occurrence histogram, write the continuous updated histogram
 * into a topic "demo-wordcount-output" where each record is an updated count of a single word.
 */
public class WordCount {

    private static final Logger log = LoggerFactory.getLogger(WordCount.class);

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("Usage <streamsPropertiesFile> <appPropertiesFile>");
            System.exit(1);
        }

        final Properties streamsConfig = loadConfig(args[0]);
        final String applicationId = UUID.randomUUID().toString();
        //streamsConfig.put(ConsumerConfig.GROUP_ID_CONFIG, applicationId);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // streamsConfig.put("consumer.interceptor.classes", FailProofInterceptor.class.getName());

        final Properties appConfig = loadConfig(args[1]);
        final var applicationPort = Integer.parseInt(appConfig.getProperty("application.port"));

        log.info("Starting the application..");
        final Topology topology = new WordCountTopology().buildTopology();
        final KafkaStreams streams = new KafkaStreams(topology, streamsConfig);
        streams.setUncaughtExceptionHandler(new ReplaceThreadExceptionHandler()); // TODO !!!

        final Javalin server = Javalin.create();
        final var metadataService = new MetadataService(streams, server);
        final var wordCountService = new WordCountService(streams, server, "localhost", applicationPort);

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
            wordCountService.bind();

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
