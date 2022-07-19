package io.confluent.main;

import io.confluent.resilience.RetrieveContextTransformer;
import io.confluent.resilience.ReplaceThreadExceptionHandler;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class Uppercase {

    private static final Logger log = LoggerFactory.getLogger(Uppercase.class);

    private static final String INPUT_TOPIC = "uppercase-input";
    private static final String OUTPUT_TOPIC = "uppercase-output";

    private static Topology buildTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.<String, String>stream(INPUT_TOPIC)
                .transform(() -> new RetrieveContextTransformer<>()) // TODO !!!
                .mapValues(s -> {
                    if ("quack".equals(s)) throw new RuntimeException("quacking failure"); // might fail here
                    return s.toUpperCase(Locale.ROOT);
                })
                .to(OUTPUT_TOPIC);
        return builder.build();
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.out.println("Usage <streamsPropertiesFile>");
            System.exit(1);
        }

        final Properties streamsConfig = loadConfig(args[0]);
        final String applicationId = UUID.randomUUID().toString();
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        log.info("Starting the application..");

        final Topology topology = buildTopology();
        final KafkaStreams streams = new KafkaStreams(topology, streamsConfig);
        streams.setUncaughtExceptionHandler(new ReplaceThreadExceptionHandler()); // TODO !!!

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
