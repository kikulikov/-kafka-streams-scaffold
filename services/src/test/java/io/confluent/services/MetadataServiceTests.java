package io.confluent.services;

import io.confluent.streams.WordCountTopology;
import io.javalin.testtools.TestUtil;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.lifecycle.Startables;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.confluent.streams.WordCountTopology.*;
import static org.assertj.core.api.Assertions.assertThat;

class MetadataServiceTests extends AbstractServiceTests {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataServiceTests.class);
    private MetadataService metadataService;

    @BeforeEach
    void setUp() {
        kafka.withEnv("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", "1");
        kafka.withStartupTimeout(Duration.ofSeconds(20));

        Startables.deepStart(Stream.of(kafka));

        Unreliables.retryUntilTrue(120, TimeUnit.SECONDS,
                () -> kafka.isRunning() && !kafka.getBootstrapServers().isBlank());

        createTopics(WORDCOUNT_INPUT, WORDCOUNT_OUTPUT);

        final var topology = new WordCountTopology().buildTopology();
        kafkaStreams = new KafkaStreams(topology, streamsProperties());
        kafkaStreams.start();

        Unreliables.retryUntilTrue(120, TimeUnit.SECONDS,
                () -> kafkaStreams.state().equals(KafkaStreams.State.RUNNING));

        metadataService = new MetadataService(kafkaStreams, application);
        metadataService.bind();
    }

    @AfterEach
    void tearDown() {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        kafka.close();
    }

    @Test
    void streamsMetadata() {
        final var streamsMetadata = metadataService.streamsMetadata();

        LOGGER.info("Metadata: " + streamsMetadata);
        Assertions.assertFalse(streamsMetadata.isEmpty());
        Assertions.assertTrue(streamsMetadata.containsValue(Collections.singleton(WORDCOUNT_STORE)));
    }

    @Test
    void streamsMetadataOverAPI() {
        TestUtil.test(application, (server, client) -> {
            assertThat(client.get("/metadata").code()).isEqualTo(200);

            final var body = client.get("/metadata").body();
            assertThat(body).isNotNull();

            final var str = body.string();
            assertThat(str).contains(WORDCOUNT_STORE);

            LOGGER.info("Metadata: " + str);
        });
    }

    @Test
    void streamsMetadataForStore() {
        final var streamsMetadata = metadataService.streamsMetadataForStore(WORDCOUNT_STORE);

        LOGGER.info("Metadata: " + streamsMetadata);
        Assertions.assertFalse(streamsMetadata.isEmpty());
        Assertions.assertTrue(streamsMetadata.containsValue(Collections.singleton(WORDCOUNT_STORE)));
    }

    @Test
    void streamsMetadataForStoreOverAPI() {
        TestUtil.test(application, (server, client) -> {
            assertThat(client.get("/metadata/" + WORDCOUNT_STORE).code()).isEqualTo(200);

            final var body = client.get("/metadata/" + WORDCOUNT_STORE).body();
            assertThat(body).isNotNull();

            final var str = body.string();
            assertThat(str).contains(WORDCOUNT_STORE);

            LOGGER.info("Metadata: " + str);
        });
    }
}
