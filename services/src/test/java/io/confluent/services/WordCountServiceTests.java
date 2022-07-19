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
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.confluent.streams.WordCountTopology.*;
import static org.assertj.core.api.Assertions.assertThat;

class WordCountServiceTests extends AbstractServiceTests {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataServiceTests.class);
    private WordCountService wordCountService;

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

        wordCountService = new WordCountService(kafkaStreams, application, "localhost", 8080);
        wordCountService.bind();
    }

    @AfterEach
    void tearDown() {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        kafka.close();
    }

    @Test
    void recordByKey() {
        produceEvent("the quick brown fox jumps over the lazy dog");
        final var searchResult2 = wordCountService.byStoreAndKey(WORDCOUNT_STORE, "fox");
        Assertions.assertEquals(1L, searchResult2.get("fox"));

        produceEvent("the quick brown fox jumps over the lazy dog");
        final var searchResult3 = wordCountService.byStoreAndKey(WORDCOUNT_STORE, "fox");
        Assertions.assertEquals(2L, searchResult3.get("fox"));
    }

    @Test
    void recordByKeyOverAPI() {
        TestUtil.test(application, (server, client) -> {
            produceEvent("the quick brown fox jumps over the lazy dog");

            final var response1 = client.get("/wordcount/fox");
            assertThat(response1.code()).isEqualTo(200);
            assertThat(response1.body()).isNotNull();
            assertThat(response1.body().string()).isEqualTo("{\"fox\":1}");

            produceEvent("the quick brown fox jumps over the lazy dog");
            produceEvent("the quick brown fox jumps over the lazy dog");

            final var response2 = client.get("/wordcount/fox");
            assertThat(response2.code()).isEqualTo(200);
            assertThat(response2.body()).isNotNull();
            assertThat(response2.body().string()).isEqualTo("{\"fox\":3}");
        });
    }
}