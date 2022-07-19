package io.confluent.services;

import io.javalin.Javalin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.ClassRule;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static io.confluent.streams.WordCountTopology.WORDCOUNT_INPUT;
import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;

public class AbstractServiceTests {

    static final String STRING_SERDE_NAME = Serdes.String().getClass().getName();
    static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:7.0.1";
    static final DockerImageName KAFKA_IMAGE = DockerImageName.parse(KAFKA_IMAGE_NAME);

    final Javalin application = Javalin.create();
    KafkaStreams kafkaStreams;

    @ClassRule
    static final KafkaContainer kafka = new KafkaContainer(KAFKA_IMAGE);

    Properties producerProperties() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, this.getClass().getSimpleName() + "Producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    void createTopics(String... topics) {
        final var newTopics = Arrays.stream(topics)
                .map(topic -> new NewTopic(topic, 5, (short) 1))
                .collect(Collectors.toList());

        final var config =
                Map.<String, Object>of(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

        try (var admin = AdminClient.create(config)) {
            admin.createTopics(newTopics);
        }
    }

    void produceEvent(String value) {
        final var producer = new KafkaProducer<String, String>(producerProperties());

        try {
            producer.send(new ProducerRecord<>(WORDCOUNT_INPUT, null, value)).get();
            producer.flush();
            producer.close();

            Thread.sleep(500);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    Properties streamsProperties() {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, this.getClass().getSimpleName() + "-" + Math.random());
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, STRING_SERDE_NAME);
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, STRING_SERDE_NAME);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // Important! Property you must have configured to make metadata service work.
        props.setProperty(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:8080");
        return props;
    }
}
