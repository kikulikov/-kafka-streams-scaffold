package io.confluent.services;

import io.confluent.resilience.ContextContainer;
import io.confluent.resilience.RetrieveContextTransformer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class ReplaceThreadTests {

    private static final String STRING_SERDE_NAME = Serdes.String().getClass().getName();
    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.0.1");
    private static final String APPLICATION_ID = "TEST";

    private static final String INPUT_TOPIC = "input";
    private static final String OUTPUT_TOPIC = "output";

    @ClassRule
    static final KafkaContainer kafka = new KafkaContainer(KAFKA_IMAGE);
    private KafkaStreams kafkaStreams;

    @BeforeEach
    void setUp() throws Exception {
        kafka.withEnv("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", "1");
        kafka.withStartupTimeout(Duration.ofSeconds(60));
        kafka.start();

        try (var admin = AdminClient.create(adminConfig())) {
            admin.createTopics(Stream.of(INPUT_TOPIC, OUTPUT_TOPIC)
                    .map(n -> new NewTopic(n, 3, (short) 1))
                    .collect(Collectors.toList())).all().get();
        }

        kafkaStreams = new KafkaStreams(buildTopology(), streamsConfig());
        kafkaStreams.setUncaughtExceptionHandler(new StreamsUncaughtExceptionHandler() {
            @Override
            public StreamThreadExceptionResponse handle(Throwable exception) {
                var context = ContextContainer.threadLocal.get();
                System.out.println(context.topic());
                System.out.println(context.partition());
                System.out.println(context.offset());
                System.out.println("Handling the failure and promoting the offset");
                context.commit(); // committing the current offset though it was not processed properly

                return StreamThreadExceptionResponse.REPLACE_THREAD; // replaces the thread to proceed
            }
        });
        kafkaStreams.start();
    }

    private Topology buildTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream(INPUT_TOPIC)
                .transform(() -> new RetrieveContextTransformer<>())
                .mapValues(s -> {
                    if ("quack".equals(s)) throw new RuntimeException("quacking failure"); // might fail here
                    return s.toUpperCase(Locale.ROOT);
                })
                .to(OUTPUT_TOPIC);
        return builder.build();
    }

    private Properties adminConfig() {
        final Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, APPLICATION_ID + "-test-admin");
        return props;
    }

    private Properties producerConfig(Class<? extends Serializer<?>> valueKlass) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, APPLICATION_ID + "-test-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueKlass.getName());
        return props;
    }

    private Properties consumerConfig(Class<? extends Deserializer<?>> valueKlass) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, APPLICATION_ID + "-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueKlass.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private Properties streamsConfig() {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, STRING_SERDE_NAME);
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, STRING_SERDE_NAME);
        return props;
    }

    @Test
    void manipulatingContext() throws Exception {

        new Thread("other thread") {
            public void run() {
                Assertions.assertNull(ContextContainer.threadLocal.get());
            }
        }.start();

        try (var producer = new KafkaProducer<String, String>(producerConfig(StringSerializer.class))) {
            producer.send(new ProducerRecord<>(INPUT_TOPIC, "test", "moo")).get();
            producer.send(new ProducerRecord<>(INPUT_TOPIC, "test", "moo moo")).get();
            producer.send(new ProducerRecord<>(INPUT_TOPIC, "test", "quack")).get();
            producer.send(new ProducerRecord<>(INPUT_TOPIC, "test", "moo moo moo")).get();
        }

        try (var consumer = new KafkaConsumer<String, String>(consumerConfig(StringDeserializer.class))) {
            consumer.subscribe(Collections.singleton(OUTPUT_TOPIC));

            for (int i = 0; i < 20; i++) {
                final var records = consumer.poll(Duration.ofMillis(500));
                records.forEach(record -> {
                    System.out.println(">>> " + record);
                });
            }
        }

        try (var producer = new KafkaProducer<String, String>(producerConfig(StringSerializer.class))) {
            producer.send(new ProducerRecord<>(INPUT_TOPIC, "test", "oink")).get();
            producer.send(new ProducerRecord<>(INPUT_TOPIC, "test", "oink oink")).get();
        }

        try (var consumer = new KafkaConsumer<String, String>(consumerConfig(StringDeserializer.class))) {
            consumer.subscribe(Collections.singleton(OUTPUT_TOPIC));

            for (int i = 0; i < 20; i++) {
                final var records = consumer.poll(Duration.ofMillis(500));
                records.forEach(record -> {
                    System.out.println(">>> " + record);
                });
            }
        }
    }

    @AfterEach
    void tearDown() {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        kafka.close();
    }
}