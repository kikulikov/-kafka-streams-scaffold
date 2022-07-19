package io.confluent.services;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class InterceptorTests {

    private static final String STRING_SERDE_NAME = Serdes.String().getClass().getName();
    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.0.1");
    private static final String APPLICATION_ID = "TEST"; // UUID.randomUUID().toString();

    private static final String INPUT_TOPIC = "input";
    private static final String AUDIT_OFFSETS = "audit-offsets";
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

            admin.createTopics(Stream.of(AUDIT_OFFSETS)
                    .map(n -> new NewTopic(n, 1, (short) 1))
                    .collect(Collectors.toList())).all().get();
        }

        try (var producer = new KafkaProducer<String, String>(producerConfig(StringSerializer.class))) {
            producer.send(new ProducerRecord<>(INPUT_TOPIC, 1, "test", "quack")).get();
            producer.send(new ProducerRecord<>(INPUT_TOPIC, 1, "test", "quack quack")).get();
            producer.send(new ProducerRecord<>(INPUT_TOPIC, 1, "test", "quack quack quack")).get();
        }

        try (var producer = new KafkaProducer<String, Long>(producerConfig(LongSerializer.class))) {
            producer.send(new ProducerRecord<>(AUDIT_OFFSETS, APPLICATION_ID + "," + INPUT_TOPIC + "," + 1, 0L)).get();
            producer.send(new ProducerRecord<>(AUDIT_OFFSETS, APPLICATION_ID + "," + INPUT_TOPIC + "," + 1, 1L)).get();
            producer.send(new ProducerRecord<>(AUDIT_OFFSETS, APPLICATION_ID + "," + INPUT_TOPIC + "," + 1, 2L)).get();
        }

        kafkaStreams = new KafkaStreams(buildTopology(), streamsConfig());
        kafkaStreams.start();
    }

    private Topology buildTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream(INPUT_TOPIC)
                .mapValues(s -> s.toUpperCase(Locale.ROOT))
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
        props.put(ConsumerConfig.GROUP_ID_CONFIG, APPLICATION_ID + "-test-consumer");
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
        props.setProperty("consumer.interceptor.classes", AuditInterceptor.class.getName());
        return props;
    }

    public static class AuditInterceptor<K, V> implements ConsumerInterceptor<K, V> {

        private KafkaProducer<String, Long> auditProducer;

        @Override
        public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
            if (APPLICATION_ID != null) {
                records.forEach(record -> {
                    if (INPUT_TOPIC.equals(record.topic())) {
                        final var key = APPLICATION_ID + "," + record.topic() + "," + record.partition();
                        final Long offset = record.offset();
                        auditProducer.send(new ProducerRecord<>(AUDIT_OFFSETS, key, offset));
                        auditProducer.flush(); // TODO fix
                        System.out.println(">>> consume >>> " + key + "::" + offset);
                    }
                });
            }
            return records;
        }

        @Override
        public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
            // do nothing
        }

        @Override
        public void close() {
            if (auditProducer != null) {
                auditProducer.close();
            }
        }

        @Override
        public void configure(Map<String, ?> configs) {
            if (configs.containsKey(StreamsConfig.APPLICATION_ID_CONFIG)) {

                // Audit Producer
                final Map<String, Object> producerConfig = new HashMap<>(configs);
                producerConfig.put(ProducerConfig.ACKS_CONFIG, "1");
                producerConfig.put(ProducerConfig.RETRIES_CONFIG, "0");
                producerConfig.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "0");
                producerConfig.put(ProducerConfig.LINGER_MS_CONFIG, "0");
                producerConfig.remove(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG);
                producerConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID + "-audit-producer");
                producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, APPLICATION_ID + "-audit-producer");
                producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
                producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());

                auditProducer = new KafkaProducer<>(producerConfig);

                // Audit Consumer
                final Map<String, Object> consumerConfig = new HashMap<>(configs);
                consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
                consumerConfig.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
                consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                consumerConfig.remove(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG);
                consumerConfig.remove(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG);
                consumerConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID + "-audit-consumer");
                consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, APPLICATION_ID + "-audit-consumer");
                consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, APPLICATION_ID + "-audit-consumer");
                consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

                final var auditConsumer = new KafkaConsumer<String, String>(consumerConfig);
                auditConsumer.subscribe(Collections.singleton(AUDIT_OFFSETS));

                final var ends = auditConsumer.endOffsets(Collections.singleton(new TopicPartition(AUDIT_OFFSETS, 0)));

                // https://stackoverflow.com/questions/54480715/no-current-assignment-for-partition-occurs-even-after-poll-in-kafka
                // auditConsumer.seekToBeginning(Collections.singleton(new TopicPartition(AUDIT_OFFSETS, 0)));

                ends.forEach((tp, endOffset) -> {
                    var marker = 0L;

                    while (marker < endOffset) {
                        System.out.println(">>> Polling");
                        final ConsumerRecords<String, String> records = auditConsumer.poll(Duration.ofMillis(1000));
                        final var iter = records.iterator();

                        while (iter.hasNext()) {
                            var record = iter.next();
                            marker = Math.max(record.offset(), marker + 1);
                        }
                    }
                });
            }
        }
    }

    @Test
    void interceptor() throws Exception {
        try (var producer = new KafkaProducer<String, String>(producerConfig(StringSerializer.class))) {
            producer.send(new ProducerRecord<>(INPUT_TOPIC, 1, "test", "moo")).get();
            producer.send(new ProducerRecord<>(INPUT_TOPIC, 1, "test", "moo moo")).get();
        }

        Thread.sleep(1000);

        try (final var consumer = new KafkaConsumer<String, Long>(consumerConfig(LongDeserializer.class))) {
            consumer.subscribe(Collections.singleton(AUDIT_OFFSETS));

            for (int i = 0; i < 20; i++) {
                final var records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> {
                    System.out.println(">>> " + record);
                });
                Thread.sleep(500);
            }
        }

        Assertions.assertTrue(true);
    }

    @AfterEach
    void tearDown() {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        kafka.close();
    }
}