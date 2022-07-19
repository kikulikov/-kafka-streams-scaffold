package io.confluent.interceptors;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FailProofInterceptor<K, V> implements ConsumerInterceptor<K, V> {

    // [groupId,topicName,partitionNumber]::[Offset]
    public static final String AUDIT_OFFSETS = "audit-offsets";

    // private KafkaConsumer<String, Long> auditConsumer;
    private KafkaProducer<String, Long> auditProducer;

    private String auditTopic = "demo-plaintext-input";
    private String auditApplicationId = null;
    private int auditRetries = 5;

    @Override
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        if (auditApplicationId != null) {
            records.forEach(rec -> {
                if (auditTopic.equals(rec.topic())) {
                    final var key = auditApplicationId + "," + rec.topic() + "," + rec.partition();
                    final Long offset = rec.offset();
                    auditProducer.send(new ProducerRecord<>(AUDIT_OFFSETS, key, offset));
                    System.out.println(">>> consume >>> " + key + "::" + offset);
                }
            });
        }
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (auditApplicationId != null) {
            offsets.forEach((m, n) -> {
                if (auditTopic.equals(m.topic())) {
                    final var key = auditApplicationId + "," + m.topic() + "," + m.partition();
                    final var offset = n.offset();
                    System.out.println(">>> commit >>> " + key + "::" + offset);
                }
            });
        }
    }

    @Override
    public void close() {
//        if (auditConsumer != null) {
//            auditConsumer.close();
//        }
        if (auditProducer != null) {
            auditProducer.close();
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        if (configs.containsKey(StreamsConfig.APPLICATION_ID_CONFIG)) {
            auditApplicationId = configs.get(StreamsConfig.APPLICATION_ID_CONFIG).toString();

            // Audit Producer
            final Map<String, Object> producerConfig = new HashMap<>(configs);
            producerConfig.put(ProducerConfig.ACKS_CONFIG, "1");
            producerConfig.put(ProducerConfig.RETRIES_CONFIG, "0");
            producerConfig.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "0");
            producerConfig.remove(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG);
            producerConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, auditApplicationId + "-audit-producer");
            producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, auditApplicationId + "-audit-producer");
            producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());

            auditProducer = new KafkaProducer<>(producerConfig);

            // Audit Consumer
            final Map<String, Object> consumerConfig = new HashMap<>(configs);
            consumerConfig.remove(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG);
            consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, auditApplicationId + "-Audit");
            consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, auditApplicationId + "-Audit");
            consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, auditApplicationId + "-Audit");
            consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

            try (final var kafkaConsumer = new KafkaConsumer<String, Long>(consumerConfig)) {
                kafkaConsumer.subscribe(Collections.singleton(AUDIT_OFFSETS));
                kafkaConsumer.poll(Duration.ZERO);

                Thread.sleep(10000);

                while (true) {
                    System.out.println(">>> Polling");
                    final ConsumerRecords<String, Long> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                    records.forEach(System.out::println);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
