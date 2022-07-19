package io.confluent.interceptors;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Calendar;
import java.util.Map;

public class AuditConsumerInterceptor<K, V> implements ConsumerInterceptor<K, V> {

    // Record Metadata
    private static final String TRACKING_PARTITION = "partition";
    private static final String TRACKING_OFFSET = "offset";
    private static final String TRACKING_TIMESTAMP = "timestamp";
    private static final String TRACKING_TOPIC = "topic";

    private static final String JSON_OPEN_BRACKET = "{";
    private static final String JSON_CLOSE_BRACKET = "}";

    // private String auditTopic;
    private final String auditApplicationId = "zzz";

    @Override
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        records.forEach(r -> {
            final var tracking = getJsonTrackingMessage(r);
            System.out.println(">>> consuming >>>" + tracking);
        });
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((m, n) -> {
            System.out.println(">>> committing >>> " + m.topic() + ", " + m.partition() + ", " + n.offset());
        });
    }

    private String getJsonTrackingMessage(ConsumerRecord<K, V> record) {
        return JSON_OPEN_BRACKET +
                "\"" + "timestamp" + "\":\"" + Calendar.getInstance().getTimeInMillis() + "\"" +
                "\",client\":" +
                JSON_OPEN_BRACKET +
                "\"" + "applicationId" + "\":\"" + auditApplicationId + "\"" +
                ",\"" + "type" + "\":\"consumer\"" +
                JSON_CLOSE_BRACKET +
                ",\"record\":" +
                JSON_OPEN_BRACKET +
                "\"" + TRACKING_PARTITION + "\":\"" + record.partition() + "\"" +
                ",\"" + TRACKING_TOPIC + "\":\"" + record.topic() + "\"" +
                ",\"" + TRACKING_OFFSET + "\":\"" + record.offset() + "\"" +
                ",\"" + TRACKING_TIMESTAMP + "\":\"" + record.timestamp() + "\"" +
                JSON_CLOSE_BRACKET +
                JSON_CLOSE_BRACKET;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
//        final Map<String, Object> copyConfigs = new HashMap<>(configs);
//        // Drop interceptor classes configuration to not introduce loop.
//        copyConfigs.remove(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG);
//
//        this.originalsClientId = (String) configs.get(ProducerConfig.CLIENT_ID_CONFIG);
//
//        String interceptorClientId = (originalsClientId == null) ?
//                "interceptor-consumer-" + ClientIdGenerator.nextClientId() :
//                "interceptor-" + originalsClientId;
//
//        copyConfigs.put(ProducerConfig.CLIENT_ID_CONFIG, interceptorClientId);
//
//        this.configs = new AuditInterceptorConfig(copyConfigs);
//
//        copyConfigs.putAll(this.configs.getOverrideProducerConfigs());
//
//        // Enforce some properties to get a non-blocking producer;
//        copyConfigs.put(ProducerConfig.RETRIES_CONFIG, "0");
//        copyConfigs.put(ProducerConfig.ACKS_CONFIG, "1");
//        copyConfigs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "0");
//        copyConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        copyConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//
//        this.producer = new KafkaProducer<>(copyConfigs);
    }

//    private static class ClientIdGenerator {
//
//        private static final AtomicInteger IDS = new AtomicInteger(0);
//
//        static int nextClientId() {
//            return IDS.getAndIncrement();
//        }
//    }
}
