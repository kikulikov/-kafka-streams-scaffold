package io.confluent.streams;

import io.confluent.resilience.RetrieveContextTransformer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Locale;

public class WordCountTopology {

    public static final Logger LOGGER = LoggerFactory.getLogger(WordCountTopology.class);
    public static final String WORDCOUNT_INPUT = "demo-plaintext-input";
    public static final String WORDCOUNT_OUTPUT = "demo-wordcount-output";
    public static final String WORDCOUNT_STORE = "demo-wordcount-store";

    public Topology buildTopology() {

        final StreamsBuilder builder = new StreamsBuilder();

        builder.<String, String>stream(WORDCOUNT_INPUT, Consumed.as("plaintext-read"))
                .transform(() -> new RetrieveContextTransformer<>()) // TODO !!!
                .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
                .groupBy((key, value) -> value)
                .count(Named.as("counting-words"), Materialized.as(WORDCOUNT_STORE))
                .toStream()
                .peek((m, n) -> LOGGER.debug("Counted [" + m + ":" + n + "]"))
                .to(WORDCOUNT_OUTPUT, Produced.with(Serdes.String(), Serdes.Long()));

        return builder.build();
    }
}
