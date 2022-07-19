package io.confluent.streams;

import io.confluent.model.CountAndTotal;
import io.confluent.model.Purchase;
import io.confluent.serde.GsonSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PurchasesTopology {

    public static final Logger LOGGER = LoggerFactory.getLogger(PurchasesTopology.class);
    public static final String PURCHASES_INPUT = "demo-json-purchases";
    public static final String PURCHASES_AGGREGATE_STORE = "demo-json-aggregate-store";
    public static final String PURCHASES_OUTPUT = "demo-json-aggregate";

    private static final Serde<Purchase> PURCHASE_SERDE = Serdes.serdeFrom(
            new GsonSerde.GsonSerializer<>(),
            new GsonSerde.GsonDeserializer<>(Purchase.class)
    );

    private static final Serde<CountAndTotal> CAT_SERDE = Serdes.serdeFrom(
            new GsonSerde.GsonSerializer<>(),
            new GsonSerde.GsonDeserializer<>(CountAndTotal.class)
    );

    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final String STRING_SERDE_NAME = Serdes.String().getClass().getName();

    /*
    <customerId> :: < customerId | productId | quantity >

    Scenario: Calculate the number of orders and purchased products per customer

    GIVEN The input data stream of purchases is highly skewed
    WHEN events are repartitioned per customer and product
    THEN total quantity calculated and returned as a result
     */

    public Topology buildTopology() {

        final StreamsBuilder builder = new StreamsBuilder();

        final Materialized<String, CountAndTotal, KeyValueStore<Bytes, byte[]>> mat =
                Materialized.<String, CountAndTotal, KeyValueStore<Bytes, byte[]>>as(PURCHASES_AGGREGATE_STORE)
                        .withKeySerde(STRING_SERDE).withValueSerde(CAT_SERDE);

        final KTable<String, CountAndTotal> aggregated =
                builder.stream(PURCHASES_INPUT, Consumed.with(STRING_SERDE, PURCHASE_SERDE))
                        .selectKey((m, n) -> n.getCustomerId() + "-" + n.getProductId())
                        .repartition()
                        .groupByKey()
                        .aggregate(this::buildCountAndTotal, (key, value, agg) -> {
                            agg.setCount(agg.getCount() + 1);
                            agg.setTotal(agg.getTotal() + value.getQuantity());
                            return agg;
                        }, mat);

        aggregated.toStream()
                .peek((m, n) -> LOGGER.debug("Aggregated [" + m + ":" + n + "]"))
                .to(PURCHASES_OUTPUT, Produced.with(STRING_SERDE, CAT_SERDE));

        return builder.build();
    }

    private CountAndTotal buildCountAndTotal() {
        final var cat = new CountAndTotal();
        cat.setCount(0);
        cat.setTotal(0);
        return cat;
    }
}
