package io.confluent.serde;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonObjectSerdeTests {

    private static final String INPUT_TOPIC = "json-input-topic";
    private static final String OUTPUT_TOPIC = "json-output-topic";

    private static final JsonObjectSerde JSON_SERDE = new JsonObjectSerde();
    private static final String STRING_SERDE_NAME = Serdes.String().getClass().getName();

    private TestInputTopic<String, JsonObject> inputTopic;
    private TestOutputTopic<String, JsonObject> outputTopic;

    @BeforeEach
    void setUp() {
        final Topology topology = buildTopology();
        final TopologyTestDriver testDriver = new TopologyTestDriver(topology, getConfig());

        inputTopic = testDriver.createInputTopic(INPUT_TOPIC,
                Serdes.String().serializer(), JSON_SERDE.serializer());

        outputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC,
                Serdes.String().deserializer(), JSON_SERDE.deserializer());
    }

    private Properties getConfig() {
        final Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "testing");
        config.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, STRING_SERDE_NAME);
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, STRING_SERDE_NAME);
        return config;
    }

    public Topology buildTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), JSON_SERDE))
                .mapValues(this::flagAsProcessed, Named.as("json-processor"))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), JSON_SERDE));

        return builder.build();
    }

    private JsonObject flagAsProcessed(JsonObject obj) {
        obj.addProperty("processed", true);
        return obj;
    }

    @Test
    void testJsonParser() {
        // constructs the first record
        var in1 = JsonParser.parseString("{\"hello\":\"world\"}").getAsJsonObject();

        // sends the first record
        inputTopic.pipeInput(in1);

        var out1 = outputTopic.readValue();
        assertEquals("world", out1.get("hello").getAsString());
        assertTrue(out1.get("processed").getAsBoolean());
    }
}