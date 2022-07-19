package io.confluent.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

class WordCountTopologyTest {

    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;

    @BeforeEach
    void setUp() {
        final Topology topology = new WordCountTopology().buildTopology();
        final TopologyTestDriver testDriver = new TopologyTestDriver(topology, getConfig());

        inputTopic = testDriver.createInputTopic(WordCountTopology.WORDCOUNT_INPUT,
                Serdes.String().serializer(), Serdes.String().serializer());

        outputTopic = testDriver.createOutputTopic(WordCountTopology.WORDCOUNT_OUTPUT,
                Serdes.String().deserializer(), Serdes.Long().deserializer());
    }

    private Properties getConfig() {
        final String stringSerde = Serdes.String().getClass().getName();

        final Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "testing-" + Math.random());
        config.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde);
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde);

        return config;
    }

    @Test
    void testBasicScenario() {
        inputTopic.pipeInput("A property list can contain another property list as");
        inputTopic.pipeInput("this second property list is searched if");

        var intermediateResult = outputTopic.readKeyValuesToMap();
        Assertions.assertEquals(3L, intermediateResult.get("list"));

        inputTopic.pipeInput("the property key is not found in the original property list");

        var result = outputTopic.readKeyValuesToMap();
        Assertions.assertEquals(4L, result.get("list"));
    }
}