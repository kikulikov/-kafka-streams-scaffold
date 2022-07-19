package io.confluent.serde;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class JsonObjectSerde extends Serdes.WrapperSerde<JsonObject> {

    public JsonObjectSerde() {
        super(new JsonObjectSerializer(), new JsonObjectDeserializer());
    }

    static class JsonObjectSerializer implements Serializer<JsonObject> {
        public byte[] serialize(String topic, JsonObject data) {
            return data.toString().getBytes(StandardCharsets.UTF_8);
        }
    }

    static class JsonObjectDeserializer implements Deserializer<JsonObject> {
        @Override
        public JsonObject deserialize(String topic, byte[] data) {
            final String str = new String(data, StandardCharsets.UTF_8);
            return JsonParser.parseString(str).getAsJsonObject();
        }
    }
}
