package io.confluent.serde;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;

public class GsonSerde { // <T extends Object> extends Serdes.WrapperSerde<T> {

//    private final Class<T> clazz;
//
//    public GsonSerde(Serializer<T> serializer, Deserializer<T> deserializer, Class<T> clazz) {
//        super(serializer, deserializer);
//        this.clazz = clazz;
//    }

    public static class GsonSerializer<M extends Object> implements Serializer<M> {
        @Override
        public byte[] serialize(String topic, M data) {
            return new Gson().toJson(data).getBytes(StandardCharsets.UTF_8);
        }
    }

    public static class GsonDeserializer<N extends Object> implements Deserializer<N> {

        private final Class<N> clazz;

        public GsonDeserializer(Class<N> clazz) {
            this.clazz = clazz;
        }

        @Override
        public N deserialize(String topic, byte[] data) {
            final String str = new String(data, StandardCharsets.UTF_8);
            return new Gson().fromJson(str, clazz);
        }
    }
}
