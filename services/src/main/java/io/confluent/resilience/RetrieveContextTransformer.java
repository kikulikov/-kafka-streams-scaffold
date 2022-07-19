package io.confluent.resilience;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

public class RetrieveContextTransformer<K, V> implements Transformer<K, V, KeyValue<K, V>> {

    @Override
    public void init(ProcessorContext context) {
        ContextContainer.threadLocal.set(context);
    }

    @Override
    public KeyValue<K, V> transform(K key, V value) {
        return new KeyValue<>(key, value); // pass through
    }

    @Override
    public void close() {
        ContextContainer.threadLocal.remove();
    }
}