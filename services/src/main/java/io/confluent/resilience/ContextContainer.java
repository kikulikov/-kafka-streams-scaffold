package io.confluent.resilience;

import org.apache.kafka.streams.processor.ProcessorContext;

public class ContextContainer {
    public static final ThreadLocal<ProcessorContext> threadLocal = new ThreadLocal<>();
}
