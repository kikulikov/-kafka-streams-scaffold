package io.confluent.resilience;

import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

public class ReplaceThreadExceptionHandler implements StreamsUncaughtExceptionHandler {

    @Override
    public StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse handle(Throwable exception) {
        var context = ContextContainer.threadLocal.get();
        System.out.println(context.topic());
        System.out.println(context.partition());
        System.out.println(context.offset());
        System.out.println("Handling the failure and promoting the offset");
        context.commit(); // committing the current offset though it was not processed properly

        return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD; // replaces the thread to proceed
    }
}
