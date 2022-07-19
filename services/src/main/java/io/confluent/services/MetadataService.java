package io.confluent.services;

import io.javalin.Javalin;
import io.javalin.http.Context;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.StreamsMetadata;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Looks up StreamsMetadata from KafkaStreams.
 */
public class MetadataService extends AbstractService {

    private static final Collector<StreamsMetadata, ?, Map<HostInfo, Set<String>>> TO_MAP =
            Collectors.toMap(StreamsMetadata::hostInfo, StreamsMetadata::stateStoreNames);
    private static final String METADATA_PATH = "metadata";
    private final KafkaStreams streams;
    private final Javalin server;

    public MetadataService(KafkaStreams streams, Javalin server) {
        this.streams = streams;
        this.server = server;
    }

    @Override
    public void bind() {
        server.get("/" + METADATA_PATH, this::handleMetadata);
        server.get("/" + METADATA_PATH + "/{store}", this::handleMetadataForStore);
    }

    public void handleMetadata(Context ctx) {
        ctx.json(streamsMetadata());
    }

    public void handleMetadataForStore(Context ctx) {
        final var store = ctx.pathParam("store");
        ctx.json(streamsMetadataForStore(store));
    }

    /**
     * Get the metadata for all the instances of this Kafka Streams application
     */
    public Map<HostInfo, Set<String>> streamsMetadata() {
        return streams.allMetadata().stream().collect(TO_MAP);
    }

    /**
     * Get the metadata for all instances of this Kafka Streams application that currently has the
     * provided store.
     *
     * @param store The store to locate
     */
    public Map<HostInfo, Set<String>> streamsMetadataForStore(final String store) {
        return streams.allMetadataForStore(store).stream().collect(TO_MAP);
    }
}
