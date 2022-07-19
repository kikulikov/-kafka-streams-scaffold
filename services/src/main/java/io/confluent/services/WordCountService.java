package io.confluent.services;

import io.javalin.Javalin;
import io.javalin.http.Context;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Collections;
import java.util.Map;

import static io.confluent.streams.WordCountTopology.WORDCOUNT_STORE;

public class WordCountService extends AbstractService {

    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountService.class);
    static final String WORDCOUNT_PATH = "wordcount";

    private final KafkaStreams streams;
    private final Javalin server;
    private final String serverHost;
    private final int serverPort;

    public WordCountService(KafkaStreams streams, Javalin server, String serverHost, int serverPort) {
        this.streams = streams;
        this.server = server;
        this.serverHost = serverHost;
        this.serverPort = serverPort;
    }

    @Override
    public void bind() {
        server.get("/" + WORDCOUNT_PATH + "/{key}", this::handle);
    }

    private boolean thisInstance(final HostInfo host) {
        return serverHost.equals(host.host()) && serverPort == host.port();
    }

    public void handle(Context ctx) throws Exception {
        final var key = ctx.pathParam("key");
        final var host = streamsMetadataForStoreAndKey(WORDCOUNT_STORE, key, Serdes.String().serializer());

        if (thisInstance(host)) {
            final var s = byStoreAndKey(WORDCOUNT_STORE, key);
            ctx.json(s);

        } else {
            final URL localAddress = new URL(ctx.fullUrl());
            final String localHost = localAddress.getHost();
            final String localPort = String.valueOf(localAddress.getPort());

            final var redirect = ctx.fullUrl()
                    .replaceFirst(localHost, host.host())
                    .replaceFirst(localPort, String.valueOf(host.port()));

            ctx.redirect(redirect);
        }

//        final var redirect = "http://" + host.host() + ":" + host.port() + "/" + WORDCOUNT_STORE + "/" + key;
//
//        final var request = new Request.Builder()
//                .url(redirect).header("Connection", "close").get().build();
//
//        final var client = new OkHttpClient();
//
//        try (Response response = client.newCall(request).execute()) {
//            System.out.println("Response: " + response.toString());
//            ctx.json(response.body().string());
//        }
//
//        System.out.println(">>> full url " + ctx.fullUrl());
//        System.out.println(">>> meta host " + host);
//        System.out.println(">>> redirect " + redirect);
    }

    /**
     * @param store the store to look in
     * @param key   the key to get
     */
    public Map<String, Long> byStoreAndKey(String store, String key) {

        LOGGER.info("Extracting the value from the state store [{}] by the key [{}]", store, key);

        // Lookup the KeyValueStore with the provided store
        final ReadOnlyKeyValueStore<String, Long> keyValueStore =
                streams.store(storeParameters(store));

        if (keyValueStore == null) {
            LOGGER.error(stateStoreNotFoundMsg(store));
        }

        // Get the value from the store
        final var value = keyValueStore.get(key);

        if (value == null) {
            LOGGER.error(recordNotFoundMsg(store, key));
        }

        return Collections.singletonMap(key, value);
    }

    public <K> HostInfo streamsMetadataForStoreAndKey(final String store, final K key, final Serializer<K> serializer) {
        return streams.queryMetadataForKey(store, key, serializer).activeHost();
    }

    private StoreQueryParameters<ReadOnlyKeyValueStore<String, Long>> storeParameters(String store) {
        return StoreQueryParameters.fromNameAndType(store, QueryableStoreTypes.keyValueStore());
    }

    private String stateStoreNotFoundMsg(String store) {
        return String.format("State store [%s] not found.", store);
    }

    private String recordNotFoundMsg(String store, String key) {
        return String.format("Record [%s] not found in the state store [%s].", key, store);
    }
}
