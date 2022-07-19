package io.confluent.services;

import com.google.gson.Gson;
import io.confluent.model.CountAndTotal;
import io.javalin.Javalin;
import io.javalin.http.Context;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static io.confluent.streams.PurchasesTopology.PURCHASES_AGGREGATE_STORE;

public class PurchasesService extends AbstractService {

    private static final Logger LOGGER = LoggerFactory.getLogger(PurchasesService.class);
    static final String PATH = "purchases";

    private final KafkaStreams streams;
    private final Javalin server;
    private final String serverHost;
    private final int serverPort;

    public PurchasesService(KafkaStreams streams, Javalin server, String serverHost, int serverPort) {
        this.streams = streams;
        this.server = server;
        this.serverHost = serverHost;
        this.serverPort = serverPort;
    }

    @Override
    public void bind() {
        server.get("/" + PATH + "/{key}", this::handle);
    }

    private boolean thisInstance(final HostInfo host) {
        return serverHost.equals(host.host()) && serverPort == host.port();
    }

    // ordered events - timestamp?
    // pagination / max number
    // perf testing

    public void handle(Context ctx) throws Exception {
        final var key = ctx.pathParam("key");
        final var hosts = streamsMetadataForStore(PURCHASES_AGGREGATE_STORE);
        final Map<String, CountAndTotal> result = new HashMap<>();

        for (Map.Entry<HostInfo, Set<String>> entry : hosts.entrySet()) {

            if (thisInstance(entry.getKey())) {
                final var s = range(PURCHASES_AGGREGATE_STORE, key);
                result.putAll(s);
            } else if (!"true".equalsIgnoreCase(ctx.header("X-Provenance-Enabled"))) {
                final URL localAddress = new URL(ctx.fullUrl());
                final String localHost = localAddress.getHost();
                final String localPort = String.valueOf(localAddress.getPort());

                final var redirect = ctx.fullUrl()
                        .replaceFirst(localHost, entry.getKey().host())
                        .replaceFirst(localPort, String.valueOf(entry.getKey().port()));

                final var request = new Request.Builder().url(redirect).get()
                        .header("X-Provenance-Enabled", "true")
                        .build();
                final var client = new OkHttpClient();

                try (Response response = client.newCall(request).execute()) {
                    final var payload = response.body().string();

                    final Map<String, CountAndTotal> parsed =
                            new Gson().<Map<String, CountAndTotal>>fromJson(payload, result.getClass());

                    result.putAll(parsed);
                }
            }
        }

        ctx.json(result);
    }

    /**
     * @param store the store to look in
     * @param key   the key to get
     */
    public Map<String, CountAndTotal> range(String store, String key) {

        LOGGER.info("Extracting the value from the state store [{}] by the key [{}]", store, key);

        // Lookup the KeyValueStore with the provided store
        final ReadOnlyKeyValueStore<String, CountAndTotal> keyValueStore =
                streams.store(storeParameters(store));

        if (keyValueStore == null) {
            LOGGER.error(stateStoreNotFoundMsg(store));
        }

        // Get the value from the store
        final var values = keyValueStore.range(key + "-00000", key + "-99999");

        final var result = new HashMap<String, CountAndTotal>();

        while (values.hasNext()) {
            final var next = values.next();
            result.put(next.key, next.value);
        }

        return result;
    }

    private static final Collector<StreamsMetadata, ?, Map<HostInfo, Set<String>>> TO_MAP =
            Collectors.toMap(StreamsMetadata::hostInfo, StreamsMetadata::stateStoreNames);

    private Map<HostInfo, Set<String>> streamsMetadataForStore(final String store) {
        return streams.allMetadataForStore(store).stream().collect(TO_MAP);
    }

    private StoreQueryParameters<ReadOnlyKeyValueStore<String, CountAndTotal>> storeParameters(String store) {
        return StoreQueryParameters.fromNameAndType(store, QueryableStoreTypes.keyValueStore());
    }

    private String stateStoreNotFoundMsg(String store) {
        return String.format("State store [%s] not found.", store);
    }
}
