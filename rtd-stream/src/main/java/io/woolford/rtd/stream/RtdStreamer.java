package io.woolford.rtd.stream;


import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.woolford.rtd.BusPositionFeed;
import io.woolford.rtd.BusPositionSpeed;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;


public class RtdStreamer {

    private static final Logger LOG = LoggerFactory.getLogger(RtdStreamer.class);

    public static void main(String[] args) {

        //TODO: log interesting events

        //TODO: create required topics, e.g. "Could not create topic rtd-stream-busPositionStore-changelog"

        //TODO: add gauges/counters, etc... so they can be exposed in Prometheus

        //TODO: consider different version of the schema so the initial position doesn't have a field that contains a
        // mph of zero for every record

        //TODO: there *must* be a better way to handle properties

        //TODO: add environment props to README.md

        final StreamsBuilder builder = new StreamsBuilder();

        // get and set properties
        RtdStreamProperties rtdStreamProperties = new RtdStreamProperties();
        Properties props = rtdStreamProperties.getProperties();

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        // BusPosition serializer/deserializer
        final Serde<BusPositionFeed> serdeBusPosition = new SpecificAvroSerde<>();
        serdeBusPosition.configure((Map) props, false);

        // create key/value store for bus positions
        final StoreBuilder<KeyValueStore<String, BusPositionFeed>> busPositionStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("busPositionStore"),
                Serdes.String(),
                Serdes.serdeFrom(serdeBusPosition.serializer(), serdeBusPosition.deserializer()));

        builder.addStateStore(busPositionStore);

        // stream positions from the rtd-bus-position topic
        final KStream<String, BusPositionFeed> rtdBusPositionStream = builder.stream("rtd-bus-position");

        // calculate the speed using the Haversine transform
        final KStream<String, BusPositionSpeed> rtdBusPositionStreamEnriched =
                rtdBusPositionStream.transform(new HaversineTransformerSupplier("busPositionStore"), "busPositionStore");

        // write enriched records, i.e. records with speed based on the previous position, to the rtd-bus-position-enriched topic
        rtdBusPositionStreamEnriched.to("rtd-bus-position-enriched");

        // run it
        final Topology topology = builder.build();
        LOG.info(topology.describe().toString()); // write out the topology so it can be visualized in https://zz85.github.io/kafka-streams-viz/
        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

}
