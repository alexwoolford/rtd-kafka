package io.woolford.rtd.stream;


import com.uber.h3core.H3Core;
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

import java.io.IOException;
import java.util.Map;
import java.util.Properties;


public class RtdStreamer {

    private static final Logger LOG = LoggerFactory.getLogger(RtdStreamer.class);

    public static void main(String[] args) {

        //TODO: log interesting events

        //TODO: create required topics, e.g. "Could not create topic rtd-stream-busPositionStore-changelog"

        //TODO: add gauges/counters, etc... so they can be exposed in Prometheus

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

        // remove any enriched records with impossible speeds (i.e. > 140 mph), add the Uber H3 hexagon, and write
        // those records to the rtd-bus-position-enriched topic.
        rtdBusPositionStreamEnriched
                .filter((key, busPositionSpeed) -> busPositionSpeed.getMilesPerHour() < 140)
                .mapValues(busPositionSpeed -> {

                    H3Core h3 = null;
                    String hexAddr = "";
                    try {
                        // a resolution of 12 divides the land into hexagons that are about 307 square meters
                        // See https://github.com/uber/h3/blob/master/docs/core-library/restable.md and plug the hexagon
                        // area into https://www.wolframalpha.com/ to translate

                        h3 = H3Core.newInstance();
                        hexAddr = h3.geoToH3Address(busPositionSpeed.getLocation().getLat(),
                                                    busPositionSpeed.getLocation().getLon(),
                                                12);

                    } catch (IOException e) {
                        LOG.error("Error creating H3 address for location: " + busPositionSpeed.getLocation());
                        LOG.error(e.getMessage());
                    }

                    busPositionSpeed.setH3(hexAddr);
                    return busPositionSpeed;
                })
                .to("rtd-bus-position-enriched");

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
