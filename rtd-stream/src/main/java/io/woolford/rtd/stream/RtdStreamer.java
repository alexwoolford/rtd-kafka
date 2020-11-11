package io.woolford.rtd.stream;


import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.woolford.rtd.BusPosition;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;


public class RtdStreamer {

    private static final Logger LOG = LoggerFactory.getLogger(RtdStreamer.class);

    public static void main(String[] args) throws IOException {

        //TODO: log interesting events

        //TODO: create required topics

        //TODO: add topology diagram to README.md

        final StreamsBuilder builder = new StreamsBuilder();

        // create and load default properties
        final Properties props = new Properties();
        String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        String propsPath = rootPath + "config.properties";
        FileInputStream in = new FileInputStream(propsPath);
        props.load(in);
        in.close();

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        final Serde<BusPosition> serdeBusPosition = new SpecificAvroSerde<>();
        serdeBusPosition.configure((Map) props, false);

        final StoreBuilder<KeyValueStore<String, BusPosition>> busPositionStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("busPositionStore"),
                Serdes.String(),
                Serdes.serdeFrom(serdeBusPosition.serializer(), serdeBusPosition.deserializer()));

        builder.addStateStore(busPositionStore);

        final KStream<String, BusPosition> rtdBusPositionStream = builder.stream("rtd-bus-position");

        final KStream<String, BusPosition> rtdBusPositionStreamEnriched =
                rtdBusPositionStream.transform(new HaversineTransformerSupplier("busPositionStore"), "busPositionStore");

        rtdBusPositionStreamEnriched.to("rtd-bus-position-enriched");

        // run it
        final Topology topology = builder.build();

        LOG.info(topology.describe().toString());

        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();
        streams.cleanUp();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

}
