package io.woolford.rtd.stream;


import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.woolford.rtd.BusPosition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;

import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;


public class RtdStreamer {

    private static Logger logger = LoggerFactory.getLogger(RtdStreamer.class);

    private static HashMap<String, BusPosition> previousBusPositionMap = new HashMap<String, BusPosition>();

    public static void main(String[] args) throws IOException {

        // create and load default properties
        Properties props = new Properties();
        String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
        String propsPath = rootPath + "config.properties";
        FileInputStream in = new FileInputStream(propsPath);
        props.load(in);
        in.close();

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, BusPosition> rtdBusPositionStream = builder.stream("rtd-bus-position");

        rtdBusPositionStream.mapValues(value -> {
            value = RtdStreamer.enrichBusPosition(value);
            return value;
        }).to("rtd-bus-position-enriched");

        // run it
        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }

    private static BusPosition enrichBusPosition(BusPosition busPosition) {

        // current bus ID
        String busId = busPosition.getId().toString();

        // if there is a previous location for that bus ID, calculate the speed based on its previous position/timestamp.
        if (previousBusPositionMap.containsKey(busId)){
            BusPosition previousBusPosition = previousBusPositionMap.get(busId);

            // calculate distance and time between last two measurements
            HaversineDistanceCalculator haversineDistanceCalculator = new HaversineDistanceCalculator();
            double distance = haversineDistanceCalculator.calculateDistance(
                    previousBusPosition.getLocation().getLat(),
                    previousBusPosition.getLocation().getLon(),
                    busPosition.getLocation().getLat(),
                    busPosition.getLocation().getLon()); // distance is in kilometers

            long timedelta = busPosition.getTimestamp() - previousBusPosition.getTimestamp(); // time delta is in seconds
            double milesPerHour = calculateMilesPerHour(distance, timedelta);

            busPosition.setMilesPerHour(milesPerHour);

        }

        previousBusPositionMap.put(busId, busPosition);

        return busPosition;

    }

    private static double calculateMilesPerHour(double meters, long seconds) {
        if (seconds == 0){
            return 0;
        } else {
            double metersPerSecond = meters * 1000 / seconds;
            return metersPerSecond * 2.2369;
        }
    }

}
