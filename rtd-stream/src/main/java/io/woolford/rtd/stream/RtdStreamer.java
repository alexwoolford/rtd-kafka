package io.woolford.rtd.stream;

import ch.hsr.geohash.GeoHash;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.Properties;


public class RtdStreamer {

    private static Logger logger = LoggerFactory.getLogger(RtdStreamer.class);

    static RocksDB db = getRocksDB();

    public static void main(String[] args) {

        // set props for Kafka Steams app (see KafkaConstants)
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaConstants.APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.KAFKA_BROKERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.PRODUCER_PREFIX + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");
        props.put(StreamsConfig.CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> rtdBusPositionStream = builder.stream("rtd-bus-position");

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

    private static RocksDB getRocksDB() {
        RocksDB.loadLibrary();

        final Options options = new Options().setCreateIfMissing(true);

        try {
            final RocksDB db = RocksDB.open(options, "/tmp/" + KafkaConstants.APPLICATION_ID);
            return db;
        } catch (RocksDBException e) {
            logger.error(e.getMessage());
            return null;
        }
    }

    private static String enrichBusPosition(String value) {

        // map record to BusPosition POJO
        ObjectMapper mapper = new ObjectMapper();
        BusPosition busPosition = new BusPosition();
        try {
            busPosition = mapper.readValue(value, BusPosition.class);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

        // get previous bus position from RocksDB
        String busId = busPosition.getId();
        BusPosition previousBusPosition = new BusPosition();
        String previousBusPositionString;
        try {
            byte[] busIdBytes = db.get(busId.getBytes());
            if (busIdBytes != null){
                previousBusPositionString = new String(busIdBytes);
                previousBusPosition = mapper.readValue(previousBusPositionString, BusPosition.class);
            }
        } catch (RocksDBException | IOException e) {
            logger.error(e.getMessage());
        }

        // persist current bus position to RocksDB
        try {
            db.put(busId.getBytes(), value.getBytes());
        } catch (RocksDBException e) {
            logger.error(e.getMessage());
        }

        // calculate distance and time between last two measurements
        HaversineDistanceCalculator haversineDistanceCalculator = new HaversineDistanceCalculator();
        double distance = haversineDistanceCalculator.calculateDistance(
                previousBusPosition.getLatitude(),
                previousBusPosition.getLongitude(),
                busPosition.getLatitude(),
                busPosition.getLongitude()); // distance is in kilometers

        long timedelta = busPosition.getTimestamp() - previousBusPosition.getTimestamp(); // time delta is in seconds

        // calculate and set miles per hour
        double milesPerHour = calculateMilesPerHour(distance, timedelta);
        busPosition.setMilesPerHour(milesPerHour);

        // Geohashed to tiles approx. 150m in size: https://gis.stackexchange.com/questions/115280/what-is-the-precision-of-a-geohash
        GeoHash geohash = GeoHash.withCharacterPrecision(busPosition.getLatitude(), busPosition.getLongitude(),7);
        String geohashString = geohash.toBase32();
        busPosition.setGeohash(geohashString);

        // return enriched JSON
        try {
            return mapper.writeValueAsString(busPosition);
        } catch (JsonProcessingException e) {
            logger.error(e.getMessage());
        }

        return null;

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
