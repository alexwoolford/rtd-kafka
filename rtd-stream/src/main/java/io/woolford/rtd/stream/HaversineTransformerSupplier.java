package io.woolford.rtd.stream;

import io.woolford.rtd.BusPosition;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;

import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class HaversineTransformerSupplier implements TransformerSupplier<String, BusPosition, KeyValue<String, BusPosition>> {

    private static final Logger LOG = LoggerFactory.getLogger(HaversineTransformerSupplier.class);

    public String busPositionStoreName;

    HaversineTransformerSupplier(String busPositionStoreName) {
        this.busPositionStoreName = busPositionStoreName;
    }

    @Override
    public Transformer<String, BusPosition, KeyValue<String, BusPosition>> get() {
        return new Transformer<String, BusPosition, KeyValue<String, BusPosition>>() {

            private KeyValueStore<String, BusPosition> busPositionStore;

            @SuppressWarnings("unchecked")
            @Override
            public void init(final ProcessorContext context) {
                busPositionStore = (KeyValueStore<String, BusPosition>) context.getStateStore(busPositionStoreName);
            }

            @Override
            public KeyValue<String, BusPosition> transform(final String dummy, final BusPosition busPosition) {

                BusPosition previousBusPosition = busPositionStore.get(String.valueOf(busPosition.getId()));

                // if there is a previous location for that bus ID, calculate the speed based on its previous position/timestamp.
                if (previousBusPosition != null){
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

                busPositionStore.put(String.valueOf(busPosition.getId()), busPosition);

                return new KeyValue<>(String.valueOf(busPosition.getId()), busPosition);

            }

            @Override
            public void close() {
            }

        };
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

