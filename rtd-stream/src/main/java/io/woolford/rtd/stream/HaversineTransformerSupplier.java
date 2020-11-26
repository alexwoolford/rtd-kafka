package io.woolford.rtd.stream;

import io.woolford.rtd.BusPositionFeed;
import io.woolford.rtd.BusPositionSpeed;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;

import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class HaversineTransformerSupplier implements TransformerSupplier<String, BusPositionFeed, KeyValue<String, BusPositionSpeed>> {

    private static final Logger LOG = LoggerFactory.getLogger(HaversineTransformerSupplier.class);

    public String busPositionStoreName;

    HaversineTransformerSupplier(String busPositionStoreName) {
        this.busPositionStoreName = busPositionStoreName;
    }

    @Override
    public Transformer<String, BusPositionFeed, KeyValue<String, BusPositionSpeed>> get() {
        return new Transformer<String, BusPositionFeed, KeyValue<String, BusPositionSpeed>>() {

            private KeyValueStore<String, BusPositionFeed> busPositionStore;

            @SuppressWarnings("unchecked")
            @Override
            public void init(final ProcessorContext context) {
                busPositionStore = (KeyValueStore<String, BusPositionFeed>) context.getStateStore(busPositionStoreName);
            }

            @Override
            public KeyValue<String, BusPositionSpeed> transform(final String dummy, final BusPositionFeed busPosition) {

                BusPositionFeed previousBusPosition = busPositionStore.get(String.valueOf(busPosition.getId()));

                BusPositionSpeed busPositionSpeed = new BusPositionSpeed();
                busPositionSpeed.setId(busPosition.getId());
                busPositionSpeed.setTimestamp(busPosition.getTimestamp());
                busPositionSpeed.setLocation(busPosition.getLocation());
                busPositionSpeed.setBearing(busPosition.getBearing());
                busPositionSpeed.setMilesPerHour(0);

                // if there is a previous location for that bus ID, calculate the speed based on its previous position/timestamp.
                if (previousBusPosition != null){

                    // calculate distance and time between last two measurements
                    HaversineDistanceCalculator haversineDistanceCalculator = new HaversineDistanceCalculator();
                    double distanceKm = haversineDistanceCalculator.calculateDistance(
                            previousBusPosition.getLocation().getLat(),
                            previousBusPosition.getLocation().getLon(),
                            busPosition.getLocation().getLat(),
                            busPosition.getLocation().getLon()); // distance is in kilometers

                    long timeDeltaMillis = busPosition.getTimestamp() - previousBusPosition.getTimestamp();
                    double milesPerHour = calculateMilesPerHour(distanceKm * 1000, timeDeltaMillis / 1000);
                    busPositionSpeed.setMilesPerHour(milesPerHour);

                }

                busPositionStore.put(String.valueOf(busPosition.getId()), busPosition);

                return new KeyValue<>(String.valueOf(busPosition.getId()), busPositionSpeed);

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
            double metersPerSecond = meters / seconds;
            return metersPerSecond * 2.2369;
        }
    }

}

