package io.woolford.rtd.feed;

import com.google.transit.realtime.GtfsRealtime;
import io.woolford.rtd.BusPositionFeed;
import io.woolford.rtd.Location;
import kong.unirest.Unirest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.InputStream;

@Component
@EnableScheduling
public class FeedPoller {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${rtd.username}")
    private String rtdUsername;

    @Value("${rtd.password}")
    private String rtdPassword;

    @Autowired
    KafkaTemplate<String, BusPositionFeed> kafkaTemplate;

    @Scheduled(cron = "*/30 * * * * *")
    private void getBusPositions() {
        try {
            // Docs for stream source available here: http://www.rtd-denver.com/gtfs-developer-guide.shtml#samples
            logger.info("Getting latest vehicle positions from RTD feed.");

            // get latest vehicle positions
            String userName = "RTDgtfsRT";
            String password = "realT!m3Feed";
            String url = "http://www.rtd-denver.com/google_sync/VehiclePosition.pb";

            Unirest.get(url)
                    .basicAuth(userName, password)
                    .thenConsume(rawResponse -> {
                        try {
                            InputStream stream = rawResponse.getContent();

                            GtfsRealtime.FeedMessage feed = GtfsRealtime.FeedMessage.parseFrom(stream);

                            for (GtfsRealtime.FeedEntity entity : feed.getEntityList()) {
                                GtfsRealtime.VehiclePosition vehiclePosition = entity.getVehicle();

                                Location location = new Location();
                                location.setLat(vehiclePosition.getPosition().getLatitude());
                                location.setLon(vehiclePosition.getPosition().getLongitude());

                                BusPositionFeed busPosition = new BusPositionFeed();

                                busPosition.setId(vehiclePosition.getVehicle().getId());
                                busPosition.setTimestamp(vehiclePosition.getTimestamp() * 1000); // convert seconds to Avro-friendly millis
                                busPosition.setLocation(location);
                                busPosition.setBearing(vehiclePosition.getPosition().getBearing());

                                Message<BusPositionFeed> message = MessageBuilder
                                        .withPayload(busPosition)
                                        .setHeader(KafkaHeaders.TOPIC, "rtd-bus-position")
                                        .setHeader(KafkaHeaders.MESSAGE_KEY, entity.getVehicle().getVehicle().getId())
                                        .build();

                                // publish to `rtd-bus-position` Kafka topic
                                kafkaTemplate.send(message);
                            }
                        } catch (Exception e) {
                            throw new Error(e);
                        }
                    });

            logger.info("Published latest vehicle positions to Kafka.");
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}
