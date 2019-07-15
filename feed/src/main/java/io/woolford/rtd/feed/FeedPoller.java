package io.woolford.rtd.feed;


import com.google.transit.realtime.GtfsRealtime;
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
import java.net.URL;
import java.net.URLConnection;
import java.util.Base64;

@Component
@EnableScheduling
public class FeedPoller {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${rtd.username}")
    private String rtdUsername;

    @Value("${rtd.password}")
    private String rtdPassword;

    @Autowired
    KafkaTemplate<String, BusPosition> kafkaTemplate;

    @Scheduled(cron="*/30 * * * * *")
    private void getBusPositions() {
        try {
            logger.info("Getting latest vehicle positions from RTD feed.");
            URL url = new URL("http://www.rtd-denver.com/google_sync/VehiclePosition.pb");
            URLConnection uc = url.openConnection();
            String userpass = rtdUsername + ":" + rtdPassword;
            String basicAuth = "Basic " + new String(Base64.getEncoder().encode(userpass.getBytes()));
            uc.setRequestProperty("Authorization", basicAuth);
            InputStream in = uc.getInputStream();

            GtfsRealtime.FeedMessage feed = GtfsRealtime.FeedMessage.parseFrom(in);
            for (GtfsRealtime.FeedEntity entity : feed.getEntityList()) {

                BusPosition busPosition = new BusPosition();
                busPosition.setId(entity.getVehicle().getVehicle().getId());
                busPosition.setTimestamp(entity.getVehicle().getTimestamp());
                busPosition.setLatitude(entity.getVehicle().getPosition().getLatitude());
                busPosition.setLongitude(entity.getVehicle().getPosition().getLongitude());

                Message<BusPosition> message = MessageBuilder
                        .withPayload(busPosition)
                        .setHeader(KafkaHeaders.TOPIC, "rtd-bus-position")
                        .setHeader(KafkaHeaders.MESSAGE_KEY, entity.getVehicle().getVehicle().getId())
                        .build();

                kafkaTemplate.send(message);

            }

            logger.info("Published latest vehicle positions to Kafka.");

        } catch (Exception e) {
            logger.error(e.getMessage());
        }

    }


}
