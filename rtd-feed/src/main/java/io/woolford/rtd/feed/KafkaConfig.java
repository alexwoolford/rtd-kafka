package io.woolford.rtd.feed;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");
        return props;
    }

    @Bean
    public ProducerFactory<String, io.woolford.rtd.BusPosition> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String, io.woolford.rtd.BusPosition> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

}
