package io.woolford.rtd.stream;

import java.util.Properties;

public class RtdStreamProperties {

    public Properties getProperties() {

        // create and load default properties
        final Properties props = new Properties();

        props.setProperty("application.id", "rtd-stream");


        // Kafka properties
        props.setProperty("bootstrap.servers", System.getenv("BOOTSTRAP_SERVERS"));

        // option properties for a secure cluster
        if (System.getenv("SECURITY_PROTOCOL") != null) {
            props.setProperty("security.protocol", System.getenv("SECURITY_PROTOCOL"));
        }

        if (System.getenv("SASL_JAAS_CONFIG") != null) {
            props.setProperty("sasl.jaas.config", System.getenv("SASL_JAAS_CONFIG"));
        }

        if (System.getenv("SASL_ENDPOINT_IDENTIFICATION_ALGORITHM") != null) {
            props.setProperty("sasl.endpoint.identification.algorithm", System.getenv("SASL_ENDPOINT_IDENTIFICATION_ALGORITHM"));
        }

        if (System.getenv("SASL_MECHANISM") != null) {
            props.setProperty("sasl.mechanism", System.getenv("SASL_MECHANISM"));
        }


        // Schema Registry properties
        props.setProperty("schema.registry.url", System.getenv("SCHEMA_REGISTRY_URL"));

        // optional properties for secure schema registry
        if (System.getenv("BASIC_AUTH_CREDENTIALS_SOURCE") != null) {
            props.setProperty("basic.auth.credentials.source", System.getenv("BASIC_AUTH_CREDENTIALS_SOURCE"));
        }

        if (System.getenv("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO") != null) {
            props.setProperty("schema.registry.basic.auth.user.info", System.getenv("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO"));
        }

        return props;

    }

}
