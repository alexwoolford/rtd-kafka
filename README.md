# RTD Bus Feed

The __rtd-feed__ module publishes the latest Denver RTD vehicle positions, every 30 seconds, to the `rtd-bus-position` Kafka topic.

The messages are serialized as Avro and look like this where deserialized:

    {
      "id": "985CC5EC1D3FC176E053DD4D1FAC4E39",
      "timestamp": 1574904869,
      "location": {
        "lon": -104.98970794677734,
        "lat": 39.697723388671875
      },
      "milesPerHour": 0
    }

The message key is the vehicle ID, which means that telemetry for a vehicle is always sent to the same Kafka partition.

The __rtd-stream__ module is a Kafka Streams job that enriches the feed data with the speed, based on the distance traveled between the last known position. The enriched records are written to the `rtd-bus-postition-enriched` topic. Here's a sample record:

    {
      "id": "985CC5EC1D3FC176E053DD4D1FAC4E39",
      "timestamp": 1574904869,
      "location": {
        "lon": -104.98970794677734,
        "lat": 39.697723388671875
      },
      "milesPerHour": 34.18513011763098
    }

Note that the milesPerHour has been updated.

Once the enriched data is in Kafka, it can be written to Elastic using Kafka Connect. In order to recognise the timestamp and location using the proper datatypes, we create a dynamic template in Elastic:

    {
      "dynamic_templates": [
        {
          "dates": {
            "mapping": {
              "format": "epoch_millis",
              "type": "date"
            },
            "match": "*timestamp"
          }
        },
        {
          "locations": {
            "mapping": {
              "type": "geo_point"
            },
            "match": "*location"
          }
        }
      ]
    }

The following `curl` command was then run to create the connector:

    curl -X PUT -H "Content-Type: application/json" --data '
    {
        "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
        "type.name": "_doc",
        "topics": "rtd-bus-position-enriched",
        "name": "rtd-elastic",
        "connection.url": "http://elastic.woolford.io:9200",
        "key.ignore": "true",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "schema.ignore": "true"
    }' http://cp01.woolford.io:8083/connectors/rtd-elastic/config

Here's a diagram of the solution:

![topology](img/rtd-arch.png)

Here's the topology, generated by the [Kafka Streams Topology Visualizer](https://zz85.github.io/kafka-streams-viz/) tool. Note the `busPositionStore`, which is a key/value store cache of the previous bus positions. This positions are written back to a Kafka changelog topic so the state can be maintained if the streaming job was to fail:

![topology-from-visualizer](img/topology.png)

Here's a screenshot from the Kibana dashboard:

![Kibana screenshot](img/rtd-elastic.png)

See the feed in action:

[![Real-time vehicle telemetry analysis with Kafka Streams](https://img.youtube.com/vi/yIFOCYy7Wmc/0.jpg)](https://www.youtube.com/watch?v=yIFOCYy7Wmc)


## Build

To create an executable jar for the RTD feed:

    cd rtd-feed
    mvn generate-sources
    mvn package
    
This creates an executable jar: `target/feed-0.1-spring-boot.jar`.

To create an executable jar for the Kafka Streams job that calculates the speed:

    cd rtd-stream
    mvn generate-sources
    mvn assembly:single

This creates an executable jar: `target/rtd-stream-1.0.jar`
