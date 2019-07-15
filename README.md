# RTD Bus Feed

Publishes the latest Denver RTD vehicle positions, every 30 seconds, to the `rtd-bus-position` Kafka topic.

The messages are serialized as JSON and look like this:
    {"id":"9406","timestamp":1563149624,"latitude":39.737300872802734,"longitude":-104.82324981689453}

The message key is the vehicle ID, which means that telemetry for a vehicle is always sent to the same Kafka partition.
