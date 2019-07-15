# RTD Bus Feed

The __rtd-feed__ module publishes the latest Denver RTD vehicle positions, every 30 seconds, to the `rtd-bus-position` Kafka topic.

The messages are serialized as JSON and look like this:

    {"id":"9406","timestamp":1563149624,"latitude":39.737300872802734,"longitude":-104.82324981689453}

The message key is the vehicle ID, which means that telemetry for a vehicle is always sent to the same Kafka partition.


The __rtd-stream__ module is a Kafka Streams job that enriches the feed data with the speed, based on the distance traveled between the last known position, and adds a geohash that can be used to group geographically close records together. Here's a sample record:

    {"id":"9333","timestamp":1563165646,"latitude":39.770381927490234,"longitude":-104.98400115966797,"milesPerHour":0.0,"geohash":"9xj65pb"}

