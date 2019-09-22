package io.woolford.rtd.feed;

import com.google.transit.realtime.GtfsRealtime;

public class BusPosition {

    private String id;
    private long timestamp;
    private double latitude;
    private double longitude;

    static BusPosition build(String id, long timestamp, GtfsRealtime.Position position) {
        return new BusPosition(id, timestamp, position.getLatitude(), position.getLongitude());
    }

    private BusPosition(String id, long timestamp, double latitude, double longitude) {
        this.id = id;
        this.timestamp = timestamp;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public BusPosition() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    @Override
    public String toString() {
        return "BusPosition{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                '}';
    }

}
