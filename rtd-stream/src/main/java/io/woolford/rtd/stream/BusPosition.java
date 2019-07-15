package io.woolford.rtd.stream;

public class BusPosition {

    private String id;
    private long timestamp;
    private double latitude;
    private double longitude;
    private double milesPerHour;
    private String geohash;

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

    public double getMilesPerHour() {
        return milesPerHour;
    }

    public void setMilesPerHour(double milesPerHour) {
        this.milesPerHour = milesPerHour;
    }

    public String getGeohash() {
        return geohash;
    }

    public void setGeohash(String geohash) {
        this.geohash = geohash;
    }

    @Override
    public String toString() {
        return "BusPosition{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", milesPerHour=" + milesPerHour +
                ", geohash='" + geohash + '\'' +
                '}';
    }

}
