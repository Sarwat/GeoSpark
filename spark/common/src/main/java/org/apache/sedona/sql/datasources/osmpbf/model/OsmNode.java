package org.apache.sedona.sql.datasources.osmpbf.model;

import java.util.HashMap;

public class OsmNode {
    private long id;
    private double latitude;
    private double longitude;
    private HashMap<String, String> tags;

    public OsmNode(long id, double latitude, double longitude, HashMap<String, String> tags) {
        this.id = id;
        this.latitude = latitude;
        this.longitude = longitude;
        this.tags = tags;
    }

    public long getId() {
        return id;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public String toString() {
        return "Node [id=" + id + ", latitude=" + latitude + ", longitude=" + longitude + ", tags=" + tags + "]";
    }

    public HashMap<String, String> getTags() {
        return tags;
    }
}

