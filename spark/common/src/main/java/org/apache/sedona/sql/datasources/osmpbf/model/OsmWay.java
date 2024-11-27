package org.apache.sedona.sql.datasources.osmpbf.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class OsmWay {
    private long id;
    private List<Long> refs;

    private HashMap<String, String> tags;

    public OsmWay(long id, List<Long> refs, HashMap<String, String> tags) {
        this.id = id;
        this.refs = refs;
        this.tags = tags;
    }

    public long getId() {
        return id;
    }

    public List<Long> getRefs() {
        return refs;
    }

    public void setId(long id) {
        this.id = id;
    }

    public void setRefs(ArrayList<Long> refs) {
        this.refs = refs;
    }

    public String toString() {
        return "Way [id=" + id + ", refs=" + refs + "]";
    }

    public HashMap<String, String> getTags() {
        return tags;
    }
}
