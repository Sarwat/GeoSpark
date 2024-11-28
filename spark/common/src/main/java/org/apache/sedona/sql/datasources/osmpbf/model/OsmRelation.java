package org.apache.sedona.sql.datasources.osmpbf.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class OsmRelation {
    private HashMap<String, String> tags;
    private List<String> types;
    private long id;
    private List<Long> memberIds;

    public OsmRelation(long id, List<Long> memberIds, HashMap<String, String> tags, List<String> types) {
        this.id = id;
        this.memberIds = memberIds;
        this.tags = tags;
        this.types = types;
    }

    public long getId() {
        return id;
    }

    public List<Long> getMemberIds() {
        return memberIds;
    }

    public List<String> getTypes() {
        return types;
    }

    public HashMap<String, String> getTags() {
        return tags;
    }
}
