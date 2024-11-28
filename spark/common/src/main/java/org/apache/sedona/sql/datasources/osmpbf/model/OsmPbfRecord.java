package org.apache.sedona.sql.datasources.osmpbf.model;

public class OsmPbfRecord {
    OsmNode node;
    OsmWay way;
    OsmRelation relation;

    public OsmPbfRecord(OsmNode node) {
        this.node = node;
    }

    public OsmPbfRecord(OsmWay way) {
        this.way = way;
    }

    public OsmPbfRecord(OsmRelation relation) {
        this.relation = relation;
    }


    public OsmPbfRecord() {
    }

    public OsmNode getNode() {
        return node;
    }

    public OsmWay getWay() {
        return way;
    }

    public OsmRelation getRelation() {
        return relation;
    }
}
