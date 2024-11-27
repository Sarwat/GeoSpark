package org.apache.sedona.sql.datasources.osmpbf.features;


import java.util.ArrayList;
import java.util.HashMap;
import org.apache.sedona.sql.datasources.osmpbf.build.Osmformat;
import org.apache.sedona.sql.datasources.osmpbf.model.OsmNode;
import org.apache.sedona.sql.datasources.osmpbf.model.OsmPbfRecord;

public class DenseNodeParser {
    private static long granularity;
    private static long latOffset;
    private static long lonOffset;

    public DenseNodeParser(long granularity, long latOffset, long lonOffset) {
        this.granularity = granularity;
        this.latOffset = latOffset;
        this.lonOffset = lonOffset;
    }

    public ArrayList<OsmPbfRecord> parse(Osmformat.DenseNodes nodes, Osmformat.StringTable stringTable) {
        if (nodes == null) {
            return null;
        }

        long firstId = nodes.getId(0);
        long firstLat = nodes.getLat(0);
        long firstLon = nodes.getLon(0);

        float lat = (float) (.000000001 * (latOffset + (firstLat * granularity)));
        float lon = (float) (.000000001 * (lonOffset + (firstLon * granularity)));

        ArrayList<OsmPbfRecord> parsedNodes = new ArrayList<OsmPbfRecord>();
        Integer keyIndex = 0;

        TagsHelper tagsHelper = parseTags(nodes, keyIndex, stringTable);
        keyIndex = tagsHelper.keyIndex;

        parsedNodes.add(new OsmPbfRecord(new OsmNode(firstId, lat, lon, tagsHelper.tags)));


        for (int i = 1; i < nodes.getIdCount(); i++) {
            tagsHelper = parseTags(nodes, keyIndex, stringTable);

            keyIndex = tagsHelper.keyIndex;

            long id = nodes.getId(i);
            long idMoved = id + firstId;

            long latitude = nodes.getLat(i) + firstLat;
            long longitude = nodes.getLon(i) + firstLon;

            lat = (float) (.000000001 * (latOffset + (latitude * granularity)));
            lon = (float) (.000000001 * (lonOffset + (longitude * granularity)));

            parsedNodes.add(
                    new OsmPbfRecord(
                            new OsmNode(idMoved, lat, lon, tagsHelper.tags)
                    )
            );

            firstId = idMoved;
            firstLat = latitude;
            firstLon = longitude;
        }

        return parsedNodes;
    }

    class TagsHelper {
        HashMap<String, String> tags;
        Integer keyIndex;


        public TagsHelper(HashMap<String, String> tags, Integer keyIndex) {
            this.tags = tags;
            this.keyIndex = keyIndex;
        }
    }


    TagsHelper parseTags(Osmformat.DenseNodes nodes, Integer keyIndex, Osmformat.StringTable stringTable) {
        if (nodes.getKeysVals(keyIndex) != 0) {
            HashMap<String, String> tags = new HashMap<>();

            while (nodes.getKeysVals(keyIndex) != 0) {
                Integer key = nodes.getKeysVals(keyIndex);
                Integer value = nodes.getKeysVals(keyIndex + 1);

                String keyString = stringTable.getS(key).toStringUtf8();
                String valueString = stringTable.getS(value).toStringUtf8();

                tags.put(keyString, valueString);

                keyIndex = keyIndex + 2;
            }

            return new TagsHelper(tags, keyIndex);
        }

        return new TagsHelper(new HashMap<>(), keyIndex + 1);
    }
}

