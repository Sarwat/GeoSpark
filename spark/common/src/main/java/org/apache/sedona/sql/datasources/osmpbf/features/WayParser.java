package org.apache.sedona.sql.datasources.osmpbf.features;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.sedona.sql.datasources.osmpbf.build.Osmformat;
import org.apache.sedona.sql.datasources.osmpbf.model.OsmPbfRecord;
import org.apache.sedona.sql.datasources.osmpbf.model.OsmWay;

public class WayParser {
    public ArrayList<OsmPbfRecord> parse(List<Osmformat.Way> ways, Osmformat.StringTable stringTable) {
        ArrayList<OsmPbfRecord> records = new ArrayList<>();
        if (ways == null || ways.isEmpty()) {
            return records;
        }

        for (Osmformat.Way way : ways) {
            List<Long> refs = new ArrayList<Long>();

            if (way.getRefsCount() != 0) {
                long firstRef = way.getRefs(0);
                refs.add(firstRef);

                for (int i = 1; i < way.getRefsCount(); i++) {
                    refs.add(way.getRefs(i) + firstRef);
                }
            }

            int number = way.getKeysCount();

            HashMap<String, String> tags = new HashMap<>();

            for (int i = 0; i < number; i++) {
                int key = way.getKeys(i);
                int value = way.getVals(i);

                String keyString = stringTable.getS(key).toStringUtf8();
                String valueString = stringTable.getS(value).toStringUtf8();

                tags.put(keyString, valueString);
            }

            OsmWay wayObj = new OsmWay(way.getId(), refs, tags);

            records.add(new OsmPbfRecord(wayObj));
        }

        return records;
    }
}
