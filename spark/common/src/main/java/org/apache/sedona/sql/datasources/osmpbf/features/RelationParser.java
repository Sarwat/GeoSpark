package org.apache.sedona.sql.datasources.osmpbf.features;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.sedona.sql.datasources.osmpbf.build.Osmformat;
import org.apache.sedona.sql.datasources.osmpbf.model.OsmPbfRecord;
import org.apache.sedona.sql.datasources.osmpbf.model.OsmRelation;
import org.apache.sedona.sql.datasources.osmpbf.model.RelationType;

public class RelationParser {
    public static ArrayList<OsmPbfRecord> parse(List<Osmformat.Relation> relations, Osmformat.StringTable stringTable) {
        ArrayList<OsmPbfRecord> records = new ArrayList<>();
        if (relations == null || relations.isEmpty()) {
            return records;
        }

        for (Osmformat.Relation relation : relations) {
            List<Long> memberIds = resolveMemberIds(relation);
            List<String> memberTypes = resolveTypes(relation);

            HashMap<String, String> tags = resolveTags(relation, stringTable);

            OsmRelation relationObj = new OsmRelation(
                    relation.getId(),
                    memberIds,
                    tags,
                    memberTypes
            );

            records.add(new OsmPbfRecord(relationObj));
        }

        return records;
    }

    public static List<Long> resolveMemberIds(Osmformat.Relation relation) {
        List<Long> memberIds = new ArrayList<>();

        if (relation.getMemidsCount() != 0) {
            long firstId = relation.getMemids(0);
            memberIds.add(firstId);

            for (int i = 1; i < relation.getMemidsCount(); i++) {
                memberIds.add(relation.getMemids(i) + firstId);
            }
        }

        return memberIds;
    }

    public static List<String> resolveTypes(Osmformat.Relation relation) {
        List<String> types = new ArrayList<>();

        for (int i = 0; i < relation.getTypesCount(); i++) {
            Osmformat.Relation.MemberType memberType = relation.getTypes(i);
            types.add(RelationType.fromValue(memberType.getNumber()));
        }

        return types;
    }

    public static HashMap<String, String> resolveTags(Osmformat.Relation relation, Osmformat.StringTable stringTable) {
        HashMap<String, String> tags = new HashMap<>();

        for (int i = 0; i < relation.getKeysCount(); i++) {
            int key = relation.getKeys(i);
            int value = relation.getVals(i);

            String keyString = stringTable.getS(key).toStringUtf8();
            String valueString = stringTable.getS(value).toStringUtf8();
            tags.put(keyString, valueString);
        }

        return tags;
    }
}
