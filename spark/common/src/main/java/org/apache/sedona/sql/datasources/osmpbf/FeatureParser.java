package org.apache.sedona.sql.datasources.osmpbf;

import org.apache.sedona.sql.datasources.osmpbf.build.Osmformat;

public class FeatureParser {
    FeatureParser() {

    }

    public static OsmDataType getType(Osmformat.PrimitiveGroup primitiveGroup) {
        if (primitiveGroup.hasDense()) {
            return OsmDataType.DENSE_NODE;
        }

        if (primitiveGroup.getNodesCount() > 0) {
            return OsmDataType.NODE;
        }

        if (primitiveGroup.getWaysCount() > 0) {
            return OsmDataType.WAY;
        }

        if (primitiveGroup.getRelationsCount() > 0) {
            return OsmDataType.RELATION;
        }

        return OsmDataType.UNKNOWN;
    }

}
