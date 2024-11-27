package org.apache.sedona.sql.datasources.osmpbf;

public class OsmPbfOptions {
    String inputPath;
    OsmDataType dataType;
    Integer startOffset;
    Integer endOffset;

    public OsmPbfOptions(
            String inputPath,
            OsmDataType dataType,
            Integer startOffset,
            Integer endOffset
    ) {
        this.inputPath = inputPath;
        this.dataType = dataType;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }
}
