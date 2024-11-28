package org.apache.sedona.sql.datasources.osmpbf;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import org.apache.sedona.sql.datasources.osmpbf.build.Fileformat;
import org.apache.sedona.sql.datasources.osmpbf.build.Osmformat;
import org.apache.sedona.sql.datasources.osmpbf.features.DenseNodeParser;
import org.apache.sedona.sql.datasources.osmpbf.features.RelationParser;
import org.apache.sedona.sql.datasources.osmpbf.features.WayParser;
import org.apache.sedona.sql.datasources.osmpbf.model.OsmPbfRecord;

public class OsmPbfReader {

    FileInputStream stream;
    DataInputStream pbfStream;

    int currentValue = 0;
    int endOffset = 0;
    int startOffset = 0;


    boolean hasMoreRows = true;
    ArrayList<OsmPbfRecord> records = new ArrayList<OsmPbfRecord>();
    int recordIndex = 0;

    public OsmPbfReader(OsmPbfOptions options) throws IOException {
        long currentTime = System.currentTimeMillis();

        RandomAccessFile randomAccessFile = new RandomAccessFile(new File(options.inputPath), "r");
        randomAccessFile.seek(options.startOffset);

        this.pbfStream = new DataInputStream(
                new FileInputStream(randomAccessFile.getFD())
        );
        this.stream = new FileInputStream(randomAccessFile.getFD());

        System.out.println("Time to read file: " + (System.currentTimeMillis() - currentTime) + "for file offset " + options.startOffset + " - " + options.endOffset);

        endOffset = options.endOffset;
        startOffset = options.startOffset;
    }

    public OsmPbfReader() {
    }

    public boolean next() throws IOException {
        if (recordIndex < records.size()) {
            return true;
        }

        return currentValue + startOffset < endOffset;
    }

    public OsmPbfRecord get() throws DataFormatException, IOException {
        if (recordIndex >= records.size()) {
            getNextRow();
        }

        if (records.isEmpty()) {
            return new OsmPbfRecord();
        }

        OsmPbfRecord record = records.get(recordIndex);

        recordIndex++;
        return record;
    }

    public void getNextRow() throws IOException, DataFormatException {
        if (this.stream.available() == 0) {
            hasMoreRows = false;
            records = new ArrayList<>();
            return;
        }

        int size = pbfStream.readInt();
        if (size == -1) {
            return;
        }

        currentValue+=4;
        currentValue+=size;

        byte[] bufferBlobHeader = new byte[size];

        pbfStream.readFully(bufferBlobHeader);

        Fileformat.BlobHeader blobHeader = Fileformat.BlobHeader.parseFrom(bufferBlobHeader);

        byte[] bufferBlob = new byte[blobHeader.getDatasize()];

        currentValue+=blobHeader.getDatasize();

        pbfStream.readFully(bufferBlob);

        Fileformat.Blob blob = Fileformat.Blob.parseFrom(bufferBlob);

        Osmformat.PrimitiveBlock pb = Osmformat.PrimitiveBlock.parseFrom(dataInputStreamBlob(blob));

        long latOffset = pb.getLatOffset();
        long lonOffset = pb.getLonOffset();
        int granularity = pb.getGranularity();
        Osmformat.StringTable stringTable = pb.getStringtable();

        records = new ArrayList<>();

        for (Osmformat.PrimitiveGroup group : pb.getPrimitivegroupList()) {
            OsmDataType type = FeatureParser.getType(group);

            if (type == OsmDataType.DENSE_NODE) {
                DenseNodeParser denseNodeParser = new DenseNodeParser(granularity, latOffset, lonOffset);
                records.addAll(denseNodeParser.parse(group.getDense(), stringTable));
                recordIndex = 0;
            } else if (type == OsmDataType.WAY) {
                WayParser wayParser = new WayParser();
                records.addAll(wayParser.parse(group.getWaysList(), stringTable));
                recordIndex = 0;
            } else if (type == OsmDataType.RELATION) {
                records.addAll(RelationParser.parse(group.getRelationsList(), stringTable));
                recordIndex = 0;
            }
             else {
                recordIndex = 0;
                records = new ArrayList<>();
            }
        }
    }

    public DataInputStream dataInputStreamBlob(Fileformat.Blob blob) throws DataFormatException {
        if (!blob.getRaw().isEmpty()) {
            return new DataInputStream(new ByteArrayInputStream(blob.getRaw().toByteArray()));
        }

        if (!blob.getZlibData().isEmpty()) {
            Inflater inflater = new Inflater();
            inflater.setInput(blob.getZlibData().toByteArray());
            byte[] decompressedData = new byte[blob.getRawSize()];
            inflater.inflate(decompressedData);
            inflater.end();
            return new DataInputStream(new ByteArrayInputStream(decompressedData));
        }

        throw new RuntimeException("Data not found even compressed.");
    }
}
