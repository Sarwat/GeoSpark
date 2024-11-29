/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.sedona.sql.datasources.osmpbf;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.sedona.sql.datasources.osmpbf.build.Fileformat;
import org.apache.sedona.sql.datasources.osmpbf.build.Osmformat;
import org.apache.sedona.sql.datasources.osmpbf.features.DenseNodeParser;
import org.apache.sedona.sql.datasources.osmpbf.features.NodeParser;
import org.apache.sedona.sql.datasources.osmpbf.features.RelationParser;
import org.apache.sedona.sql.datasources.osmpbf.features.WayParser;
import org.apache.sedona.sql.datasources.osmpbf.model.OsmPbfRecord;

public class OsmPbfReader {

  DataInputStream pbfStream;
  int currentValue = 0;
  int endOffset = 0;
  int startOffset = 0;

  boolean hasMoreRows = true;
  ArrayList<OsmPbfRecord> records = new ArrayList<OsmPbfRecord>();
  int recordIndex = 0;

  FSDataInputStream openedFile;

  public OsmPbfReader(OsmPbfOptions options, FileSystem fs) throws IOException {
    openedFile = fs.open(options.inputPath);
    openedFile.skip(options.startOffset);

    this.pbfStream = new DataInputStream(openedFile);

    endOffset = options.endOffset;
    startOffset = options.startOffset;
  }

  public OsmPbfReader() {}

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
    if (this.openedFile.available() == 0) {
      hasMoreRows = false;
      records = new ArrayList<>();
      return;
    }

    int size = pbfStream.readInt();
    if (size == -1) {
      return;
    }

    currentValue += 4;
    currentValue += size;

    byte[] bufferBlobHeader = new byte[size];

    pbfStream.readFully(bufferBlobHeader);

    Fileformat.BlobHeader blobHeader = Fileformat.BlobHeader.parseFrom(bufferBlobHeader);

    byte[] bufferBlob = new byte[blobHeader.getDatasize()];

    currentValue += blobHeader.getDatasize();

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

      switch (type) {
        case DENSE_NODE:
          DenseNodeParser denseNodeParser = new DenseNodeParser(granularity, latOffset, lonOffset);
          records.addAll(denseNodeParser.parse(group.getDense(), stringTable));
          recordIndex = 0;
          break;
        case WAY:
          WayParser wayParser = new WayParser();
          records.addAll(wayParser.parseWays(group.getWaysList(), stringTable));
          recordIndex = 0;
          break;
        case RELATION:
          records.addAll(RelationParser.parseRelations(group.getRelationsList(), stringTable));
          recordIndex = 0;
          break;
        case NODE:
          NodeParser nodeParser = new NodeParser(granularity, latOffset, lonOffset);
          records.addAll(nodeParser.parseNodes(group.getNodesList(), stringTable));
        default:
          recordIndex = 0;
          records = new ArrayList<>();
          break;
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

  public void close() throws IOException {
    pbfStream.close();
    openedFile.close();
  }
}
