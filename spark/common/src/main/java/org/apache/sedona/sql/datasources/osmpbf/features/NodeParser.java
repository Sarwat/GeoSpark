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
package org.apache.sedona.sql.datasources.osmpbf.features;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.sedona.sql.datasources.osmpbf.build.Osmformat;
import org.apache.sedona.sql.datasources.osmpbf.model.OsmNode;
import org.apache.sedona.sql.datasources.osmpbf.model.OsmPbfRecord;

public class NodeParser {

  private final long granularity;
  private final long latOffset;
  private final long lonOffset;

  public NodeParser(long granularity, long latOffset, long lonOffset) {
    this.granularity = granularity;
    this.latOffset = latOffset;
    this.lonOffset = lonOffset;
  }

  public List<OsmPbfRecord> parseNodes(
      List<Osmformat.Node> nodes, Osmformat.StringTable stringTable) {
    if (nodes == null) {
      return null;
    }

    return nodes.stream().map(node -> parseNode(node, stringTable)).collect(Collectors.toList());
  }

  public OsmPbfRecord parseNode(Osmformat.Node node, Osmformat.StringTable stringTable) {
    float lat = (float) (.000000001 * (latOffset + (node.getLat() * granularity)));
    float lon = (float) (.000000001 * (lonOffset + (node.getLon() * granularity)));

    OsmNode osmNode =
        new OsmNode(
            node.getId(),
            lon,
            lat,
            TagsResolver.resolveTags(
                node.getKeysCount(), node::getKeys, node::getVals, stringTable));

    return new OsmPbfRecord(osmNode);
  }
}
