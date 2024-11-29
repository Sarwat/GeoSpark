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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.sedona.sql.datasources.osmpbf.build.Osmformat;
import org.apache.sedona.sql.datasources.osmpbf.model.OsmPbfRecord;
import org.apache.sedona.sql.datasources.osmpbf.model.OsmWay;

public class WayParser {
  public List<OsmPbfRecord> parseWays(List<Osmformat.Way> ways, Osmformat.StringTable stringTable) {
    if (ways == null || ways.isEmpty()) {
      return new ArrayList<>();
    }

    return ways.stream().map(way -> parse(way, stringTable)).collect(Collectors.toList());
  }

  public OsmPbfRecord parse(Osmformat.Way way, Osmformat.StringTable stringTable) {
    if (way == null) {
      return null;
    }

    List<Long> refs = new ArrayList<Long>();

    if (way.getRefsCount() != 0) {
      long firstRef = way.getRefs(0);
      refs.add(firstRef);

      for (int i = 1; i < way.getRefsCount(); i++) {
        refs.add(way.getRefs(i) + firstRef);
      }
    }

    HashMap<String, String> tags =
        TagsResolver.resolveTags(way.getKeysCount(), way::getKeys, way::getVals, stringTable);

    OsmWay wayObj = new OsmWay(way.getId(), refs, tags);

    return new OsmPbfRecord(wayObj);
  }
}
