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
import org.apache.sedona.sql.datasources.osmpbf.model.OsmRelation;
import org.apache.sedona.sql.datasources.osmpbf.model.RelationType;

public class RelationParser {
  public static List<OsmPbfRecord> parseRelations(
      List<Osmformat.Relation> relations, Osmformat.StringTable stringTable) {
    if (relations == null || relations.isEmpty()) {
      return new ArrayList<>();
    }

    return relations.stream()
        .map(relation -> parse(relation, stringTable))
        .collect(Collectors.toList());
  }

  private static OsmPbfRecord parse(
      Osmformat.Relation relation, Osmformat.StringTable stringTable) {
    if (relation == null) {
      return null;
    }

    List<Long> memberIds = resolveMemberIds(relation);
    List<String> memberTypes = resolveTypes(relation);

    HashMap<String, String> tags =
        TagsResolver.resolveTags(
            relation.getKeysCount(), relation::getKeys, relation::getVals, stringTable);

    OsmRelation relationObj = new OsmRelation(relation.getId(), memberIds, tags, memberTypes);

    return new OsmPbfRecord(relationObj);
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
}
