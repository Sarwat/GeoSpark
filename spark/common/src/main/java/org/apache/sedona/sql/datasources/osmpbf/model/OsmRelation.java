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
package org.apache.sedona.sql.datasources.osmpbf.model;

import java.util.HashMap;
import java.util.List;

public class OsmRelation {
  private HashMap<String, String> tags;
  private List<String> types;
  private long id;
  private List<Long> memberIds;

  public OsmRelation(
      long id, List<Long> memberIds, HashMap<String, String> tags, List<String> types) {
    this.id = id;
    this.memberIds = memberIds;
    this.tags = tags;
    this.types = types;
  }

  public long getId() {
    return id;
  }

  public List<Long> getMemberIds() {
    return memberIds;
  }

  public List<String> getTypes() {
    return types;
  }

  public HashMap<String, String> getTags() {
    return tags;
  }
}
