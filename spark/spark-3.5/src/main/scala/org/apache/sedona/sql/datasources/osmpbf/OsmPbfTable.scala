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
package org.apache.sedona.sql.datasources.osmpbf

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.{ArrayType, DoubleType, LongType, MapType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class OsmPbfTable(
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userSpecifiedSchema: Option[StructType])
    extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    Some(
      StructType(
        Seq(
          StructField(
            "node",
            StructType(Seq(
              StructField("id", LongType, nullable = false),
              StructField(
                "location",
                StructType(Seq(
                  StructField("longitude", DoubleType, nullable = false),
                  StructField("latitude", DoubleType, nullable = false))),
                nullable = false),
              StructField(
                "tags",
                MapType(StringType, StringType, valueContainsNull = true),
                nullable = true))),
            nullable = true),
          StructField(
            "way",
            StructType(Seq(
              StructField("id", LongType, nullable = false),
              StructField("refs", ArrayType(LongType), nullable = false),
              StructField(
                "tags",
                MapType(StringType, StringType, valueContainsNull = true),
                nullable = true))),
            nullable = true),
          StructField(
            "relation",
            StructType(Seq(
              StructField("id", LongType, nullable = false),
              StructField("member_ids", ArrayType(LongType), nullable = false),
              StructField("types", ArrayType(StringType), nullable = false),
              StructField(
                "tags",
                MapType(StringType, StringType, valueContainsNull = true),
                nullable = true))),
            nullable = true))))
  }

  override def formatName: String = "OsmPbf"

  override def fallbackFileFormat: Class[_ <: FileFormat] = null

  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder = {
    val path = caseInsensitiveStringMap.get("path")

    var factor = 1
    val parallelFactor = caseInsensitiveStringMap.get("parallelFactor")

    if (parallelFactor != null) {
      factor = parallelFactor.toInt
    }

    OsmPbfScanBuilder(sparkSession, fileIndex, schema, OsmPbfLoadOptions(path, factor))
  }

  override def newWriteBuilder(logicalWriteInfo: LogicalWriteInfo): WriteBuilder = null

  override def name(): String = "OsmPbf"
}
