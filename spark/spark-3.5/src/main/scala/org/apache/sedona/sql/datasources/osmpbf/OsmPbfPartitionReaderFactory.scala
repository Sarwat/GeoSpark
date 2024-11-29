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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.util.SerializableConfiguration

case class OsmPbfPartitionReaderFactory(
    conf: Broadcast[SerializableConfiguration],
    headers: Seq[Int],
    loadOptions: OsmPbfLoadOptions)
    extends PartitionReaderFactory {

  override def createReader(inputPartition: InputPartition): PartitionReader[InternalRow] = {
    val index =
      inputPartition.asInstanceOf[org.apache.spark.sql.execution.datasources.FilePartition].index
    inputPartition match {
      case filePartition: FilePartition =>
        val path = filePartition.files.head.toPath

        val options = new OsmPbfOptions(path, headers(index), headers(index + 1))

        val fs = path.getFileSystem(conf.value.value)
        val reader = new OsmPbfReader(options, fs)
        OsmPbfPartitionReader(reader)
      case _ =>
        throw new IllegalArgumentException(
          s"Unexpected partition type: ${inputPartition.getClass.getCanonicalName}")
    }
  }
}
