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

import org.apache.sedona.sql.datasources.osmpbf.build.Fileformat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.{InMemoryFileIndex, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.types.StructType

import java.io.DataInputStream

case class OsmPbfScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    options: OsmPbfLoadOptions)
    extends FileScanBuilder(sparkSession, fileIndex, dataSchema) {

  override def build(): Scan = {
    val paths = fileIndex.allFiles().map(_.getPath.toString)

    val fs = fileIndex
      .allFiles()
      .head
      .getPath
      .getFileSystem(sparkSession.sparkContext.hadoopConfiguration)

    val stream = fs.open(fileIndex.allFiles().head.getPath)

    val streamSize = stream.available
    val pbfStream = new DataInputStream(stream);

    val headers = findHeaders(pbfStream) :+ streamSize
    println(headers.length)

    val fileIndexAdjusted = new InMemoryFileIndex(
      sparkSession,
      (0 until options.parallelFactor)
        .map(_ => paths.head)
        .map(new org.apache.hadoop.fs.Path(_)),
      Map.empty,
      Some(dataSchema))

    OsmPbfScan(
      dataSchema,
      sparkSession,
      fileIndexAdjusted,
      readDataSchema,
      readPartitionSchema,
      headers,
      options)
  }

  def findHeaders(stream: DataInputStream): Seq[Int] = {
    val length = stream.available

    val chunk = length / options.parallelFactor

    val chunks = (0 to options.parallelFactor).map { i =>
      i * chunk
    }

    var dataSum = 0
    var currentIndex = 0

    var result = Seq[Int]()
    var finished = false

    while (!finished) {
      val size = stream.readInt
      dataSum = dataSum + size + 4

      val bufferBlobHeader = new Array[Byte](size)

      stream.readFully(bufferBlobHeader)

      val blobHeader = Fileformat.BlobHeader.parseFrom(bufferBlobHeader)
      dataSum = dataSum + blobHeader.getDatasize

      if (currentIndex < chunks.length && dataSum > chunks(currentIndex)) {
        result = result :+ dataSum
        currentIndex += 1
      }

      stream.skip(blobHeader.getDatasize)
      if (dataSum >= length) {
        finished = true
      }
    }

    result
  }
}
