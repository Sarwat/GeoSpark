package org.apache.sedona.sql.datasources.osmpbf

import org.apache.sedona.sql.datasources.shapefile.ShapefilePartition
import org.apache.sedona.sql.datasources.spider.SpiderPartition
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{HasPartitionKey, InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.streaming.MemoryStreamInputPartition
import org.apache.spark.sql.execution.streaming.continuous.{RateStreamContinuousInputPartition, TextSocketContinuousInputPartition}
import org.apache.spark.sql.execution.streaming.sources.{ContinuousMemoryStreamInputPartition, RatePerMicroBatchStreamInputPartition, RateStreamMicroBatchInputPartition, TextSocketInputPartition}

case class OsmPbfPartitionReaderFactory(
  sparkSession: SparkSession,
  headers: Seq[Int],
  path: String
) extends PartitionReaderFactory {

  override def createReader(inputPartition: InputPartition): PartitionReader[InternalRow] = {
    val index = inputPartition.asInstanceOf[org.apache.spark.sql.execution.datasources.FilePartition].index
    println(index)
    val options = new OsmPbfOptions(
      path,
      OsmDataType.NODE,
      headers(index),
      headers(index+1)
    )

    val reader = new OsmPbfReader(options)
    OsmPbfPartitionReader(reader)
  }
}
