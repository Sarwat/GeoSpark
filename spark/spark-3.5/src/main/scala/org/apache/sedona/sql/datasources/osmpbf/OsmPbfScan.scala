package org.apache.sedona.sql.datasources.osmpbf

import org.apache.sedona.sql.datasources.geopackage.model.GeoPackageOptions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class OsmPbfScan(
                       dataSchema: StructType,
                       sparkSession: SparkSession,
                       fileIndex: PartitioningAwareFileIndex,
                       readDataSchema: StructType,
                       readPartitionSchema: StructType,
                       headers: Seq[Int],
                       path: String
                     ) extends FileScan {
  override def partitionFilters: Seq[Expression] = Seq()

  override def dataFilters: Seq[Expression] = Seq()

  override def createReaderFactory(): PartitionReaderFactory = {
    OsmPbfPartitionReaderFactory(
      sparkSession,
      headers,
      path
    )
  }
}
