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
                        userSpecifiedSchema: Option[StructType],
                      ) extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] = {
    Some(StructType(Seq(
      StructField("node", StructType(
        Seq(
          StructField("id", LongType, nullable = false),
          StructField("location", StructType(
            Seq(
              StructField("longitude", DoubleType, nullable = false),
              StructField("latitude", DoubleType, nullable = false),
            )
          ), nullable = false),
          StructField("tags", MapType(StringType, StringType, valueContainsNull = true), nullable = true),
        )
      ), nullable = true),
      StructField("way", StructType(
        Seq(
          StructField("id", LongType, nullable = false),
          StructField("refs", ArrayType(LongType), nullable = false),
          StructField("tags", MapType(StringType, StringType, valueContainsNull = true), nullable = true),
        )
      ), nullable = true),
      StructField("relation", StructType(
        Seq(
          StructField("id", LongType, nullable = false),
          StructField("member_ids", ArrayType(LongType), nullable = false),
          StructField("types", ArrayType(StringType), nullable = false),
          StructField("tags", MapType(StringType, StringType, valueContainsNull = true), nullable = true),
        )
      ), nullable = true),
    )
    ))
  }

  override def formatName: String = "OsmPbf"

  override def fallbackFileFormat: Class[_ <: FileFormat] = null

  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder = {
    val path = caseInsensitiveStringMap.get("path")

    println(caseInsensitiveStringMap.get("path"))
    OsmPbfScanBuilder(sparkSession, fileIndex, schema, path)
  }

  override def newWriteBuilder(logicalWriteInfo: LogicalWriteInfo): WriteBuilder = null

  override def name(): String = "OsmPbf"
}
