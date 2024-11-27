package org.apache.sedona.sql.datasources.osmpbf

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class OsmPbfDataSource extends FileDataSourceV2 with DataSourceRegister {

  override protected def getTable(options: CaseInsensitiveStringMap): Table = {
    OsmPbfTable(
      sparkSession,
      options,
      getPaths(options),
      None
    )
  }

  override def shortName(): _root_.scala.Predef.String = "osmpbf"

  override def fallbackFileFormat: Class[_ <: FileFormat] = null
}
