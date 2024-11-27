package org.apache.sedona.sql.datasources.osmpbf

import org.apache.sedona.sql.datasources.osmpbf.build.Fileformat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.{InMemoryFileIndex, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.types.StructType

import java.io.{DataInputStream, FileInputStream}

case class OsmPbfScanBuilder(
  sparkSession: SparkSession,
  fileIndex: PartitioningAwareFileIndex,
  dataSchema: StructType,
  path: String,
) extends FileScanBuilder(sparkSession, fileIndex, dataSchema) {

  val factor = 12

  override def build(): Scan = {
    val paths = fileIndex.allFiles().map(_.getPath.toString)
    val stream = new FileInputStream(path)
    val pbfStream = new DataInputStream(stream);

    val headers = findHeaders(pbfStream) :+ stream.available

    val length = stream.available

    val chunk = length/factor

    val chunks = (0 until factor).map { i =>
      (i * chunk, (i+1) * chunk)
    }

    val fileIndexAdjusted = new InMemoryFileIndex(
      sparkSession,
      chunks.indices
        .map(_ => paths.head)
        .map(new org.apache.hadoop.fs.Path(_),
      ),
      Map.empty,
      Some(dataSchema)
    )

    OsmPbfScan(
      dataSchema,
        sparkSession,
        fileIndexAdjusted,
        readDataSchema,
        readPartitionSchema,
      headers,
      path
    )
  }

  def findHeaders(stream: DataInputStream): Seq[Int] = {
    val length = stream.available

    val chunk = length/factor

    val chunks = (0 until factor).map { i =>
      (i+1) * chunk
    }

    var dataSum = 0
    var currentIndex = 0

    var result = Seq[Int]()

    while (stream.available() > 0) {
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
    }

    result
  }
}
