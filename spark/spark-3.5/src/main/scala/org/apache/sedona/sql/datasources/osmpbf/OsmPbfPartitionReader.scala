package org.apache.sedona.sql.datasources.osmpbf

import org.apache.sedona.sql.datasources.geopackage.model.GeoPackageReadOptions
import org.apache.sedona.sql.datasources.osmpbf.build.Osmformat.Way
import org.apache.sedona.sql.datasources.osmpbf.model.OsmNode
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

import java.io.File
import java.util
import scala.collection.immutable.Seq

case class OsmPbfPartitionReader(
  reader: OsmPbfReader
) extends PartitionReader[InternalRow] {

  override def next(): Boolean = reader.next()

  override def get(): InternalRow = {
    val pbfData = reader.get()

    val node = pbfData.getNode
    if (node == null && pbfData.getWay == null) {
      return InternalRow.fromSeq(Seq(null, null, null, null))
    }

    if (pbfData.getWay != null) {
      val way = pbfData.getWay

      var refs = Seq().map(Long.unbox)

      pbfData.getWay.getRefs.forEach(r => {
        refs :+= r.toLong
      })

      val refsArray = ArrayData.toArrayData(Array(
        refs:_*
      ))

      val refsMap = transformTags(pbfData.getWay.getTags)

      return InternalRow.fromSeq(Seq(
        null, null, null,
        InternalRow.fromSeq(
          Seq(
            way.getId,
            refsArray,
            refsMap
          )
        )
      ))
    }

    InternalRow.fromSeq(Seq(
      UTF8String.fromString(pbfData.getNode.getId.toString),
      InternalRow.fromSeq(Seq(
        pbfData.getNode.getLatitude,
        pbfData.getNode.getLongitude,
      )),
      transformTags(pbfData.getNode.getTags),
      null,
    ))
  }

  def transformTags(tags: util.Map[String, String]): ArrayBasedMapData = {
    var keys = Seq().map(UTF8String.fromString)
    var values = Seq().map(UTF8String.fromString)

    tags.forEach((k, v) => {
      keys :+= UTF8String.fromString(k)
      values :+= UTF8String.fromString(v)
    })

    val keyArray = ArrayData.toArrayData(Array(
      keys:_*
    ))

    val valArray = ArrayData.toArrayData(Array(
      values:_*
    ))

    new ArrayBasedMapData(keyArray, valArray)
  }

  override def close(): Unit = {}

}
