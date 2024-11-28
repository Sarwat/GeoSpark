package org.apache.sedona.sql.datasources.osmpbf

import org.apache.sedona.sql.datasources.geopackage.model.GeoPackageReadOptions
import org.apache.sedona.sql.datasources.osmpbf.build.Osmformat.Way
import org.apache.sedona.sql.datasources.osmpbf.model.{OsmNode, OsmPbfRecord}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

import java.io.File
import java.util
import scala.collection.immutable.Seq
import scala.reflect.ClassTag

case class OsmPbfPartitionReader(
  reader: OsmPbfReader
) extends PartitionReader[InternalRow] {

  implicit val f1: () => Seq[Long] = () => Seq().map(Long.unbox)
  implicit val f2: () => Seq[UTF8String] = () => Seq()
  implicit val t1 = (x: java.lang.Long) => x.toLong
  implicit val t2 = (x: String) => UTF8String.fromString(x)

  override def next(): Boolean = reader.next()

  override def get(): InternalRow = {
    val record = reader.get()
    if (record == null) {
      return InternalRow.fromSeq(Seq(null, null, null))
    }

    InternalRow.fromSeq(Seq(
      resolveNode(record),
      resolveWay(record),
      resolveRelation(record)
    ))
  }

  private def resolveRelation(record: OsmPbfRecord): InternalRow = {
    val relation = record.getRelation

    if (relation == null) {
      return null
    }

    val tags = transformTags(relation.getTags)

    InternalRow.fromSeq(Seq(
      relation.getId,
      transformList[java.lang.Long, Long](relation.getMemberIds),
      transformList[java.lang.String, UTF8String](relation.getTypes),
      tags
    ))
  }

  private def resolveWay(record: OsmPbfRecord): InternalRow = {
    val way = record.getWay

    if (way == null) {
      return null
    }

    val tags = transformTags(way.getTags)

    InternalRow.fromSeq(Seq(
      way.getId,
      transformList[java.lang.Long, Long](way.getRefs),
      tags
    ))
  }

  private def resolveNode(record: OsmPbfRecord): InternalRow = {
    val node = record.getNode

    if (node == null) {
      return null
    }

    InternalRow.fromSeq(Seq(
      UTF8String.fromString(record.getNode.getId.toString),
      InternalRow.fromSeq(Seq(
        node.getLatitude,
        node.getLongitude,
      )),
      transformTags(node.getTags)
    ))
  }

  def transformList[T <: java.lang.Object, R: ClassTag](data: util.List[T])(
    implicit f: () => Seq[R],
    t: T => R,
  ): ArrayData = {
    var refs = f()

    data.forEach(r => {
      refs :+= t(r)
    })

    val refsArray = ArrayData.toArrayData(Array(
      refs:_*
    ))

    refsArray
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
