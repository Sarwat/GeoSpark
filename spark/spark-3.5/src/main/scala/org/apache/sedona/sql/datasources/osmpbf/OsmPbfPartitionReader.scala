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

import org.apache.sedona.sql.datasources.osmpbf.model.OsmPbfRecord
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.unsafe.types.UTF8String

import java.util
import scala.reflect.ClassTag

case class OsmPbfPartitionReader(reader: OsmPbfReader) extends PartitionReader[InternalRow] {

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

    InternalRow.fromSeq(Seq(resolveNode(record), resolveWay(record), resolveRelation(record)))
  }

  private def resolveRelation(record: OsmPbfRecord): InternalRow = {
    val relation = record.getRelation

    if (relation == null) {
      return null
    }

    val tags = transformTags(relation.getTags)

    InternalRow.fromSeq(
      Seq(
        relation.getId,
        transformList[java.lang.Long, Long](relation.getMemberIds),
        transformList[java.lang.String, UTF8String](relation.getTypes),
        tags))
  }

  private def resolveWay(record: OsmPbfRecord): InternalRow = {
    val way = record.getWay

    if (way == null) {
      return null
    }

    val tags = transformTags(way.getTags)

    InternalRow.fromSeq(Seq(way.getId, transformList[java.lang.Long, Long](way.getRefs), tags))
  }

  private def resolveNode(record: OsmPbfRecord): InternalRow = {
    val node = record.getNode

    if (node == null) {
      return null
    }

    InternalRow.fromSeq(
      Seq(
        UTF8String.fromString(record.getNode.getId.toString),
        InternalRow.fromSeq(Seq(node.getLatitude, node.getLongitude)),
        transformTags(node.getTags)))
  }

  def transformList[T <: java.lang.Object, R: ClassTag](
      data: util.List[T])(implicit f: () => Seq[R], t: T => R): ArrayData = {
    var refs = f()

    data.forEach(r => {
      refs :+= t(r)
    })

    val refsArray = ArrayData.toArrayData(Array(refs: _*))

    refsArray
  }

  def transformTags(tags: util.Map[String, String]): ArrayBasedMapData = {
    var keys = Seq().map(UTF8String.fromString)
    var values = Seq().map(UTF8String.fromString)

    tags.forEach((k, v) => {
      keys :+= UTF8String.fromString(k)
      values :+= UTF8String.fromString(v)
    })

    val keyArray = ArrayData.toArrayData(Array(keys: _*))

    val valArray = ArrayData.toArrayData(Array(values: _*))

    new ArrayBasedMapData(keyArray, valArray)
  }

  override def close(): Unit = {
    reader.close()
  }
}
