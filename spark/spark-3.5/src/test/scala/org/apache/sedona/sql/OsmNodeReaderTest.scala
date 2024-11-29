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
package org.apache.sedona.sql

import io.minio.{ListObjectsArgs, MakeBucketArgs, MinioClient}
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, FunSpec}
import org.testcontainers.containers.MinIOContainer

import java.io.FileInputStream

class OsmNodeReaderTest extends TestBaseScala with Matchers {
  val path: String = resourceFolder + "osmpbf/lubuskie-latest.osm.pbf"

  describe("Loading OSM data") {
    it("should load OSM data from local file") {
      sparkSession.read
        .format("osmpbf")
        .option("parallelFactor", 8)
        .load(path)
        .createOrReplaceTempView("osm")

      val cnt = sparkSession
        .sql("SELECT * FROM osm")
        .count()

      assert(cnt > 0)
    }

    it("should be able to read from osm file on s3") {
      val container = new MinIOContainer("minio/minio:latest")

      container.start()

      val minioClient = createMinioClient(container)
      val makeBucketRequest = MakeBucketArgs
        .builder()
        .bucket("sedona-osm")
        .build()

      minioClient.makeBucket(makeBucketRequest)

      adjustSparkSession(sparkSessionMinio, container)

      val inputPath: String = prepareFile("lubuskie-latest.osm.pbf", path, minioClient)

      minioClient
        .listObjects(ListObjectsArgs.builder().bucket("sedona-osm").build())
        .forEach(obj => println(obj.get().objectName()))

      sparkSessionMinio.read
        .format("osmpbf")
        .option("parallelFactor", 12)
        .load(inputPath)
        .createOrReplaceTempView("osm")

      val cnt = sparkSessionMinio
        .sql("SELECT * FROM osm")
        .count()

      assert(cnt > 0)
    }
  }

  private def prepareFile(name: String, path: String, minioClient: MinioClient): String = {
    val fis = new FileInputStream(path);
    putFileIntoBucket(name, fis, minioClient)

    s"s3a://sedona-osm/$name"
  }

}
