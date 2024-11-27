package org.apache.sedona.sql

import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, FunSpec}

class OsmNodeReaderTest extends TestBaseScala with Matchers {
  describe("") {
    it("") {
      sparkSession
        .read
        .format("osmpbf")
        .load("/Users/pawelkocinski/Desktop/projects/osm-data-reader/src/main/resources/lubuskie-latest.osm.pbf")
//        .load("/Users/pawelkocinski/Desktop/projects/osm-data-reader/src/main/resources/poland-latest.osm.pbf")
        .createOrReplaceTempView("osm")

      sparkSession.sql("SELECT * FROM osm")
        .where("ways is not null and size(ways.tags) > 0")
        .show(5, false)
    }
  }

}
