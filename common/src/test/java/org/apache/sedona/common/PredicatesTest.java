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
package org.apache.sedona.common;

import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;

import static org.apache.sedona.common.Constructors.geomFromEWKT;
import static org.apache.sedona.common.Constructors.geomFromWKT;
import static org.apache.sedona.common.Predicates.crossesDateLine;
import static org.junit.Assert.*;

public class PredicatesTest extends TestBase {

    private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

    @Test
    public void testDWithinSuccess() {
        Geometry point1 = GEOMETRY_FACTORY.createPoint(new Coordinate(1, 1));
        Geometry point2 = GEOMETRY_FACTORY.createPoint(new Coordinate(2, 2));
        double distance = 1.42;
        boolean actual = Predicates.dWithin(point1, point2, distance);
        assertTrue(actual);
    }

    @Test
    public void testDWithinFailure() {
        Geometry polygon1 = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 0, 1, 1, 1, 1, 0, 0, 0));
        Geometry polygon2 = GEOMETRY_FACTORY.createPolygon(coordArray(3, 0, 3, 3, 6, 3, 6, 0, 3, 0));


        double distance = 1.2;
        boolean actual = Predicates.dWithin(polygon1, polygon2, distance);
        assertFalse(actual);
    }

    @Test
    public void testDWithinGeomCollection() {
        Geometry polygon1 = GEOMETRY_FACTORY.createPolygon(coordArray(0, 0, 0, 1, 1, 1, 1, 0, 0, 0));
        Geometry polygon2 = GEOMETRY_FACTORY.createPolygon(coordArray(3, 0, 3, 3, 6, 3, 6, 0, 3, 0));
        Geometry point = GEOMETRY_FACTORY.createPoint(new Coordinate(1.1, 0));
        Geometry geometryCollection = GEOMETRY_FACTORY.createGeometryCollection(new Geometry[] {polygon2, point});


        double distance = 1.2;
        boolean actual = Predicates.dWithin(polygon1, geometryCollection, distance);
        assertTrue(actual);
    }

    @Test
    public void testDWithinSpheroid() {
        Geometry seattlePoint = GEOMETRY_FACTORY.createPoint(new Coordinate(-122.335167, 47.608013));
        Geometry newYorkPoint = GEOMETRY_FACTORY.createPoint(new Coordinate(-73.935242, 40.730610));

        double distance = 4000 * 1e3; //distance between NY and Seattle is less than 4000 km
        boolean actual = Predicates.dWithin(newYorkPoint, seattlePoint, distance, true);
        assertTrue(actual);
    }

    @Test
    public void testCrossesDateLine() throws ParseException {
        Geometry geom1 = geomFromEWKT("LINESTRING(170 30, -170 30)");
        Geometry geom2 = geomFromEWKT("LINESTRING(-120 30, -130 40)");
        Geometry geom3 = geomFromEWKT("POLYGON((175 10, -175 10, -175 -10, 175 -10, 175 10))");
        Geometry geom4 = geomFromEWKT("POLYGON((-120 10, -130 10, -130 -10, -120 -10, -120 10))");
        Geometry geom5 = geomFromEWKT("POINT(180 30)");
        Geometry geom6 = geomFromEWKT("POLYGON((160 20, 180 20, 180 -20, 160 -20, 160 20), (165 15, 175 15, 175 -15, 165 -15, 165 15))");
        Geometry geom8 = geomFromEWKT("POLYGON((170 -10, -170 -10, -170 10, 170 10, 170 -10), (175 -5, -175 -5, -175 5, 175 5, 175 -5))");

        assertEquals(true, crossesDateLine(geom1));
        assertEquals(false, crossesDateLine(geom2));
        assertEquals(true, crossesDateLine(geom3));
        assertEquals(false, crossesDateLine(geom4));
        assertEquals(false, crossesDateLine(geom5));
        assertEquals(false, crossesDateLine(geom6));
        assertEquals(true, crossesDateLine(geom8));
    }

}
