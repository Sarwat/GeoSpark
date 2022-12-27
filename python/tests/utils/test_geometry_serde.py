#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from shapely.geometry.base import BaseGeometry
from sedona.utils import geometry_serde

from shapely.geometry import Point
from shapely.geometry import LineString
from shapely.geometry import Polygon
from shapely.geometry import MultiPoint
from shapely.geometry import MultiLineString
from shapely.geometry import MultiPolygon
from shapely.geometry import GeometryCollection


class TestGeometrySerde:
    def test_point(self):
        points = [
            Point(),
            Point(10, 20),
            Point(10, 20, 30)
        ]
        self._test_serde_roundtrip(points)

    def test_linestring(self):
        linestrings = [
            LineString(),
            LineString([(10, 20), (30, 40)]),
            LineString([(10, 20), (30, 40), (50, 60)]),
            LineString([(10, 20, 30), (30, 40, 50), (50, 60, 70)]),
        ]
        self._test_serde_roundtrip(linestrings)

    def test_multi_point(self):
        multi_points = [
            MultiPoint(),
            MultiPoint([(10, 20)]),
            MultiPoint([(10, 20), (30, 40)]),
            MultiPoint([(10, 20), (30, 40), (50, 60)]),
            MultiPoint([(10, 20, 30), (30, 40, 50), (50, 60, 70)]),
        ]
        self._test_serde_roundtrip(multi_points)

    def test_multi_linestring(self):
        multi_linestrings = [
            MultiLineString(),
            MultiLineString([[(10, 20), (30, 40)]]),
            MultiLineString([[(10, 20), (30, 40)], [(50, 60), (70, 80)]]),
            MultiLineString([[(10, 20, 30), (30, 40, 50)], [(50, 60, 70), (70, 80, 90)]]),
        ]
        self._test_serde_roundtrip(multi_linestrings)

    def test_polygon(self):
        ext = [(0, 0), (10, 0), (10, 10), (0, 10), (0, 0)]
        int0 = [(1, 1), (1, 1.5), (1.5, 1.5), (1.5, 1), (1, 1)]
        int1 = [(2, 2), (2, 2.5), (2.5, 2.5), (2.5, 2), (2, 2)]
        polygons = [
            Polygon(),
            Polygon(ext),
            Polygon(ext, [int0]),
            Polygon(ext, [int0, int1]),
        ]
        self._test_serde_roundtrip(polygons)

    def test_multi_polygon(self):
        ext = [(0, 0), (100, 0), (100, 100), (0, 100), (0, 0)]
        int0 = [(10, 10), (10, 15), (15, 15), (15, 10), (10, 10)]
        int1 = [(2, 2), (2, 2.5), (2.5, 2.5), (2.5, 2), (2, 2)]
        multi_polygons = [
            MultiPolygon(),
            MultiPolygon([Polygon(ext)]),
            MultiPolygon([Polygon(ext), Polygon(ext, [int0])]),
            MultiPolygon([Polygon(ext), Polygon(ext, [int0, int1])]),
            MultiPolygon([Polygon(ext, [int1]), Polygon(ext), Polygon(ext, [int0, int1])]),
        ]
        self._test_serde_roundtrip(multi_polygons)

    def test_geometry_collection(self):
        geometry_collections = [
            GeometryCollection(),
            GeometryCollection([Point(10, 20), LineString([(10, 20), (30, 40)]), Point(30, 40)]),
            GeometryCollection([
                MultiPoint([(10, 20), (30, 40)]),
                MultiLineString([[(10, 20), (30, 40)], [(50, 60), (70, 80)]]),
                MultiPolygon([
                    Polygon(
                        [(0, 0), (100, 0), (100, 100), (0, 100), (0, 0)],
                        [[(10, 10), (10, 15), (15, 15), (15, 10), (10, 10)]])
                ]),
                Point(100, 200)
            ]),
            GeometryCollection([
                GeometryCollection([Point(10, 20), LineString([(10, 20), (30, 40)]), Point(30, 40)]),
                GeometryCollection([
                    MultiPoint([(10, 20), (30, 40)]),
                    MultiLineString([[(10, 20), (30, 40)], [(50, 60), (70, 80)]]),
                    Point(10, 20)
                ])
            ])
        ]
        self._test_serde_roundtrip(geometry_collections)

    @staticmethod
    def _test_serde_roundtrip(geoms):
        for geom in geoms:
            geom_actual = TestGeometrySerde.serde_roundtrip(geom)
            assert geom_actual.equals_exact(geom, 1e-6)

    @staticmethod
    def serde_roundtrip(geom: BaseGeometry) -> BaseGeometry:
        buffer = geometry_serde.serialize(geom)
        geom2, offset = geometry_serde.deserialize(buffer)
        return geom2
