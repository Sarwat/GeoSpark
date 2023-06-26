/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sedona.flink.expressions;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.TransformException;

public class Functions {
    public static class ST_Area extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.area(geom);
        }
    }

    public static class ST_AreaSpheroid extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.sphere.Spheroid.area(geom);
        }
    }

    public static class ST_Azimuth extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1,
                           @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Geometry geom1 = (Geometry) o1;
            Geometry geom2 = (Geometry) o2;
            return org.apache.sedona.common.Functions.azimuth(geom1, geom2);
        }
    }

    public static class ST_Boundary extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.boundary(geom);
        }
    }

    public static class ST_Buffer extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
                Object o, @DataTypeHint("Double") Double radius) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.buffer(geom, radius);
        }
    }

    public static class ST_ConcaveHull extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o,
                             @DataTypeHint("Double") Double pctConvex) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.concaveHull(geom, pctConvex, false);
        }

        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o,
                             @DataTypeHint("Double") Double pctConvex, @DataTypeHint("Boolean") Boolean allowHoles) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.concaveHull(geom, pctConvex, allowHoles);
        }
    }

    public static class ST_Envelope extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.envelope(geom);
        }
    }

    public static class ST_Dimension extends ScalarFunction {
        @DataTypeHint("Integer")
        public Integer eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.dimension(geom);
        }

    }

    public static class ST_Distance extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1,
                @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Geometry geom1 = (Geometry) o1;
            Geometry geom2 = (Geometry) o2;
            return org.apache.sedona.common.Functions.distance(geom1, geom2);
        }
    }

    public static class ST_DistanceSphere extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1,
                @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Geometry geom1 = (Geometry) o1;
            Geometry geom2 = (Geometry) o2;
            return org.apache.sedona.common.sphere.Haversine.distance(geom1, geom2);
        }

        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1,
                @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2, @DataTypeHint("Double") Double radius) {
            Geometry geom1 = (Geometry) o1;
            Geometry geom2 = (Geometry) o2;
            return org.apache.sedona.common.sphere.Haversine.distance(geom1, geom2, radius);
        }
    }

    public static class ST_DistanceSpheroid extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1,
                @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Geometry geom1 = (Geometry) o1;
            Geometry geom2 = (Geometry) o2;
            return org.apache.sedona.common.sphere.Spheroid.distance(geom1, geom2);
        }
    }

    public static class ST_3DDistance extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1,
                           @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Geometry geom1 = (Geometry) o1;
            Geometry geom2 = (Geometry) o2;
            return org.apache.sedona.common.Functions.distance3d(geom1, geom2);
        }
    }

    public static class ST_Length extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.length(geom);
        }
    }

    public static class ST_LengthSpheroid extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.sphere.Spheroid.length(geom);
        }
    }

    public static class ST_YMin extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o){
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.yMin(geom);
        }
    }

    public static class ST_YMax extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o){
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.yMax(geom);
        }
    }

    public static class ST_ZMax extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o){
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.zMax(geom);
        }
    }

    public static class ST_ZMin extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o){
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.zMin(geom);
        }
    }

    public static class ST_NDims extends ScalarFunction {
        @DataTypeHint("Integer")
        public Integer eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o){
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.nDims(geom);
        }
    }

    public static class ST_Transform extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o, @DataTypeHint("String") String sourceCRS, @DataTypeHint("String") String targetCRS)
            throws FactoryException, TransformException {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.transform(geom, sourceCRS, targetCRS);
        }

        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o, @DataTypeHint("String") String sourceCRS, @DataTypeHint("String") String targetCRS, @DataTypeHint("Boolean") Boolean lenient)
                throws FactoryException, TransformException {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.transform(geom, sourceCRS, targetCRS, lenient);
        }

    }

    public static class ST_FlipCoordinates extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.flipCoordinates(geom);
        }
    }

    public static class ST_GeoHash extends ScalarFunction {
        @DataTypeHint("String")
        public String eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object geometry, Integer precision) {
            Geometry geom = (Geometry) geometry;
            return org.apache.sedona.common.Functions.geohash(geom, precision);
        }
    }

    public static class ST_PointOnSurface extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.pointOnSurface(geom);
        }
    }

    public static class ST_Reverse extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.reverse(geom);
        }
    }

    public static class ST_GeometryN extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o, int n) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.geometryN(geom, n);
        }
    }

    public static class ST_InteriorRingN extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o, int n) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.interiorRingN(geom, n);
        }
    }

    public static class ST_PointN extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o, int n) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.pointN(geom, n);
        }
    }

    public static class ST_NPoints extends ScalarFunction {
        @DataTypeHint("Integer")
        public Integer eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.nPoints(geom);
        }
    }

    public static class ST_NumGeometries extends ScalarFunction {
        @DataTypeHint("Integer")
        public Integer eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.numGeometries(geom);
        }
    }

    public static class ST_NumInteriorRings extends ScalarFunction {
        @DataTypeHint("Integer")
        public Integer eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.numInteriorRings(geom);
        }
    }

    public static class ST_ExteriorRing extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.exteriorRing(geom);
        }
    }

    public static class ST_AsEWKT extends ScalarFunction {
        @DataTypeHint("String")
        public String eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.asEWKT(geom);
        }
    }

    public static class ST_AsText extends ScalarFunction {
        @DataTypeHint("String")
        public String eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.asEWKT(geom);
        }
    }

    public static class ST_AsEWKB extends ScalarFunction {
        @DataTypeHint("Bytes")
        public byte[] eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.asEWKB(geom);
        }
    }

    public static class ST_AsBinary extends ScalarFunction {
        @DataTypeHint("Bytes")
        public byte[] eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.asEWKB(geom);
        }
    }

    public static class ST_AsGeoJSON extends ScalarFunction {
        @DataTypeHint("String")
        public String eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.asGeoJson(geom);
        }
    }

    public static class ST_AsGML extends ScalarFunction {
        @DataTypeHint("String")
        public String eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.asGML(geom);
        }
    }

    public static class ST_AsKML extends ScalarFunction {
        @DataTypeHint("String")
        public String eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.asKML(geom);
        }
    }

    public static class ST_Force_2D extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.force2D(geom);
        }
    }

    public static class ST_IsEmpty extends ScalarFunction {
        @DataTypeHint("Boolean")
        public boolean eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.isEmpty(geom);
        }
    }

    public static class ST_X extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.x(geom);
        }
    }

    public static class ST_Y extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.y(geom);
        }
    }

    public static class ST_Z extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.z(geom);
        }
    }

    public static class ST_XMax extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.xMax(geom);
        }
    }

    public static class ST_XMin extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.xMin(geom);
        }
    }

    public static class ST_BuildArea extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.buildArea(geom);
        }
    }

    public static class ST_SetSRID extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o, int srid) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.setSRID(geom, srid);
        }
    }

    public static class ST_SRID extends ScalarFunction {
        @DataTypeHint("Integer")
        public Integer eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.getSRID(geom);
        }
    }

    public static class ST_IsClosed extends ScalarFunction {
        @DataTypeHint("Boolean")
        public boolean eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.isClosed(geom);
        }
    }

    public static class ST_IsRing extends ScalarFunction {
        @DataTypeHint("Boolean")
        public boolean eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.isRing(geom);
        }
    }

    public static class ST_IsSimple extends ScalarFunction {
        @DataTypeHint("Boolean")
        public boolean eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.isSimple(geom);
        }
    }

    public static class ST_IsValid extends ScalarFunction {
        @DataTypeHint("Boolean")
        public boolean eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.isValid(geom);
        }
    }

    public static class ST_Normalize extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.normalize(geom);
        }
    }

    public static class ST_AddPoint extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1,
                             @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Geometry linestring = (Geometry) o1;
            Geometry point = (Geometry) o2;
            return org.apache.sedona.common.Functions.addPoint(linestring, point);
        }

        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1,
                             @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2,
                             int position) {
            Geometry linestring = (Geometry) o1;
            Geometry point = (Geometry) o2;
            return org.apache.sedona.common.Functions.addPoint(linestring, point, position);
        }
    }

    public static class ST_RemovePoint extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.removePoint(geom);
        }

        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o, int offset) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.removePoint(geom, offset);
        }
    }

    public static class ST_SetPoint extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1, int position,
                             @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Geometry linestring = (Geometry) o1;
            Geometry point = (Geometry) o2;
            return org.apache.sedona.common.Functions.setPoint(linestring, position, point);
        }
    }

    public static class ST_LineFromMultiPoint extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.lineFromMultiPoint(geom);
        }
    }

    public static class ST_Split extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o1,
                             @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o2) {
            Geometry input = (Geometry) o1;
            Geometry blade = (Geometry) o2;
            return org.apache.sedona.common.Functions.split(input, blade);
        }
    }

    public static class ST_S2CellIDs extends ScalarFunction {
        @DataTypeHint(value = "ARRAY<BIGINT>")
        public Long[] eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o,
                             @DataTypeHint("INT") Integer level) {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.s2CellIDs(geom, level);
        }
    }

    public static class ST_GeometricMedian extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) throws Exception {
            Geometry geometry = (Geometry) o;
            return org.apache.sedona.common.Functions.geometricMedian(geometry);
        }

        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o,
                             @DataTypeHint("Double") Double tolerance) throws Exception {
            Geometry geometry = (Geometry) o;
            return org.apache.sedona.common.Functions.geometricMedian(geometry, tolerance);
        }

        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o,
                             @DataTypeHint("Double") Double tolerance,
                             int maxIter) throws Exception {
            Geometry geometry = (Geometry) o;
            return org.apache.sedona.common.Functions.geometricMedian(geometry, tolerance, maxIter);
        }

        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o,
                             @DataTypeHint("Double") Double tolerance,
                             int maxIter, @DataTypeHint("Boolean") Boolean failIfNotConverged) throws Exception {
            Geometry geometry = (Geometry) o;
            return org.apache.sedona.common.Functions.geometricMedian(geometry, tolerance, maxIter, failIfNotConverged);
        }
    }

    public static class ST_FrechetDistance extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object g1,
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object g2) {
            Geometry geom1 = (Geometry) g1;
            Geometry geom2 = (Geometry) g2;
            return org.apache.sedona.common.Functions.frechetDistance(geom1, geom2);
        }
    }

    public static class ST_NumPoints extends ScalarFunction {
        @DataTypeHint(value = "Integer")
        public int eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) throws Exception {
            Geometry geometry = (Geometry) o;
            return org.apache.sedona.common.Functions.numPoints(geometry);
        }
    }

    public static class ST_Force3D extends ScalarFunction {

        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o,
                             @DataTypeHint("Double") Double zValue) {
            Geometry geometry = (Geometry) o;
            return org.apache.sedona.common.Functions.force3D(geometry, zValue);
        }

        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geometry = (Geometry) o;
            return org.apache.sedona.common.Functions.force3D(geometry);
        }
    }

    public static class ST_NRings extends ScalarFunction {
        @DataTypeHint(value = "Integer")
        public int eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) throws Exception {
            Geometry geom = (Geometry) o;
            return org.apache.sedona.common.Functions.nRings(geom);
        }
    }

    public static class ST_Translate extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o,
                             @DataTypeHint("Double") Double deltaX, @DataTypeHint("Double") Double deltaY) {
            Geometry geometry = (Geometry) o;
            return org.apache.sedona.common.Functions.translate(geometry, deltaX, deltaY);
        }

        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o,
                             @DataTypeHint("Double") Double deltaX, @DataTypeHint("Double") Double deltaY, @DataTypeHint("Double") Double deltaZ) {
            Geometry geometry = (Geometry) o;
            return org.apache.sedona.common.Functions.translate(geometry, deltaX, deltaY, deltaZ);
        }
    }

    public static class ST_Affine extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = Geometry.class) Object o, @DataTypeHint("Double") Double a,
                             @DataTypeHint("Double") Double b, @DataTypeHint("Double") Double d, @DataTypeHint("Double") Double e, @DataTypeHint("Double") Double xOff, @DataTypeHint("Double") Double yOff, @DataTypeHint("Double") Double c,
                             @DataTypeHint("Double") Double f, @DataTypeHint("Double") Double g, @DataTypeHint("Double") Double h, @DataTypeHint("Double") Double i,
                             @DataTypeHint("Double") Double zOff) {
            Geometry geometry = (Geometry) o;
            return org.apache.sedona.common.Functions.affine(geometry, a, b, d, e, xOff, yOff, c, f, g, h, i, zOff);
        }

        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o, @DataTypeHint("Double") Double a,
                             @DataTypeHint("Double") Double b,  @DataTypeHint("Double") Double d, @DataTypeHint("Double") Double e,
                             @DataTypeHint("Double") Double xOff, @DataTypeHint("Double") Double yOff) {
            Geometry geometry = (Geometry) o;
            return org.apache.sedona.common.Functions.affine(geometry, a, b, d, e, xOff, yOff);
        }

    }

    public static class ST_BoundingDiagonal extends ScalarFunction {
        @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class)
        public Geometry eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object o) {
            Geometry geometry = (Geometry) o;
            return org.apache.sedona.common.Functions.boundingDiagonal(geometry);
        }
    }

    public static class ST_HausdorffDistance extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object g1,
                           @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object g2,
                           @DataTypeHint("Double") Double densityFrac) throws Exception {
            Geometry geom1 = (Geometry) g1;
            Geometry geom2 = (Geometry) g2;
            return org.apache.sedona.common.Functions.hausdorffDistance(geom1, geom2, densityFrac);
        }

        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object g1,
                           @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object g2) throws Exception {
            Geometry geom1 = (Geometry) g1;
            Geometry geom2 = (Geometry) g2;
            return org.apache.sedona.common.Functions.hausdorffDistance(geom1, geom2);
        }
    }

    public static class ST_Angle extends ScalarFunction {

        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object p1,
                           @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object p2,
                           @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object p3,
                           @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object p4) {
            Geometry point1 = (Geometry) p1;
            Geometry point2 = (Geometry) p2;
            Geometry point3 = (Geometry) p3;
            Geometry point4 = (Geometry) p4;

            return org.apache.sedona.common.Functions.angle(point1, point2, point3, point4);
        }

        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object p1,
                           @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object p2,
                           @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object p3) {
            Geometry point1 = (Geometry) p1;
            Geometry point2 = (Geometry) p2;
            Geometry point3 = (Geometry) p3;

            return org.apache.sedona.common.Functions.angle(point1, point2, point3);
        }

        @DataTypeHint("Double")
        public Double eval(@DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object line1,
                           @DataTypeHint(value = "RAW", bridgedTo = org.locationtech.jts.geom.Geometry.class) Object line2) {
            Geometry lineString1 = (Geometry) line1;
            Geometry lineString2 = (Geometry) line2;

            return org.apache.sedona.common.Functions.angle(lineString1, lineString2);
        }
    }

    public static class ST_Degrees extends ScalarFunction {
        @DataTypeHint("Double")
        public Double eval(@DataTypeHint("Double") Double angleInRadian) {
            return org.apache.sedona.common.Functions.degrees(angleInRadian);
        }
    }
}
