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
package org.apache.sedona.core.utils;

import org.apache.sedona.core.enums.GeometryType;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

import java.util.Objects;

public class GeomUtils
{
    public static String printGeom(Geometry geom) {
        if(geom.getUserData()!=null) return geom.toText() + "\t" + geom.getUserData();
        else return geom.toText();
    }

    public static String printGeom(Object geom) {
        Geometry g = (Geometry) geom;
        return printGeom(g);
    }

    public static int hashCode(Geometry geom) {
        return geom.getUserData()==null? geom.hashCode():geom.hashCode()*31 + geom.getUserData().hashCode();
    }
    public static boolean equalsTopoGeom(Geometry geom1, Geometry geom2) {
        if (Objects.equals(geom1.getUserData(), geom2.getUserData())) return geom1.equals(geom2);
        return false;
    }

    public static boolean equalsExactGeom(Geometry geom1, Object geom2) {
        if (! (geom2 instanceof Geometry)) return false;
        Geometry g = (Geometry) geom2;
        if (Objects.equals(geom1.getUserData(), g.getUserData())) return geom1.equalsExact(g);
        else return false;
    }

    /**
     * Swaps the XY coordinates of a geometry.
     */
    public static void flipCoordinates(Geometry g) {
        g.apply(new CoordinateSequenceFilter() {

            @Override
            public void filter(CoordinateSequence seq, int i) {
                double oldX = seq.getCoordinate(i).x;
                double oldY = seq.getCoordinateCopy(i).y;
                seq.getCoordinate(i).setX(oldY);
                seq.getCoordinate(i).setY(oldX);
            }

            @Override
            public boolean isGeometryChanged() {
                return true;
            }

            @Override
            public boolean isDone() {
                return false;
            }
        });
    }
    /*
     * Returns a POINT that is guaranteed to lie on the surface.
     */
    public static Geometry getInteriorPoint(Geometry geometry) {
        if(geometry==null) {
            return null;
        }
        return geometry.getInteriorPoint();
    }

    /**
     * Return the nth point from the given geometry (which could be a linestring or a circular linestring)
     * If the value of n is negative, return a point backwards
     * E.g. if n = 1, return 1st point, if n = -1, return last point
     *
     * @param lineString from which the nth point is to be returned
     * @param n is the position of the point in the geometry
     * @return a point
     */
    public static Geometry getNthPoint(LineString lineString, int n) {
        if (lineString == null || n == 0) {
            return null;
        }

        int p = lineString.getNumPoints();
        if (Math.abs(n) > p) {
            return null;
        }

        Coordinate[] nthCoordinate = new Coordinate[1];
        if (n > 0) {
            nthCoordinate[0] = lineString.getCoordinates()[n - 1];
        } else {
            nthCoordinate[0] = lineString.getCoordinates()[p + n];
        }
        return new Point(new CoordinateArraySequence(nthCoordinate), lineString.getFactory());
    }

    public static Geometry getExteriorRing(Geometry geometry) {
        return geometry.getFactory().createLinearRing(geometry.getCoordinates());
    }
}
