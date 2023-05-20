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
package org.apache.sedona.common.sphere;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;

import static java.lang.Math.atan2;
import static java.lang.Math.cos;
import static java.lang.Math.sin;
import static java.lang.Math.sqrt;
import static java.lang.Math.toRadians;

public class Haversine
{
    /**
     * Calculate the distance between two points on the earth using the "haversine" formula.
     * This is also known as the great-circle distance
     * This will produce almost identical result to PostGIS ST_DistanceSphere and
     * ST_Distance(useSpheroid=false)
     * @param geom1 The first geometry. Each coordinate is in lat/lon order
     * @param geom2 The second geometry. Each coordinate is in lat/lon order
     * @return
     */
    public static double distance(Geometry geom1, Geometry geom2, double AVG_EARTH_RADIUS)
    {
        Coordinate coordinate1 = geom1.getGeometryType().equals("Point")? geom1.getCoordinate():geom1.getCentroid().getCoordinate();
        Coordinate coordinate2 = geom2.getGeometryType().equals("Point")? geom2.getCoordinate():geom2.getCentroid().getCoordinate();
        // Calculate the distance between the two points
        double lat1 = coordinate1.getX();
        double lon1 = coordinate1.getY();
        double lat2 = coordinate2.getX();
        double lon2 = coordinate2.getY();
        double latDistance = toRadians(lat2 - lat1);
        double lngDistance = toRadians(lon2 - lon1);
        double a = sin(latDistance / 2) * sin(latDistance / 2)
                + cos(toRadians(lat1)) * cos(toRadians(lat2))
                * sin(lngDistance / 2) * sin(lngDistance / 2);
        double c = 2 * atan2(sqrt(a), sqrt(1 - a));
        return AVG_EARTH_RADIUS * c * 1.0;
    }

    // Calculate the distance between two points on the earth using the "haversine" formula.
    // The radius of the earth is 6371.0 km
    public static double distance(Geometry geom1, Geometry geom2)
    {
        return distance(geom1, geom2, 6378137.0);
    }
}
