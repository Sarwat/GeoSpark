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
package org.apache.sedona.flink;

import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.sedona.flink.expressions.*;

public class Catalog {
    public static UserDefinedFunction[] getFuncs() {
        return new UserDefinedFunction[]{
                new Constructors.ST_Point(),
                new Constructors.ST_PointFromText(),
                new Constructors.ST_LineStringFromText(),
                new Constructors.ST_LineFromText(),
                new Constructors.ST_PolygonFromText(),
                new Constructors.ST_PolygonFromEnvelope(),
                new Constructors.ST_GeomFromWKT(),
                new Constructors.ST_GeomFromText(),
                new Constructors.ST_GeomFromWKB(),
                new Constructors.ST_GeomFromGeoJSON(),
                new Constructors.ST_GeomFromGeoHash(),
                new Functions.ST_Buffer(),
                new Functions.ST_Distance(),
                new Functions.ST_Transform(),
                new Functions.ST_FlipCoordinates(),
                new Functions.ST_GeoHash(),
                new Functions.ST_PointOnSurface(),
                new Functions.ST_Reverse(),
                new Functions.ST_PointN(),
                new Functions.ST_ExteriorRing(),
                new Functions.ST_AsEWKT(),
                new Functions.ST_AsEWKB(),
                new Functions.ST_AsText(),
                new Functions.ST_AsBinary(),
                new Functions.ST_AsGeoJSON(),
                new Functions.ST_Force_2D(),
                new Functions.ST_IsEmpty(),
                new Functions.ST_YMax(),
                new Functions.ST_YMin(),
                new Functions.ST_XMax(),
                new Functions.ST_XMin(),
                new Functions.ST_BuildArea(),
                new Functions.ST_SetSRID(),
                new Functions.ST_SRID()
        };
    }

    public static UserDefinedFunction[] getPredicates() {
        return new UserDefinedFunction[]{
                new Predicates.ST_Intersects(),
                new Predicates.ST_Contains(),
                new Predicates.ST_Disjoint(),
                new Predicates.ST_OrderingEquals(),
        };
    }
}
