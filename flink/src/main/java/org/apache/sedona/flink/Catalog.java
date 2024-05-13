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
                new Aggregators.ST_Envelope_Aggr(),
                new Aggregators.ST_Intersection_Aggr(),
                new Aggregators.ST_Union_Aggr(),
                new Constructors.ST_Point(),
                new Constructors.ST_PointZ(),
                new Constructors.ST_PointM(),
                new Constructors.ST_PointZM(),
                new Constructors.ST_PointFromGeoHash(),
                new Constructors.ST_PointFromText(),
                new Constructors.ST_PointFromWKB(),
                new Constructors.ST_LineFromWKB(),
                new Constructors.ST_LinestringFromWKB(),
                new Constructors.ST_MakePoint(),
                new Constructors.ST_MakePointM(),
                new Constructors.ST_LineStringFromText(),
                new Constructors.ST_LineFromText(),
                new Constructors.ST_PolygonFromText(),
                new Constructors.ST_PolygonFromEnvelope(),
                new Constructors.ST_GeomFromWKT(),
                new Constructors.ST_GeomFromEWKT(),
                new Constructors.ST_GeomFromText(),
                new Constructors.ST_GeometryFromText(),
                new Constructors.ST_GeomFromWKB(),
                new Constructors.ST_GeomFromEWKB(),
                new Constructors.ST_GeomFromGeoJSON(),
                new Constructors.ST_GeomFromGeoHash(),
                new Constructors.ST_GeomFromGML(),
                new Constructors.ST_GeomFromKML(),
                new Constructors.ST_MPointFromText(),
                new Constructors.ST_MPolyFromText(),
                new Constructors.ST_MLineFromText(),
                new Constructors.ST_GeomCollFromText(),
                new Functions.GeometryType(),
                new Functions.ST_Area(),
                new Functions.ST_AreaSpheroid(),
                new Functions.ST_Azimuth(),
                new Functions.ST_Boundary(),
                new Functions.ST_Buffer(),
                new Functions.ST_BestSRID(),
                new Functions.ST_ClosestPoint(),
                new Functions.ST_Centroid(),
                new Functions.ST_Collect(),
                new Functions.ST_CollectionExtract(),
                new Functions.ST_ConcaveHull(),
                new Functions.ST_ConvexHull(),
                new Functions.ST_CrossesDateLine(),
                new Functions.ST_Envelope(),
                new Functions.ST_Difference(),
                new Functions.ST_Dimension(),
                new Functions.ST_Distance(),
                new Functions.ST_DistanceSphere(),
                new Functions.ST_DistanceSpheroid(),
                new Functions.ST_3DDistance(),
                new Functions.ST_H3CellDistance(),
                new Functions.ST_H3CellIDs(),
                new Functions.ST_H3KRing(),
                new Functions.ST_H3ToGeom(),
                new Functions.ST_Dump(),
                new Functions.ST_DumpPoints(),
                new Functions.ST_EndPoint(),
                new Functions.ST_GeometryType(),
                new Functions.ST_Intersection(),
                new Functions.ST_Length(),
                new Functions.ST_Length2D(),
                new Functions.ST_LengthSpheroid(),
                new Functions.ST_LineInterpolatePoint(),
                new Functions.ST_LineLocatePoint(),
                new Functions.ST_LongestLine(),
                new FunctionsGeoTools.ST_Transform(),
                new Functions.ST_FlipCoordinates(),
                new Functions.ST_GeoHash(),
                new Functions.ST_PointOnSurface(),
                new Functions.ST_ReducePrecision(),
                new Functions.ST_Reverse(),
                new Functions.ST_GeometryN(),
                new Functions.ST_InteriorRingN(),
                new Functions.ST_PointN(),
                new Functions.ST_NPoints(),
                new Functions.ST_NumGeometries(),
                new Functions.ST_NumInteriorRings(),
                new Functions.ST_NumInteriorRing(),
                new Functions.ST_ExteriorRing(),
                new Functions.ST_AsEWKT(),
                new Functions.ST_AsEWKB(),
                new Functions.ST_AsHEXEWKB(),
                new Functions.ST_AsText(),
                new Functions.ST_AsBinary(),
                new Functions.ST_AsGeoJSON(),
                new Functions.ST_AsGML(),
                new Functions.ST_AsKML(),
                new Functions.ST_Force_2D(),
                new Functions.ST_IsEmpty(),
                new Functions.ST_X(),
                new Functions.ST_Y(),
                new Functions.ST_Z(),
                new Functions.ST_Zmflag(),
                new Functions.ST_YMax(),
                new Functions.ST_YMin(),
                new Functions.ST_XMax(),
                new Functions.ST_XMin(),
                new Functions.ST_ZMax(),
                new Functions.ST_ZMin(),
                new Functions.ST_NDims(),
                new Functions.ST_BuildArea(),
                new Functions.ST_SetSRID(),
                new Functions.ST_SRID(),
                new Functions.ST_IsClosed(),
                new Functions.ST_IsPolygonCW(),
                new Functions.ST_IsRing(),
                new Functions.ST_IsSimple(),
                new Functions.ST_IsValid(),
                new Functions.ST_Normalize(),
                new Functions.ST_AddPoint(),
                new Functions.ST_RemovePoint(),
                new Functions.ST_SetPoint(),
                new Functions.ST_LineFromMultiPoint(),
                new Functions.ST_LineMerge(),
                new Functions.ST_LineSubstring(),
                new Functions.ST_HasZ(),
                new Functions.ST_HasM(),
                new Functions.ST_M(),
                new Functions.ST_MMin(),
                new Functions.ST_MMax(),
                new Functions.ST_MakeLine(),
                new Functions.ST_Points(),
                new Functions.ST_Polygon(),
                new Functions.ST_Polygonize(),
                new Functions.ST_MakePolygon(),
                new Functions.ST_MakeValid(),
                new Functions.ST_MaxDistance(),
                new Functions.ST_MinimumClearance(),
                new Functions.ST_MinimumBoundingCircle(),
                new Functions.ST_MinimumBoundingRadius(),
                new Functions.ST_Multi(),
                new Functions.ST_StartPoint(),
                new Functions.ST_ShiftLongitude(),
                new Functions.ST_SimplifyPreserveTopology(),
                new Functions.ST_SimplifyVW(),
                new Functions.ST_SimplifyPolygonHull(),
                new Functions.ST_Split(),
                new Functions.ST_Subdivide(),
                new Functions.ST_SymDifference(),
                new Functions.ST_S2CellIDs(),
                new Functions.ST_Snap(),
                new Functions.ST_S2ToGeom(),
                new Functions.ST_GeometricMedian(),
                new Functions.ST_NumPoints(),
                new Functions.ST_Force3D(),
                new Functions.ST_Force3DM(),
                new Functions.ST_Force3DZ(),
                new Functions.ST_Force4D(),
                new Functions.ST_ForceCollection(),
                new Functions.ST_ForcePolygonCW(),
                new Functions.ST_ForceRHR(),
                new Functions.ST_NRings(),
                new Functions.ST_IsPolygonCCW(),
                new Functions.ST_ForcePolygonCCW(),
                new Functions.ST_Translate(),
                new Functions.ST_TriangulatePolygon(),
                new Functions.ST_Union(),
                new Functions.ST_VoronoiPolygons(),
                new Functions.ST_FrechetDistance(),
                new Functions.ST_Affine(),
                new Functions.ST_BoundingDiagonal(),
                new Functions.ST_Angle(),
                new Functions.ST_Degrees(),
                new Functions.ST_HausdorffDistance(),
                new Functions.ST_IsCollection(),
                new Functions.ST_CoordDim(),
                new Functions.ST_IsValidReason()
        };
    }

    public static UserDefinedFunction[] getPredicates() {
        return new UserDefinedFunction[]{
                new Predicates.ST_Intersects(),
                new Predicates.ST_Contains(),
                new Predicates.ST_Crosses(),
                new Predicates.ST_Within(),
                new Predicates.ST_Covers(),
                new Predicates.ST_CoveredBy(),
                new Predicates.ST_Disjoint(),
                new Predicates.ST_Equals(),
                new Predicates.ST_OrderingEquals(),
                new Predicates.ST_Overlaps(),
                new Predicates.ST_Touches(),
                new Predicates.ST_Relate(),
                new Predicates.ST_RelateMatch(),
                new Predicates.ST_DWithin()
        };
    }
}
