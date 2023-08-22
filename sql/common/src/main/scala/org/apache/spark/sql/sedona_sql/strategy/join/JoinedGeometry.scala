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
package org.apache.spark.sql.sedona_sql.strategy.join

import org.apache.sedona.common.utils.GeomUtils
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.geometry.Envelope2D
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.geotools.referencing.crs.{DefaultEngineeringCRS, DefaultGeographicCRS}
import org.locationtech.jts.geom.{Envelope, Geometry}
import org.opengis.geometry.BoundingBox
import org.opengis.referencing.crs.{CoordinateReferenceSystem, GeographicCRS}

/**
 * Utility functions for generating geometries for spatial join.
 */
object JoinedGeometry {
  /**
   * Convert the given geometry to an envelope expanded by distance.
   * @param geom the geometry to expand
   * @param distance the distance to expand
   * @param isGeography whether the geometry is on a sphere
   * @return the expanded envelope
   */
  def geometryToExpandedEnvelope(geom: Geometry, distance: Double, isGeography: Boolean): Geometry = {
    val envelope = geom.getEnvelopeInternal.copy()
    // Here we use the polar radius of the spheroid as the radius of the sphere, so that the expanded
    // envelope will work for both spherical and spheroidal distances.
    expandEnvelope(envelope, distance, 6357000.0, isGeography)
    geom.getFactory.toGeometry(envelope)
  }

  /**
   * Expand the given envelope by the given distance in meter.
   * For geography, we expand the envelope by the given distance in both longitude and latitude.
   *
   * @param envelope the envelope to expand
   * @param distance in meter
   * @param sphereRadius in meter
   * @param isGeography whether the envelope is on a sphere
   */
  private def expandEnvelope(envelope: Envelope, distance: Double, sphereRadius: Double, isGeography: Boolean): Unit = {
    if (isGeography) {
      val scaleFactor = 1.1 // 10% buffer to get rid of false negatives
      val latRadian = Math.toRadians((envelope.getMinX + envelope.getMaxX) / 2.0)
      val latDeltaRadian = distance / sphereRadius;
      val latDeltaDegree = Math.toDegrees(latDeltaRadian)
      val lonDeltaRadian = Math.max(Math.abs(distance / (sphereRadius * Math.cos(latRadian + latDeltaRadian))),
        Math.abs(distance / (sphereRadius * Math.cos(latRadian - latDeltaRadian))))
      val lonDeltaDegree = Math.toDegrees(lonDeltaRadian)
      envelope.expandBy(latDeltaDegree * scaleFactor, lonDeltaDegree * scaleFactor)
    } else {
      envelope.expandBy(distance)
    }
  }

  /**
   * Convert the given raster to an envelope in WGS84 CRS.
   * @param raster the raster to convert
   * @return the envelope in WGS84 CRS
   */
  def rasterToWGS84Envelope(raster: GridCoverage2D): Geometry = {
    val crs = raster.getCoordinateReferenceSystem
    val envelope = raster.getEnvelope2D
    if (crs == null || crs.isInstanceOf[DefaultEngineeringCRS]) {
      JTS.toGeometry(envelope.asInstanceOf[BoundingBox])
    } else {
      transformToWGS84Envelope(envelope, crs)
    }
  }

  /**
   * Convert the given geometry to an envelope in WGS84 CRS.
   * @param geom the geometry to convert
   * @return the envelope in WGS84 CRS
   */
  def geometryToWGS84Envelope(geom: Geometry): Geometry = {
    val srid = geom.getSRID
    if (srid <= 0 || srid == 4326) {
      geom
    } else {
      val env = geom.getEnvelopeInternal
      val envelope = new Envelope2D(null, env.getMinX, env.getMinY, env.getWidth, env.getHeight)
      val crs = CRS.decode("EPSG:" + srid)
      transformToWGS84Envelope(envelope, crs)
    }
  }

  private def transformToWGS84Envelope(envelope: org.opengis.geometry.Envelope, crs: CoordinateReferenceSystem): Geometry = {
    // We use CRS.transform for envelopes to transform envelopes between different CRSs. This transformation function
    // could handle envelope crossing the anti-meridian and envelope near or covering poles correctly. We won't have
    // these cases properly handled if we transform the original geometries using JTS.transform.
    val transform = CRS.findMathTransform(crs, DefaultGeographicCRS.WGS84)
    val transformedEnvelope = CRS.transform(transform, envelope)
    val minX = transformedEnvelope.getMinimum(0)
    val maxX = transformedEnvelope.getMaximum(0)
    val minY = transformedEnvelope.getMinimum(1)
    val maxY = transformedEnvelope.getMaximum(1)
    val jtsEnvelope = new Envelope(minX, maxX, minY, maxY)
    jtsEnvelope.expandBy(jtsEnvelope.getWidth * 0.1, jtsEnvelope.getHeight * 0.1)
    val geom = JTS.toGeometry(jtsEnvelope)
    if (crs.isInstanceOf[GeographicCRS]) geom else GeomUtils.antiMeridianSafeGeom(geom)
  }
}
