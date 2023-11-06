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
package org.apache.sedona.common.raster;

import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.GridSampleDimension;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.grid.GridGeometry2D;
import org.opengis.referencing.FactoryException;

import javax.media.jai.RasterFactory;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class RasterBandAccessors {

    public static Double getBandNoDataValue(GridCoverage2D raster, int band) {
        RasterUtils.ensureBand(raster, band);
        GridSampleDimension bandSampleDimension = raster.getSampleDimension(band - 1);
        double noDataValue = RasterUtils.getNoDataValue(bandSampleDimension);
        if (Double.isNaN(noDataValue)) {
            return null;
        } else {
            return noDataValue;
        }
    }

    public static Double getBandNoDataValue(GridCoverage2D raster) {
        return getBandNoDataValue(raster, 1);
    }

    public static long getCount(GridCoverage2D raster, int band, boolean excludeNoDataValue) {
        Double bandNoDataValue = RasterBandAccessors.getBandNoDataValue(raster, band);
        int width = RasterAccessors.getWidth(raster);
        int height = RasterAccessors.getHeight(raster);
        if (excludeNoDataValue && bandNoDataValue != null) {
            RasterUtils.ensureBand(raster, band);
            Raster r = RasterUtils.getRaster(raster.getRenderedImage());
            double[] pixels = r.getSamples(0, 0, width, height, band - 1, (double[]) null);
            long numberOfPixel = 0;
            for (double bandValue : pixels) {
                if (Double.compare(bandValue, bandNoDataValue) != 0) {
                    numberOfPixel += 1;
                }
            }
            return numberOfPixel;
        } else {
            // code for false
            return (long) width * (long) height;
        }
    }

    public static long getCount(GridCoverage2D raster) {
        return getCount(raster, 1, true);
    }

    public static long getCount(GridCoverage2D raster, int band) {
        return getCount(raster, band, true);
    }

//    Removed for now as it InferredExpression doesn't support function with same arity but different argument types
//    Will be added later once it is supported.
//    public static Integer getCount(GridCoverage2D raster, boolean excludeNoDataValue) {
//        return getCount(raster, 1, excludeNoDataValue);
//    }

    public static double[] getSummaryStats(GridCoverage2D rasterGeom, int band, boolean excludeNoDataValue) {
        RasterUtils.ensureBand(rasterGeom, band);
        Raster raster = RasterUtils.getRaster(rasterGeom.getRenderedImage());
        int height = RasterAccessors.getHeight(rasterGeom), width = RasterAccessors.getWidth(rasterGeom);
        double[] pixels = raster.getSamples(0, 0, width, height, band - 1, (double[]) null);
        double count = 0, sum = 0, mean = 0, stddev = 0, min = Double.MAX_VALUE, max = -Double.MAX_VALUE;
        Double noDataValue = RasterBandAccessors.getBandNoDataValue(rasterGeom, band);
        for (double pixel: pixels) {
            if(excludeNoDataValue) {
            // exclude no data values
                if (noDataValue == null || pixel != noDataValue) {
                    count++;
                    sum += pixel;
                    min = Math.min(min, pixel);
                    max = Math.max(max, pixel);
                }
            } else {
                // include no data values
                count = pixels.length;
                sum += pixel;
                min = Math.min(min, pixel);
                max = Math.max(max, pixel);
            }
        }
        if (count == 0) {
            return new double[] {0, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN};
        }
        mean = sum / count;
        for(double pixel: pixels){
            if (excludeNoDataValue){
                if (noDataValue == null || pixel != noDataValue) {
                    stddev += Math.pow(pixel - mean, 2);
                }
            } else {
                stddev += Math.pow(pixel - mean, 2);
            }
        }
        stddev = Math.sqrt(stddev/count);
        return new double[]{count, sum, mean, stddev, min, max};
    }

    public static double[] getSummaryStats(GridCoverage2D raster, int band) {
        return getSummaryStats(raster, band, true);
    }

    public static double[] getSummaryStats(GridCoverage2D raster) {
        return getSummaryStats(raster, 1, true);
    }

//  Adding the function signature when InferredExpression supports function with same arity but different argument types
//    public static double[] getSummaryStats(GridCoverage2D raster, boolean excludeNoDataValue) {
//        return getSummaryStats(raster, 1, excludeNoDataValue);
//    }

    /**
     * @param rasterGeom The raster where the bands will be extracted from.
     * @param bandIndexes The bands to be added to new raster.
     *
     * @return Raster with the specified bands.
     **/
    public static GridCoverage2D getBandCreate(GridCoverage2D rasterGeom, int[] bandIndexes) throws FactoryException {

        List<Object> bandData = getBand(rasterGeom, bandIndexes);

        return RasterUtils.create((WritableRaster) bandData.get(0), (GridGeometry2D) bandData.get(1), (GridSampleDimension[]) bandData.get(2));
    }

    public static List<Object> getBand(GridCoverage2D rasterGeom, int[] bandIndexes) throws FactoryException {
        Double noDataValue;
        double[] metadata = RasterAccessors.metadata(rasterGeom);
        int width = (int) metadata[2], height = (int) metadata[3];
        GridCoverage2D resultRaster = RasterConstructors.makeEmptyRaster(bandIndexes.length, width, height,
                metadata[0], metadata[1], metadata[4], metadata[5], metadata[6], metadata[7], (int) metadata[8]);

        // Get raster data type
        Raster raster = RasterUtils.getRaster(rasterGeom.getRenderedImage());
        int dataTypeCode = raster.getDataBuffer().getDataType();
        boolean isDataTypeIntegral = RasterUtils.isDataTypeIntegral(dataTypeCode);

        // Get band data that's required
        int[] bandsDistinct = Arrays.stream(bandIndexes).distinct().toArray();
        HashMap<Integer, Object> bandData = new HashMap<>();
        for (int curBand: bandsDistinct) {
            RasterUtils.ensureBand(rasterGeom, curBand);
            if (isDataTypeIntegral) {
                bandData.put(curBand - 1, raster.getSamples(0, 0, width, height, curBand - 1, (int[]) null));
            } else {
                bandData.put(curBand - 1, raster.getSamples(0, 0, width, height, curBand - 1, (double[]) null));
            }
        }

        // Create Writable Raster with the datatype of given raster
        WritableRaster wr = RasterFactory.createBandedRaster(dataTypeCode, width, height, bandIndexes.length, null);

        GridSampleDimension[] sampleDimensionsOg = rasterGeom.getSampleDimensions();
        GridSampleDimension[] sampleDimensionsResult = resultRaster.getSampleDimensions();
        for (int i = 0; i < bandIndexes.length; i ++) {
            sampleDimensionsResult[i] = sampleDimensionsOg[bandIndexes[i] - 1];
            if (isDataTypeIntegral) {
                wr.setSamples(0, 0, width, height, i, (int[]) bandData.get(bandIndexes[i] - 1));
            } else {
                wr.setSamples(0, 0, width, height, i, (double[]) bandData.get(bandIndexes[i] - 1));
            }
            noDataValue = RasterBandAccessors.getBandNoDataValue(rasterGeom, bandIndexes[i]);
            GridSampleDimension sampleDimension = sampleDimensionsResult[i];
            if (noDataValue != null) {
                sampleDimensionsResult[i] = RasterUtils.createSampleDimensionWithNoDataValue(sampleDimension, noDataValue);
            }
        }

        return Arrays.asList(wr, resultRaster.getGridGeometry(), sampleDimensionsResult);
    }

    public static String getBandType(GridCoverage2D raster, int band) {
        RasterUtils.ensureBand(raster, band);
        GridSampleDimension bandSampleDimension = raster.getSampleDimension(band - 1);
        return bandSampleDimension.getSampleDimensionType().name();
    }

    public static String getBandType(GridCoverage2D raster){
        return getBandType(raster, 1);
    }

    /**
     * Returns true if the band is filled with only nodata values.
     * @param raster The raster to check
     * @param band The 1-based index of band to check
     * @return true if the band is filled with only nodata values, false otherwise
     */
    public static boolean bandIsNoData(GridCoverage2D raster, int band) {
        RasterUtils.ensureBand(raster, band);
        Raster rasterData = RasterUtils.getRaster(raster.getRenderedImage());
        int width = rasterData.getWidth();
        int height = rasterData.getHeight();
        double noDataValue = RasterUtils.getNoDataValue(raster.getSampleDimension(band - 1));
        if (Double.isNaN(noDataValue)) {
            return false;
        }
        double[] pixels = rasterData.getSamples(0, 0, width, height, band - 1, (double[]) null);
        for (double pixel: pixels) {
            if (Double.compare(pixel, noDataValue) != 0) {
                return false;
            }
        }
        return true;
    }

    public static boolean bandIsNoData(GridCoverage2D raster) {
        return bandIsNoData(raster, 1);
    }
}
