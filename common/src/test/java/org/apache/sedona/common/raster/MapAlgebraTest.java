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
import org.geotools.coverage.grid.GridCoverage2D;
import org.junit.Assert;
import org.junit.Test;
import org.opengis.referencing.FactoryException;

import java.awt.image.DataBuffer;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.*;

public class MapAlgebraTest extends RasterTestBase
{
    @Test
    public void testAddBandAsArrayAppend()
            throws FactoryException
    {
        GridCoverage2D raster = createEmptyRaster(1);
        double[] band1 = new double[raster.getRenderedImage().getWidth() * raster.getRenderedImage().getHeight()];
        for (int i = 0; i < band1.length; i++) {
            band1[i] = i;
        }
        double[] band2 = new double[raster.getRenderedImage().getWidth() * raster.getRenderedImage().getHeight()];
        double[] band3 = new double[raster.getRenderedImage().getWidth() * raster.getRenderedImage().getHeight()];
        for (int i = 0; i < band2.length; i++) {
            band2[i] = i * 2;
            band3[i] = i * 3;
        }
        // Replace the first band
        GridCoverage2D rasterWithBand1 = MapAlgebra.addBandFromArray(raster, band1, 1);
        assertEquals(1, RasterAccessors.numBands(rasterWithBand1));
        assertEquals(raster.getEnvelope(), rasterWithBand1.getEnvelope());
        assertEquals(raster.getCoordinateReferenceSystem2D(), rasterWithBand1.getCoordinateReferenceSystem2D());
        assertEquals(RasterAccessors.srid(raster), RasterAccessors.srid(rasterWithBand1));

        //replace the first band with a customNoDataValue
        rasterWithBand1 = MapAlgebra.addBandFromArray(rasterWithBand1, band1, 1, -999d);
        assertEquals(1, RasterAccessors.numBands(rasterWithBand1));
        assertEquals(raster.getEnvelope(), rasterWithBand1.getEnvelope());
        assertEquals(raster.getCoordinateReferenceSystem2D(), rasterWithBand1.getCoordinateReferenceSystem2D());
        assertEquals(RasterAccessors.srid(raster), RasterAccessors.srid(rasterWithBand1));
        assertEquals(-999, RasterUtils.getNoDataValue(rasterWithBand1.getSampleDimension(0)), 1e-9);

        //replace first band with a different customNoDataValue
        rasterWithBand1 = MapAlgebra.addBandFromArray(rasterWithBand1, band1, 1, -9999d);
        assertEquals(1, RasterAccessors.numBands(rasterWithBand1));
        assertEquals(raster.getEnvelope(), rasterWithBand1.getEnvelope());
        assertEquals(raster.getCoordinateReferenceSystem2D(), rasterWithBand1.getCoordinateReferenceSystem2D());
        assertEquals(RasterAccessors.srid(raster), RasterAccessors.srid(rasterWithBand1));
        assertEquals(-9999, RasterUtils.getNoDataValue(rasterWithBand1.getSampleDimension(0)), 1e-9);

        //remove noDataValue from the first band
        rasterWithBand1 = MapAlgebra.addBandFromArray(rasterWithBand1, band1, 1, null);
        assertEquals(1, RasterAccessors.numBands(rasterWithBand1));
        assertEquals(raster.getEnvelope(), rasterWithBand1.getEnvelope());
        assertEquals(raster.getCoordinateReferenceSystem2D(), rasterWithBand1.getCoordinateReferenceSystem2D());
        assertEquals(RasterAccessors.srid(raster), RasterAccessors.srid(rasterWithBand1));
        assertTrue(Double.isNaN(RasterUtils.getNoDataValue(rasterWithBand1.getSampleDimension(0))));

        // Append a new band with default noDataValue
        GridCoverage2D rasterWithBand2 = MapAlgebra.addBandFromArray(rasterWithBand1, band2);
        assertEquals(2, RasterAccessors.numBands(rasterWithBand2));
        assertEquals(raster.getEnvelope(), rasterWithBand2.getEnvelope());
        assertEquals(raster.getCoordinateReferenceSystem2D(), rasterWithBand2.getCoordinateReferenceSystem2D());
        assertEquals(RasterAccessors.srid(raster), RasterAccessors.srid(rasterWithBand2));
        assertTrue(Double.isNaN(RasterUtils.getNoDataValue(rasterWithBand2.getSampleDimension(1))));

        // Append a new band with custom noDataValue
        GridCoverage2D rasterWithBand3 = MapAlgebra.addBandFromArray(rasterWithBand2, band3, 3, 2d);
        assertEquals(3, RasterAccessors.numBands(rasterWithBand3));
        assertEquals(raster.getEnvelope(), rasterWithBand3.getEnvelope());
        assertEquals(raster.getCoordinateReferenceSystem2D(), rasterWithBand3.getCoordinateReferenceSystem2D());
        assertEquals(RasterAccessors.srid(raster), RasterAccessors.srid(rasterWithBand3));
        assertEquals(2, RasterUtils.getNoDataValue(rasterWithBand3.getSampleDimension(2)), 1e-9);

        // Check the value of the first band when use the raster with only one band
        double[] firstBand = MapAlgebra.bandAsArray(rasterWithBand1, 1);
        for (int i = 0; i < firstBand.length; i++) {
            assertEquals(i, firstBand[i], 0.1);
        }
        // Check the value of the first band when use the raster with two bands

        //Check the value of the first band when use the raster with three bands
        firstBand = MapAlgebra.bandAsArray(rasterWithBand3, 1);
        for (int i = 0; i < firstBand.length; i++) {
            assertEquals(i, firstBand[i], 0.1);
        }
        // Check the value of the second band
        double[] secondBand = MapAlgebra.bandAsArray(rasterWithBand2, 2);
        for (int i = 0; i < secondBand.length; i++) {
            assertEquals(i * 2, secondBand[i], 0.1);
        }

        // Check the value of the third band
        double[] thirdBand = MapAlgebra.bandAsArray(rasterWithBand3, 3);
        for (int i = 0; i < secondBand.length; i++) {
            assertEquals(i * 3, thirdBand[i], 0.1);
        }
    }

    @Test
    public void testAddBandAsArrayReplace()
            throws FactoryException
    {
        GridCoverage2D raster = createEmptyRaster(2);
        double[] band1 = new double[raster.getRenderedImage().getWidth() * raster.getRenderedImage().getHeight()];
        for (int i = 0; i < band1.length; i++) {
            band1[i] = i;
        }
        double[] band2 = new double[raster.getRenderedImage().getWidth() * raster.getRenderedImage().getHeight()];
        for (int i = 0; i < band2.length; i++) {
            band2[i] = i * 2;
        }
        // Replace the first band
        GridCoverage2D rasterWithBand1 = MapAlgebra.addBandFromArray(raster, band1, 1);
        assertEquals(2, RasterAccessors.numBands(rasterWithBand1));
        assertEquals(raster.getEnvelope(), rasterWithBand1.getEnvelope());
        assertEquals(raster.getCoordinateReferenceSystem2D(), rasterWithBand1.getCoordinateReferenceSystem2D());
        assertEquals(RasterAccessors.srid(raster), RasterAccessors.srid(rasterWithBand1));

        // Replace the second band
        GridCoverage2D rasterWithBand2 = MapAlgebra.addBandFromArray(rasterWithBand1, band2, 2);
        assertEquals(2, RasterAccessors.numBands(rasterWithBand2));
        assertEquals(raster.getEnvelope(), rasterWithBand2.getEnvelope());
        assertEquals(raster.getCoordinateReferenceSystem2D(), rasterWithBand2.getCoordinateReferenceSystem2D());
        assertEquals(RasterAccessors.srid(raster), RasterAccessors.srid(rasterWithBand2));

        // Check the value of the first band when use the raster with only one band
        double[] firstBand = MapAlgebra.bandAsArray(rasterWithBand1, 1);
        for (int i = 0; i < firstBand.length; i++) {
            assertEquals(i, firstBand[i], 0.1);
        }
        // Check the value of the first band when use the raster with two bands
        firstBand = MapAlgebra.bandAsArray(rasterWithBand2, 1);
        for (int i = 0; i < firstBand.length; i++) {
            assertEquals(i, firstBand[i], 0.1);
        }
        // Check the value of the second band
        double[] secondBand = MapAlgebra.bandAsArray(rasterWithBand2, 2);
        for (int i = 0; i < secondBand.length; i++) {
            assertEquals(i * 2, secondBand[i], 0.1);
        }
    }

    @Test
    public void testBandAsArray()
            throws FactoryException
    {
        int widthInPixel = 10;
        int heightInPixel = 10;
        double upperLeftX = 0;
        double upperLeftY = 0;
        double cellSize = 1;
        int numbBands = 1;
        GridCoverage2D raster = RasterConstructors.makeEmptyRaster(numbBands, widthInPixel, heightInPixel, upperLeftX, upperLeftY, cellSize);
        // Out of bound index should return null
        double[] band = MapAlgebra.bandAsArray(raster, 0);
        assertNull(band);
        band = MapAlgebra.bandAsArray(raster, 1);
        assertEquals(widthInPixel * heightInPixel, band.length);
        for (int i = 0; i < band.length; i++) {
            // The default value is 0.0
            assertEquals(0.0, band[i], 0.1);
        }
        // Now set the value of the first band and check again
        for (int i = 0; i < band.length; i++) {
            band[i] = i * 0.1;
        }
        double[] bandNew = MapAlgebra.bandAsArray(MapAlgebra.addBandFromArray(raster, band, 1), 1);
        assertEquals(band.length, bandNew.length);
        for (int i = 0; i < band.length; i++) {
            assertEquals(band[i], bandNew[i], 1e-9);
        }
    }

    @Test
    public void testMultiplyFactor() {
        double[] input = new double[] {200, 100, 145, 255};
        double factor = 1.5;
        double[] actual = MapAlgebra.multiplyFactor(input, factor);
        double[] expected = new double[] {300.0, 150.0, 217.5, 382.5};
        assertArrayEquals(expected, actual, 0.01d);

        factor = 2;
        actual = MapAlgebra.multiplyFactor(input, factor);
        expected = new double[]{400.0, 200.0, 290.0, 510.0};
        assertArrayEquals(expected, actual, 0.1d);
    }

    @Test
    public void testAdd() {
        double[] band1 = new double[] {200, 100, 145, 245};
        double[] band2 = new double[] {55, 155, 110, 10};
        double[] actual = MapAlgebra.add(band1, band2);
        double[] expected = new double[] {255.0, 255.0, 255.0, 255.0};
        assertArrayEquals(expected, actual, 0.1d);
    }

    @Test
    public void testSubtract() {
        double[] band1 = new double[] {55, 155, 110, 10};
        double[] band2 = new double[] {255.0, 255.0, 255.0, 255.0};
        double[] actual = MapAlgebra.subtract(band1, band2);
        double[] expected = new double[] {200, 100, 145, 245};
        assertArrayEquals(expected, actual, 0.1d);
    }

    @Test
    public void testMultiply() {
        double[] band1 = new double[] {20.43, 40.67, 60.91};
        double[] band2 = new double[] {2.84, 5.26, 8.97};
        double[] actual = MapAlgebra.multiply(band1, band2);
        double[] expected = new double[] {58.02119999999999, 213.9242, 546.3627};
        assertArrayEquals(expected, actual, 0.00001d);

        band1 = new double[] {200, 400, 500};
        band2 = new double[] {2, 2.5, 3};
        actual = MapAlgebra.multiply(band1, band2);
        expected = new double[] {400.0, 1000.0, 1500.0};
        assertArrayEquals(expected, actual, 0.1d);
    }

    @Test
    public void testDivide() {
        double[] band1 = new double[] {20.43, 40.67, 60.91};
        double[] band2 = new double[] {2.84, 5.26, 8.97};
        double[] actual = MapAlgebra.divide(band1, band2);
        double[] expected = new double[] {7.19, 7.73, 6.79};
        assertArrayEquals(expected, actual, 0.001d);

        band1 = new double[] {200, 400, 500};
        band2 = new double[] {2, 2.5, 3};
        actual = MapAlgebra.divide(band1, band2);
        expected = new double[] {100.0, 160.0, 166.67};
        assertArrayEquals(expected, actual, 0.01d);
    }

    @Test
    public void testModulo() {
        double[] band = new double[] {100.0, 260.0, 189.0, 106.0, 230.0, 169.0, 196.0};
        double dividend = 90;
        double[] actual = MapAlgebra.modulo(band, dividend);
        double[] expected = new double[] {10.0, 80.0, 9.0, 16.0, 50.0, 79.0, 16.0};
        assertArrayEquals(expected, actual, 0.1d);

        band = new double[] {230.0, 345.0, 136.0, 106.0, 134.0, 105.0};
        actual = MapAlgebra.modulo(band, dividend);
        expected = new double[] {50.0, 75.0, 46.0, 16.0, 44.0, 15.0};
        assertArrayEquals(expected, actual, 0.1d);
    }

    @Test
    public void testSquareRoot() {
        double[] band = new double[] {8.0, 16.0, 24.0};
        double[] actual = MapAlgebra.squareRoot(band);
        double[] expected = new double[] {2.83, 4.0, 4.9};
        assertArrayEquals(expected, actual, 0.01d);
    }

    @Test
    public void testBitwiseAnd() {
        double[] band1 = new double[] {15.0, 25.0, 35.0};
        double[] band2 = new double[] {5.0, 15.0, 25.0};
        double[] actual = MapAlgebra.bitwiseAnd(band1, band2);
        double[] expected = new double[]{5.0, 9.0, 1.0};
        assertArrayEquals(expected, actual, 0.1d);
    }

    @Test
    public void testBitwiseOr() {
        double[] band1 = new double[] {15.0, 25.0, 35.0};
        double[] band2 = new double[] {5.0, 15.0, 25.0};
        double[] actual = MapAlgebra.bitwiseOr(band1, band2);
        double[] expected = new double[]{15.0, 31.0, 59.0};
        assertArrayEquals(expected, actual, 0.1d);
    }

    @Test
    public void testLogicalDifference() {
        double[] band1 = new double[] {10.0, 20.0, 30.0};
        double[] band2 = new double[] {40.0, 20.0, 50.0};
        double[] actual = MapAlgebra.logicalDifference(band1, band2);
        double[] expected = new double[] {10.0, 0.0, 30.0};
        assertArrayEquals(expected, actual, 0.1d);
    }

    @Test
    public void testLogicalOver() {
        double[] band1 = new double[] {0.0, 0.0, 30.0};
        double[] band2 = new double[] {40.0, 20.0, 50.0};
        double[] actual = MapAlgebra.logicalOver(band1, band2);
        double[] expected = new double[] {40.0, 20.0, 30.0};
        assertArrayEquals(expected, actual, 0.1d);
    }

    @Test
    public void testNormalize() {
        double[] band1 = {800.0, 900.0, 0.0, 255.0};
        double[] band2 = {0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15};
        double[] band3 = {16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31};
        double[] band4 = {-16,-15,-14,-13,-12,-11,-10,-9,-8,-7,-6,-5,-4,-3,-2,-1};
        double[] actual1 = MapAlgebra.normalize(band1);
        double[] actual2 = MapAlgebra.normalize(band2);
        double[] actual3 = MapAlgebra.normalize(band3);
        double[] actual4 = MapAlgebra.normalize(band4);
        double[] expected1 = {226.66666666666666, 255.0, 0.0, 72.25};
        double[] expected2 = {0.0, 17.0, 34.0, 51.0, 68.0, 85.0, 102.0, 119.0, 136.0, 153.0, 170.0, 187.0, 204.0, 221.0, 238.0, 255.0};
        assertArrayEquals(expected1, actual1, 0.1d);
        assertArrayEquals(expected2, actual2, 0.1d);
        assertArrayEquals(expected2, actual3, 0.1d);
        assertArrayEquals(expected2, actual4, 0.1d);
    }

    @Test
    public void testNormalizeAll() throws FactoryException {
        GridCoverage2D raster1 = RasterConstructors.makeEmptyRaster(2, 4, 4, 0, 0, 1);
        GridCoverage2D raster2 = RasterConstructors.makeEmptyRaster(2, 4, 4, 0, 0, 1);
        GridCoverage2D raster3 = RasterConstructors.makeEmptyRaster(2, "I", 4, 4, 0, 0, 1);
        GridCoverage2D raster4 = RasterConstructors.makeEmptyRaster(2, 4, 4, 0, 0, 1);
        GridCoverage2D raster5 = RasterConstructors.makeEmptyRaster(2, 4, 4, 0, 0, 1);

        for (int band = 1; band <= 2; band++) {
            double[] bandValues1 = new double[4 * 4];
            double[] bandValues2 = new double[4 * 4];
            double[] bandValues3 = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16};
            double[] bandValues4 = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,0};
            double[] bandValues5 = new double[4 * 4];
            for (int i = 0; i < bandValues1.length; i++) {
                bandValues1[i] = (i) * band;
                bandValues2[i] = (1) * (band-1);
                bandValues5[i] = i + ((band-1)*15);
            }
            raster1 = MapAlgebra.addBandFromArray(raster1, bandValues1, band);
            raster2 = MapAlgebra.addBandFromArray(raster2, bandValues2, band);
            raster3 = MapAlgebra.addBandFromArray(raster3, bandValues3, band);
            raster4 = MapAlgebra.addBandFromArray(raster4, bandValues4, band);
            raster4 = RasterBandEditors.setBandNoDataValue(raster4, band, 0.0);
            raster5 = MapAlgebra.addBandFromArray(raster5, bandValues5, band);
        }
        raster3 = RasterBandEditors.setBandNoDataValue(raster3, 1, 16.0);
        raster3 = RasterBandEditors.setBandNoDataValue(raster3, 2, 1.0);

        GridCoverage2D normalizedRaster1 = MapAlgebra.normalizeAll(raster1, 0, 255, false, -9999.0);
        GridCoverage2D normalizedRaster2 = MapAlgebra.normalizeAll(raster1, 256d, 511d, false, -9999.0);
        GridCoverage2D normalizedRaster3 = MapAlgebra.normalizeAll(raster2);
        GridCoverage2D normalizedRaster4 = MapAlgebra.normalizeAll(raster3, 0, 255, true, 95.0);
        GridCoverage2D normalizedRaster5 = MapAlgebra.normalizeAll(raster4, 0, 255, true, 255.0);
        GridCoverage2D normalizedRaster6 = MapAlgebra.normalizeAll(raster5, 0.0, 255.0, -9999.0, 0.0, 30.0);
        GridCoverage2D normalizedRaster7 = MapAlgebra.normalizeAll(raster5, 0, 255, false, -9999.0);
        GridCoverage2D normalizedRaster8 = MapAlgebra.normalizeAll(raster3, 0, 255);
        GridCoverage2D normalizedRaster9 = MapAlgebra.normalizeAll(raster3, 0, 255, false);

        double[] expected1 = {0.0, 17.0, 34.0, 51.0, 68.0, 85.0, 102.0, 119.0, 136.0, 153.0, 170.0, 187.0, 204.0, 221.0, 238.0, 255.0};
        double[] expected2 = {256.0, 273.0, 290.0, 307.0, 324.0, 341.0, 358.0, 375.0, 392.0, 409.0, 426.0, 443.0, 460.0, 477.0, 494.0, 511.0};
        double[] expected3 = {0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0};
        double[] expected4 = {0.0, 17.0, 34.0, 51.0, 68.0, 85.0, 102.0, 119.0, 136.0, 153.0, 170.0, 187.0, 204.0, 221.0, 238.0, 95.0};
        double[] expected5 = {95.0, 17.0, 34.0, 51.0, 68.0, 85.0, 102.0, 119.0, 136.0, 153.0, 170.0, 187.0, 204.0, 221.0, 238.0, 255.0};
        double[] expected6 = {0.0, 18.214285714285715, 36.42857142857143, 54.642857142857146, 72.85714285714286, 91.07142857142857, 109.28571428571429, 127.5, 145.71428571428572, 163.92857142857142, 182.14285714285714, 200.35714285714286, 218.57142857142858, 236.78571428571428, 255.0, 255.0};
        double[] expected7 = {0.0, 16.0, 33.0, 50.0, 67.0, 84.0, 101.0, 118.0, 135.0, 152.0, 169.0, 186.0, 203.0, 220.0, 237.0, 255.0};
        double[] expected8 = {255.0, 16.0, 33.0, 50.0, 67.0, 84.0, 101.0, 118.0, 135.0, 152.0, 169.0, 186.0, 203.0, 220.0, 237.0, 254.0};
        double[] expected9 = {0.0, 18.0, 36.0, 54.0, 72.0, 90.0, 108.0, 127.0, 145.0, 163.0, 181.0, 199.0, 217.0, 235.0, 254.0, 255.0};
        double[] expected10 = {255.0, 0.0, 18.0, 36.0, 54.0, 72.0, 90.0, 108.0, 127.0, 145.0, 163.0, 181.0, 199.0, 217.0, 235.0, 254.0};

        // Step 3: Validate the results for each band
        for (int band = 1; band <= 2; band++) {
            double[] normalizedBand1 = MapAlgebra.bandAsArray(normalizedRaster1, band);
            double[] normalizedBand2 = MapAlgebra.bandAsArray(normalizedRaster2, band);
            double[] normalizedBand5 = MapAlgebra.bandAsArray(normalizedRaster5, band);
            double[] normalizedBand6 = MapAlgebra.bandAsArray(normalizedRaster6, band);
            double[] normalizedBand7 = MapAlgebra.bandAsArray(normalizedRaster7, band);
            double normalizedMin6 = Arrays.stream(normalizedBand6).min().getAsDouble();
            double normalizedMax6 = Arrays.stream(normalizedBand6).max().getAsDouble();

            assertEquals(Arrays.toString(expected1), Arrays.toString(normalizedBand1));
            assertEquals(Arrays.toString(expected2), Arrays.toString(normalizedBand2));
            assertEquals(Arrays.toString(expected6), Arrays.toString(normalizedBand5));
            assertEquals(Arrays.toString(expected1), Arrays.toString(normalizedBand7));

            assertEquals(0+((band-1)*127.5), normalizedMin6, 0.01d);
            assertEquals(127.5+((band-1)*127.5), normalizedMax6, 0.01d);
        }

        assertEquals(95.0, RasterUtils.getNoDataValue(normalizedRaster4.getSampleDimension(0)), 0.01d);
        assertEquals(95.0, RasterUtils.getNoDataValue(normalizedRaster4.getSampleDimension(1)), 0.01d);

        assertEquals(Arrays.toString(expected3), Arrays.toString(MapAlgebra.bandAsArray(normalizedRaster3, 1)));
        assertEquals(Arrays.toString(expected4), Arrays.toString(MapAlgebra.bandAsArray(normalizedRaster4, 1)));
        assertEquals(Arrays.toString(expected5), Arrays.toString(MapAlgebra.bandAsArray(normalizedRaster4, 2)));
        assertEquals(Arrays.toString(expected7), Arrays.toString(MapAlgebra.bandAsArray(normalizedRaster8, 1)));
        assertEquals(Arrays.toString(expected8), Arrays.toString(MapAlgebra.bandAsArray(normalizedRaster8, 2)));
        assertEquals(Arrays.toString(expected9), Arrays.toString(MapAlgebra.bandAsArray(normalizedRaster9, 1)));
        assertEquals(Arrays.toString(expected10), Arrays.toString(MapAlgebra.bandAsArray(normalizedRaster9, 2)));
    }

    @Test
    public void testNormalizeAll2() throws FactoryException {
        String[] pixelTypes = {"B", "I", "S", "US", "F", "D"}; // Byte, Integer, Short, Unsigned Short, Float, Double
        for (String pixelType : pixelTypes) {
            testNormalizeAll2(10, 10, pixelType);
        }
    }

    private void testNormalizeAll2(int width, int height, String pixelType) throws FactoryException {
        // Create an empty raster with the specified pixel type
        GridCoverage2D raster = RasterConstructors.makeEmptyRaster(1, pixelType, width, height, 10, 20, 1);

        // Fill raster
        double[] bandValues = new double[width * height];
        for (int i = 0; i < bandValues.length; i++) {
            bandValues[i] = i;
        }
        raster = MapAlgebra.addBandFromArray(raster, bandValues, 1);

        GridCoverage2D normalizedRaster = MapAlgebra.normalizeAll(raster, 0, 255);

        // Check the normalized values and data type
        double[] normalizedBandValues = MapAlgebra.bandAsArray(normalizedRaster, 1);
        for (int i = 0; i < bandValues.length; i++) {
            double expected = (bandValues[i] - 0) * (254 - 0) / (99 - 0);
            double actual = normalizedBandValues[i];
            switch (normalizedRaster.getRenderedImage().getSampleModel().getDataType()) {
                case DataBuffer.TYPE_BYTE:
                case DataBuffer.TYPE_SHORT:
                case DataBuffer.TYPE_USHORT:
                case DataBuffer.TYPE_INT:
                    assertEquals((int) expected, (int) actual);
                    break;
                default:
                    assertEquals(expected, actual, 0.01);
            }
        }

        // Assert the data type remains as expected
        int resultDataType = normalizedRaster.getRenderedImage().getSampleModel().getDataType();
        int expectedDataType = RasterUtils.getDataTypeCode(pixelType);
        assertEquals(expectedDataType, resultDataType);
    }


    @Test
    public void testNormalizedDifference() {
        double[] band1 = new double[] {960, 1067, 107, 20, 1868};
        double[] band2 = new double[] {1967, 951, 622, 223, 152};
        double[] actual = MapAlgebra.normalizedDifference(band1, band2);
        double[] expected = new double[] {0.34, -0.06, 0.71, 0.84, -0.85};
        assertArrayEquals(expected, actual, 0.001d);
    }

    @Test
    public void testMean() {
        double[] band = new double[] {200.0, 400.0, 600.0, 200.0};
        double actual = MapAlgebra.mean(band);
        double expected = 350.0;
        assertEquals(expected, actual, 0.1d);

        band = new double[] {200.0, 400.0, 600.0, 700.0};
        actual = MapAlgebra.mean(band);
        expected = 475.0;
        assertEquals(expected, actual, 0.1d);

        band = new double[] {0.43, 0.36, 0.73, 0.56};
        actual = MapAlgebra.mean(band);
        expected = 0.52;
        assertEquals(expected, actual, 0.001d);
    }

    @Test
    public void testMode() {
        double[] band = new double[] {200.0, 400.0, 600.0, 200.0};
        double[] actual = MapAlgebra.mode(band);
        double[] expected = new double[] {200d};
        assertArrayEquals(expected, actual, 0.1d);

        band = new double[] {200.0, 400.0, 600.0, 700.0};
        actual = MapAlgebra.mode(band);
        expected = new double[] {200.0, 400.0, 600.0, 700.0};
        assertArrayEquals(expected, actual, 0.1d);
    }

    @Test
    public void testFetchRegion() {
        double[] band = new double[] {100.0, 260.0, 189.0, 106.0, 230.0, 169.0, 196.0, 200.0, 460.0};
        int[] coordinates = new int[] {0, 0, 1, 2};
        int[] dimension = new int[] {3, 3};
        double[] actual = MapAlgebra.fetchRegion(band, coordinates, dimension);
        double[] expected = new double[] {100.0, 260.0, 189.0, 106.0, 230.0, 169.0};
        assertArrayEquals(expected, actual, 0.1d);
    }

    @Test
    public void testGreaterThan() {
        double[] band = new double[] {0.42, 0.36, 0.18, 0.20, 0.21, 0.2001, 0.19};
        double target = 0.2;
        double[] actual = MapAlgebra.greaterThan(band, target);
        double[] expected = new double[] {1.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0};
        assertArrayEquals(expected, actual, 0.1d);

        band = new double[] {0.14, 0.13, 0.10, 0.86, 0.01};
        actual = MapAlgebra.greaterThan(band, target);
        expected = new double[] {0.0, 0.0, 0.0, 1.0, 0.0};
        assertArrayEquals(expected, actual, 0.1d);
    }

    @Test
    public void testGreaterThanEqual() {
        double[] band = new double[] {0.42, 0.36, 0.18, 0.20, 0.21, 0.2001, 0.19};
        double target = 0.2;
        double[] actual = MapAlgebra.greaterThanEqual(band, target);
        double[] expected = new double[] {1.0, 1.0, 0.0, 1.0, 1.0, 1.0, 0.0};
        assertArrayEquals(expected, actual, 0.1d);

        band = new double[] {0.14, 0.13, 0.10, 0.86, 0.01};
        actual = MapAlgebra.greaterThanEqual(band, target);
        expected = new double[] {0.0, 0.0, 0.0, 1.0, 0.0};
        assertArrayEquals(expected, actual, 0.1d);
    }

    @Test
    public void testLessThan() {
        double[] band = new double[] {0.42, 0.36, 0.18, 0.20, 0.21, 0.2001, 0.19};
        double target = 0.2;
        double[] actual = MapAlgebra.lessThan(band, target);
        double[] expected = new double[] {0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 1.0};
        assertArrayEquals(expected, actual, 0.1d);

        band = new double[] {0.14, 0.13, 0.10, 0.86, 0.01};
        actual = MapAlgebra.lessThan(band, target);
        expected = new double[] {1.0, 1.0, 1.0, 0.0, 1.0};
        assertArrayEquals(expected, actual, 0.1d);
    }

    @Test
    public void testLessThanEqual() {
        double[] band = new double[] {0.42, 0.36, 0.18, 0.20, 0.21, 0.2001, 0.19};
        double target = 0.2;
        double[] actual = MapAlgebra.lessThanEqual(band, target);
        double[] expected = new double[] {0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 1.0};
        assertArrayEquals(expected, actual, 0.1d);

        band = new double[] {0.14, 0.13, 0.10, 0.86, 0.01};
        actual = MapAlgebra.lessThanEqual(band, target);
        expected = new double[] {1.0, 1.0, 1.0, 0.0, 1.0};
        assertArrayEquals(expected, actual, 0.1d);
    }

    @Test
    public void testCountValue() {
        double[] band = new double[] {200.0, 400.0, 600.0, 200.0, 600.0, 600.0, 800.0};
        double target = 600d;
        int actual = MapAlgebra.countValue(band, target);
        int expected = 3;
        assertEquals(expected, actual);
    }

    @Test
    public void testMapAlgebra2Rasters() throws FactoryException {
        Random random = new Random();
        String[] pixelTypes = {null, "b", "i", "s", "us", "f", "d"};
        for (String pixelType : pixelTypes) {
            int width = random.nextInt(100) + 10;
            int height = random.nextInt(100) + 10;
            testMapAlgebra2Rasters(width, height, pixelType, null);
            testMapAlgebra2Rasters(width, height, pixelType, 100.0);
            testMapAlgebra2RastersMultiBand(width, height, pixelType, null);
            testMapAlgebra2RastersMultiBand(width, height, pixelType, 100.0);
        }
    }

    private void testMapAlgebra2RastersMultiBand(int width, int height, String pixelType, Double noDataValue) throws FactoryException {
        GridCoverage2D rast0 = RasterConstructors.makeEmptyRaster(2, "b", width, height, 10, 20, 1);
        GridCoverage2D rast1 = RasterConstructors.makeEmptyRaster(2, "b", width, height, 10, 20, 1);
        double[] band1 = new double[width * height];
        double[] band2 = new double[width * height];
        double[] band3 = new double[width * height];
        double[] band4 = new double[width * height];
        for (int i = 0; i < band1.length; i++) {
            band1[i] = Math.random() * 10;
            band2[i] = Math.random() * 10;
            band3[i] = Math.random() * 10;
            band4[i] = Math.random() * 10;
        }
        rast0 = MapAlgebra.addBandFromArray(rast0, band1, 1);
        rast0 = MapAlgebra.addBandFromArray(rast0, band2, 2);
        rast1 = MapAlgebra.addBandFromArray(rast1, band3, 1);
        rast1 = MapAlgebra.addBandFromArray(rast1, band4, 2);
        GridCoverage2D result = MapAlgebra.mapAlgebra(rast0, rast1, pixelType, "out = (rast0[0] + rast0[1] + rast1[0] + rast1[1]) * 0.4;", noDataValue);
        double actualNoDataValue = RasterUtils.getNoDataValue(result.getSampleDimension(0));
        if (noDataValue != null) {
            Assert.assertEquals(noDataValue, actualNoDataValue, 1e-9);
        } else {
            Assert.assertTrue(Double.isNaN(actualNoDataValue));
        }

        int resultDataType = result.getRenderedImage().getSampleModel().getDataType();
        int expectedDataType;
        if (pixelType != null) {
            expectedDataType = RasterUtils.getDataTypeCode(pixelType);
        } else {
            expectedDataType = rast0.getRenderedImage().getSampleModel().getDataType();
        }
        Assert.assertEquals(expectedDataType, resultDataType);

        Assert.assertEquals(rast0.getGridGeometry().getGridToCRS2D(), result.getGridGeometry().getGridToCRS2D());
        band1 = MapAlgebra.bandAsArray(rast0, 1);
        band2 = MapAlgebra.bandAsArray(rast0, 2);
        band3 = MapAlgebra.bandAsArray(rast1, 1);
        band4 = MapAlgebra.bandAsArray(rast1, 2);
        double[] bandResult = MapAlgebra.bandAsArray(result, 1);
        Assert.assertEquals(band1.length, bandResult.length);
        for (int i = 0; i < band1.length; i++) {
            double expected = (band1[i] + band2[i] + band3[i] + band4[i]) * 0.4;
            double actual = bandResult[i];
            switch (resultDataType) {
                case DataBuffer.TYPE_BYTE:
                case DataBuffer.TYPE_SHORT:
                case DataBuffer.TYPE_USHORT:
                case DataBuffer.TYPE_INT:
                    Assert.assertEquals((int) expected, (int) actual);
                    break;
                default:
                    Assert.assertEquals(expected, actual, FP_TOLERANCE);
            }
        }
    }

    private void testMapAlgebra2Rasters(int width, int height, String pixelType, Double noDataValue) throws FactoryException {
        GridCoverage2D rast0 = RasterConstructors.makeEmptyRaster(1, "b", width, height, 10, 20, 1);
        GridCoverage2D rast1 = RasterConstructors.makeEmptyRaster(1, "b", width, height, 10, 20, 1);
        double[] band1 = new double[width * height];
        double[] band2 = new double[width * height];
        for (int i = 0; i < band1.length; i++) {
            band1[i] = Math.random() * 10;
            band2[i] = Math.random() * 10;
        }
        rast0 = MapAlgebra.addBandFromArray(rast0, band1, 1);
        rast1 = MapAlgebra.addBandFromArray(rast1, band2, 1);
        GridCoverage2D result = MapAlgebra.mapAlgebra(rast0, rast1, pixelType, "out = (rast0[0] + rast1[0]) * 0.4;", noDataValue);
        double actualNoDataValue = RasterUtils.getNoDataValue(result.getSampleDimension(0));
        if (noDataValue != null) {
            Assert.assertEquals(noDataValue, actualNoDataValue, 1e-9);
        } else {
            Assert.assertTrue(Double.isNaN(actualNoDataValue));
        }

        int resultDataType = result.getRenderedImage().getSampleModel().getDataType();
        int expectedDataType;
        if (pixelType != null) {
            expectedDataType = RasterUtils.getDataTypeCode(pixelType);
        } else {
            expectedDataType = rast0.getRenderedImage().getSampleModel().getDataType();
        }
        Assert.assertEquals(expectedDataType, resultDataType);

        Assert.assertEquals(rast0.getGridGeometry().getGridToCRS2D(), result.getGridGeometry().getGridToCRS2D());
        band1 = MapAlgebra.bandAsArray(rast0, 1);
        band2 = MapAlgebra.bandAsArray(rast1, 1);
        double[] bandResult = MapAlgebra.bandAsArray(result, 1);
        Assert.assertEquals(band1.length, bandResult.length);
        for (int i = 0; i < band1.length; i++) {
            double expected = (band1[i] + band2[i]) * 0.4;
            double actual = bandResult[i];
            switch (resultDataType) {
                case DataBuffer.TYPE_BYTE:
                case DataBuffer.TYPE_SHORT:
                case DataBuffer.TYPE_USHORT:
                case DataBuffer.TYPE_INT:
                    Assert.assertEquals((int) expected, (int) actual);
                    break;
                default:
                    Assert.assertEquals(expected, actual, FP_TOLERANCE);
            }
        }
    }

    @Test
    public void testMapAlgebra() throws FactoryException {
        Random random = new Random();
        String[] pixelTypes = {null, "b", "i", "s", "us", "f", "d"};
        for (String pixelType : pixelTypes) {
            int width = random.nextInt(100) + 10;
            int height = random.nextInt(100) + 10;
            testMapAlgebra(width, height, pixelType, null);
            testMapAlgebra(width, height, pixelType, 100.0);
        }
    }

    private void testMapAlgebra(int width, int height, String pixelType, Double noDataValue) throws FactoryException {
        GridCoverage2D raster = RasterConstructors.makeEmptyRaster(2, "b", width, height, 10, 20, 1);
        double[] band1 = new double[width * height];
        double[] band2 = new double[width * height];
        for (int i = 0; i < band1.length; i++) {
            band1[i] = Math.random() * 10;
            band2[i] = Math.random() * 10;
        }
        raster = MapAlgebra.addBandFromArray(raster, band1, 1);
        raster = MapAlgebra.addBandFromArray(raster, band2, 2);
        GridCoverage2D result = MapAlgebra.mapAlgebra(raster, pixelType, "out = (rast[0] + rast[1]) * 0.4;", noDataValue);
        double actualNoDataValue = RasterUtils.getNoDataValue(result.getSampleDimension(0));
        if (noDataValue != null) {
            Assert.assertEquals(noDataValue, actualNoDataValue, 1e-9);
        } else {
            Assert.assertTrue(Double.isNaN(actualNoDataValue));
        }

        int resultDataType = result.getRenderedImage().getSampleModel().getDataType();
        int expectedDataType;
        if (pixelType != null) {
            expectedDataType = RasterUtils.getDataTypeCode(pixelType);
        } else {
            expectedDataType = raster.getRenderedImage().getSampleModel().getDataType();
        }
        Assert.assertEquals(expectedDataType, resultDataType);

        Assert.assertEquals(raster.getGridGeometry().getGridToCRS2D(), result.getGridGeometry().getGridToCRS2D());
        band1 = MapAlgebra.bandAsArray(raster, 1);
        band2 = MapAlgebra.bandAsArray(raster, 2);
        double[] bandResult = MapAlgebra.bandAsArray(result, 1);
        Assert.assertEquals(band1.length, bandResult.length);
        for (int i = 0; i < band1.length; i++) {
            double expected = (band1[i] + band2[i]) * 0.4;
            double actual = bandResult[i];
            switch (resultDataType) {
                case DataBuffer.TYPE_BYTE:
                case DataBuffer.TYPE_SHORT:
                case DataBuffer.TYPE_USHORT:
                case DataBuffer.TYPE_INT:
                    Assert.assertEquals((int) expected, (int) actual);
                    break;
                default:
                    Assert.assertEquals(expected, actual, 1e-3);
            }
        }
    }
}
