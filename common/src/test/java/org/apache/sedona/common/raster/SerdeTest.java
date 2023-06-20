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
package org.apache.sedona.common.raster;

import org.geotools.coverage.grid.GridCoverage2D;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SerdeTest extends RasterTestBase {

    @Test
    public void testRoundtripSerdeSingelbandRaster() throws IOException, ClassNotFoundException {
        byte[] bytes = Serde.serialize(oneBandRaster);
        GridCoverage2D raster = Serde.deserialize(bytes);
        assertNotNull(raster);
        assertEquals(1, raster.getNumSampleDimensions());
    }

    @Test
    public void testRoundtripSerdeMultibandRaster() throws IOException, ClassNotFoundException {
        byte[] bytes = Serde.serialize(this.multiBandRaster);
        GridCoverage2D raster = Serde.deserialize(bytes);
        assertNotNull(raster);
        assertEquals(4, raster.getNumSampleDimensions());
    }
}