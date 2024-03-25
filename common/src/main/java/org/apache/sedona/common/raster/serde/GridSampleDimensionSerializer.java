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
package org.apache.sedona.common.raster.serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.sedona.common.utils.RasterUtils;
import org.geotools.coverage.Category;
import org.geotools.coverage.GridSampleDimension;

import java.util.List;

/**
 * GridSampleDimension and RenderedSampleDimension are not serializable. We need to provide a custom serializer
 */
public class GridSampleDimensionSerializer extends Serializer<GridSampleDimension> {
    @Override
    public void write(Kryo kryo, Output output, GridSampleDimension sampleDimension) {
        String description = sampleDimension.getDescription().toString();
        List<Category> categories = sampleDimension.getCategories();
        double offset = sampleDimension.getOffset();
        double scale = sampleDimension.getScale();
        double noDataValue = RasterUtils.getNoDataValue(sampleDimension);
        KryoUtil.writeUTF8String(output, description);
        output.writeDouble(offset);
        output.writeDouble(scale);
        output.writeDouble(noDataValue); // for interoperability with Python RasterType.
        KryoUtil.writeObjectWithLength(kryo, output, categories.toArray());
    }

    @Override
    public GridSampleDimension read(Kryo kryo, Input input, Class aClass) {
        String description = KryoUtil.readUTF8String(input);
        double offset = input.readDouble();
        double scale = input.readDouble();
        input.readDouble();  // noDataValue is included in categories, so we just skip it
        input.readInt(); // skip the length of the next object
        Category[] categories = kryo.readObject(input, Category[].class);
        return new GridSampleDimension(description, categories, offset, scale);
    }
}
