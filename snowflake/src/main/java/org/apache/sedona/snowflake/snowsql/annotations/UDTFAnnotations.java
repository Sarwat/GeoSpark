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
package org.apache.sedona.snowflake.snowsql.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

public class UDTFAnnotations {
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public static @interface CallOnNull {
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public static @interface Immutable {
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public static @interface Volatile {
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public static @interface TabularFunc {
        String name();

        String[] argNames();
        // Arg types are optional. If not specified, Reflection is used to get the type name
        // This is required for Snowflake Geometry type which is serialized as a Java String type when UDF
        // is called from Snowflake.
        // If specified, the length of argTypes must match the length of argNames
        // If specified, the type name must be one of the keys in Constants.snowflakeTypeMap
        String[] argTypes() default {};
        // Return types are optional. If not specified, Reflection is used to get the type name
        // This is required for Snowflake Geometry type which is serialized as a Java String type when UDF
        String returnTypes() default "";
    }
}
