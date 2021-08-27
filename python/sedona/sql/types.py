#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from pyspark.sql.types import UserDefinedType, ArrayType, ByteType

from sedona.core.serde.binary.buffer import BinaryBuffer
from sedona.core.serde.binary.parser import BinaryParser
from sedona.core.serde.geom_factory import SerializationFactory, geometry_serializers_instances
from sedona.core.serde.spark_config import spark_conf_getter


class GeometryType(UserDefinedType):

    @classmethod
    def sqlType(cls):
        return ArrayType(ByteType(), containsNull=False)

    def fromInternal(self, obj):
        return self.deserialize(obj)

    def toInternal(self, obj):
        return [el - 256 if el >= 128 else el for el in self.serialize(obj)]

    def serialize(self, obj):
        binary_buffer = BinaryBuffer()
        binary_buffer.put_byte(0)
        return self.factory.serialize(obj, binary_buffer)

    def deserialize(self, datum):
        parser = BinaryParser(list(datum))
        _ = parser.read_byte()
        return self.factory.deserialize(parser)

    @classmethod
    def module(cls):
        return "sedona.sql.types"

    def needConversion(self):
        return True

    @classmethod
    def scalaUDT(cls):
        return "org.apache.spark.sql.sedona_sql.UDT.GeometryUDT"

    @property
    def factory(self) -> SerializationFactory:
        if not hasattr(self, "__factory"):
            setattr(self, "__factory", geometry_serializers_instances[spark_conf_getter.serialization])
        return getattr(self, "__factory")
