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

from os import path

from shapely.geometry import Point

from sedona.utils.spatial_rdd_parser import GeoData

tests_path = path.abspath(path.join(__file__ ,"../../../core/src/test/"))
tests_resource = path.abspath(path.join(__file__ ,"../../../core/src/test/resources/"))
serializer_absolute_path = path.abspath(path.join(__file__, "../"))

serializer_path = path.join(serializer_absolute_path, "serializer_type.txt")


def distance_sorting_functions(geo_data: GeoData, query_point: Point):
    return geo_data.geom.distance(query_point)
