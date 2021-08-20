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

import struct
from enum import Enum
from typing import List, Union

import attr
from pyspark import SparkContext
from shapely.wkb import loads

DOUBLE_SIZE = 8
INT_SIZE = 4
BYTE_SIZE = 1
CHAR_SIZE = 1
BOOLEAN_SIZE = 1

size_dict = {
    "d": DOUBLE_SIZE,
    "i": INT_SIZE,
    "b": BYTE_SIZE,
    "s": CHAR_SIZE,
    "?": BOOLEAN_SIZE
}

order_type = {
    "little_endian": "<",
    "big_endian": ">"
}


class ByteOrderType(Enum):
    LITTLE_ENDIAN = "<"
    BIG_ENDIAN = ">"






