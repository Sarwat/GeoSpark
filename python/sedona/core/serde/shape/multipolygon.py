import attr
from shapely.geometry import MultiPolygon

from sedona.core.serde.binary.buffer import BinaryBuffer
from sedona.core.serde.binary.order import ByteOrderType
from sedona.core.serde.binary.parser import BinaryParser
from sedona.core.serde.shape.enums import GeomEnum
from sedona.core.serde.shape.util import get_number_of_polygon_points, get_number_of_rings, add_shape_geometry_metadata, \
    add_offsets_to_polygon, reverse_linear_ring, put_coordinates
from sedona.utils.abstract_parser import GeometryParser


@attr.s
class MultiPolygonParser(GeometryParser):
    name = "MultiPolygon"

    @classmethod
    def serialize(cls, obj: MultiPolygon, binary_buffer: BinaryBuffer):
        if isinstance(obj, MultiPolygon):
            num_polygons = len(obj.geoms)
            num_points = sum([get_number_of_polygon_points(polygon) for polygon in obj.geoms])
            num_rings = sum([get_number_of_rings(polygon) for polygon in obj.geoms])
            binary_buffer.put_int(0, ByteOrderType.LITTLE_ENDIAN)
            add_shape_geometry_metadata(GeomEnum.polygon.value, binary_buffer)
            binary_buffer.add_empty_bytes("double", 4, ByteOrderType.LITTLE_ENDIAN)
            binary_buffer.put_int(num_rings, ByteOrderType.LITTLE_ENDIAN)
            binary_buffer.put_int(num_points, ByteOrderType.LITTLE_ENDIAN)

            offset = 0
            for geom in obj.geoms:
                offset = add_offsets_to_polygon(geom, binary_buffer, offset)

            for geom in obj.geoms:
                coordinates_exterior = reverse_linear_ring(geom.exterior, False)
                put_coordinates(coordinates_exterior, binary_buffer)

                for ring in geom.interiors:
                    coordinates = reverse_linear_ring(ring)
                    put_coordinates(coordinates, binary_buffer)

            binary_buffer.put_byte(1)
            binary_buffer.put_byte(3)
            binary_buffer.put_byte(1)
            binary_buffer.put_byte(-127)
        else:
            raise TypeError(f"Need a {cls.name} instance")
        return binary_buffer.byte_array

    @classmethod
    def deserialize(cls, parser: BinaryParser) -> MultiPolygon:
        raise NotImplementedError("For multipolygon, PolygonParser class is used.")
