package org.apache.sedona.core.formatMapper.parquet;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericRecord;
import org.apache.sedona.core.enums.GeometryType;
import org.apache.sedona.core.exceptions.SedonaException;
import org.apache.sedona.core.geometryObjects.Circle;
import org.apache.sedona.core.geometryObjects.schema.CircleSchema;
import org.apache.sedona.core.geometryObjects.schema.CoordinateArraySchema;
import org.apache.sedona.core.geometryObjects.schema.CoordinateSchema;
import org.apache.sedona.core.geometryObjects.schema.PolygonSchema;
import org.apache.sedona.core.io.avro.SchemaUtils;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.apache.sedona.core.io.avro.schema.*;
import org.apache.sedona.core.io.avro.utils.AvroUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.util.Assert;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.StreamSupport;

public class ParquetFormatMapperTest extends BaseSchemaTest {
    
    
    private static GenericRecord arrayRecord;
    private static GenericRecord geometryCollectionRecord;
    private static GenericRecord lineStringRecord;
    private static GenericRecord pointRecord;
    private static GenericRecord multiPointRecord;
    private static GenericRecord multiLineStringRecord;
    private static GenericRecord multiPolygonRecordWithHoles;
    private static GenericRecord rectArrayRecord;
    private static GenericRecord circleRecord;
    private static GenericRecord polygonRecordWithHoles;
    private static GenericRecord polygonRecordWithoutHoles;
    
    private static Map<String,Object> getRecordWithUserData(String geometryColumnName,Object geometryData, GeometryType geometryType){
        return ImmutableMap.of(geometryColumnName,ImmutableMap.of(AvroConstants.GEOMETRY_OBJECT,geometryData,
                                                                  AvroConstants.GEOMETRY_SHAPE,geometryType.getName()),
                               "i",1,"j","Test Dummy");
    }
    @BeforeClass
    public static void init() throws SedonaException {
        List<Object> arrMap = Lists.newArrayList(
                ImmutableMap.of(CoordinateSchema.X_COORDINATE,0, CoordinateSchema.Y_COORDINATE, 0),
                ImmutableMap.of(CoordinateSchema.X_COORDINATE,10, CoordinateSchema.Y_COORDINATE, 0),
                ImmutableMap.of(CoordinateSchema.X_COORDINATE,10, CoordinateSchema.Y_COORDINATE, 10),
                ImmutableMap.of(CoordinateSchema.X_COORDINATE,0, CoordinateSchema.Y_COORDINATE, 10),
                ImmutableMap.of(CoordinateSchema.X_COORDINATE,0, CoordinateSchema.Y_COORDINATE, 0)
                                                             );
        Schema arrSchema = new RecordSchema(TEST_NAMESPACE, "array",
                                            Lists.newArrayList(new Field("arr", CoordinateArraySchema.getSchema()),
                                                               new Field("i",new SimpleSchema(AvroConstants.PrimitiveDataType.INT)),
                                                               new Field("j",new SimpleSchema(AvroConstants.PrimitiveDataType.STRING))));
        arrayRecord = AvroUtils.getRecord(SchemaUtils.SchemaParser.getSchema(arrSchema.getDataType().toString()), ImmutableMap.of("arr",arrMap,"i",1,"j","Test Dummy"));
        rectArrayRecord = AvroUtils.getRecord(SchemaUtils.SchemaParser.getSchema(arrSchema.getDataType().toString()), ImmutableMap.of("arr", Lists.newArrayList(
                ImmutableMap.of(CoordinateSchema.X_COORDINATE,10, CoordinateSchema.Y_COORDINATE, 20),
                ImmutableMap.of(CoordinateSchema.X_COORDINATE,20, CoordinateSchema.Y_COORDINATE, 40)), "i",1,"j","Test Dummy"));

        org.apache.avro.Schema sedonaGeometrySchema = AvroUtils.getSchema("gc",Lists.newArrayList(
                new Field("i",new SimpleSchema(AvroConstants.PrimitiveDataType.INT)),
                new Field("j",new SimpleSchema(AvroConstants.PrimitiveDataType.STRING))),TEST_NAMESPACE,"DummyGeometry");
        
        circleRecord = AvroUtils.getRecord(sedonaGeometrySchema,
                                           getRecordWithUserData("gc",
                                                                 ImmutableMap.of(
                                                                         CircleSchema.CENTER,
                                                                         ImmutableMap.of(CoordinateSchema.X_COORDINATE,
                                                                                         20,
                                                                                         CoordinateSchema.Y_COORDINATE,
                                                                                         40),
                                                                         CircleSchema.RADIUS,10),
                                                                 GeometryType.CIRCLE
                                                                 ));
        List<Object> exRing =  Lists.newArrayList(
                ImmutableMap.of(CoordinateSchema.X_COORDINATE,0, CoordinateSchema.Y_COORDINATE, 0),
                ImmutableMap.of(CoordinateSchema.X_COORDINATE,10, CoordinateSchema.Y_COORDINATE, 0),
                ImmutableMap.of(CoordinateSchema.X_COORDINATE,10, CoordinateSchema.Y_COORDINATE, 10),
                ImmutableMap.of(CoordinateSchema.X_COORDINATE,0, CoordinateSchema.Y_COORDINATE, 10),
                ImmutableMap.of(CoordinateSchema.X_COORDINATE,0, CoordinateSchema.Y_COORDINATE, 0)
                                                 );
        List<Object> holes = Lists.newArrayList(Lists.newArrayList(ImmutableMap.of(CoordinateSchema.X_COORDINATE,0, CoordinateSchema.Y_COORDINATE, 0),
                                                                   ImmutableMap.of(CoordinateSchema.X_COORDINATE,0, CoordinateSchema.Y_COORDINATE, 5),
                                                                   ImmutableMap.of(CoordinateSchema.X_COORDINATE,5, CoordinateSchema.Y_COORDINATE, 5),
                                                                   ImmutableMap.of(CoordinateSchema.X_COORDINATE,0, CoordinateSchema.Y_COORDINATE, 0)),
                                                Lists.newArrayList(ImmutableMap.of(CoordinateSchema.X_COORDINATE,0, CoordinateSchema.Y_COORDINATE, 0),
                                                                   ImmutableMap.of(CoordinateSchema.X_COORDINATE,5, CoordinateSchema.Y_COORDINATE, 0),
                                                                   ImmutableMap.of(CoordinateSchema.X_COORDINATE,5, CoordinateSchema.Y_COORDINATE, 5),
                                                                   ImmutableMap.of(CoordinateSchema.X_COORDINATE,0, CoordinateSchema.Y_COORDINATE, 0)
                                                                   ));
        
        polygonRecordWithoutHoles = AvroUtils.getRecord(
                sedonaGeometrySchema,
                getRecordWithUserData("gc",ImmutableMap.of(PolygonSchema.EXTERIOR_RING, exRing),
                                      GeometryType.POLYGON));
        polygonRecordWithHoles = AvroUtils.getRecord(
                sedonaGeometrySchema,
                getRecordWithUserData("gc",ImmutableMap.of(PolygonSchema.EXTERIOR_RING, exRing,PolygonSchema.HOLES,holes),
                                GeometryType.POLYGON));
        multiPolygonRecordWithHoles = AvroUtils.getRecord(
                sedonaGeometrySchema,
                getRecordWithUserData("gc",Lists.newArrayList(ImmutableMap.of(PolygonSchema.EXTERIOR_RING, exRing,PolygonSchema.HOLES,holes)),
                                      GeometryType.MULTIPOLYGON));
        pointRecord = AvroUtils.getRecord(sedonaGeometrySchema,
                                          getRecordWithUserData("gc",
                                                                ImmutableMap.of(CoordinateSchema.X_COORDINATE,5,
                                                                                CoordinateSchema.Y_COORDINATE, 5),
                                                                GeometryType.POINT)
                                         );
        multiPointRecord = AvroUtils.getRecord(
                sedonaGeometrySchema,
                getRecordWithUserData("gc",Lists.newArrayList(ImmutableMap.of(CoordinateSchema.X_COORDINATE,5,
                                                                              CoordinateSchema.Y_COORDINATE, 5)),
                                      GeometryType.MULTIPOINT));
        lineStringRecord = AvroUtils.getRecord(sedonaGeometrySchema,
                                               getRecordWithUserData("gc",
                                                                     arrMap,
                                                                     GeometryType.LINESTRING)
                                              );
        multiLineStringRecord = AvroUtils.getRecord(sedonaGeometrySchema,
                                                    getRecordWithUserData("gc",
                                                                          Arrays.asList(arrMap),
                                                                          GeometryType.MULTILINESTRING)
                                                   );
        geometryCollectionRecord = AvroUtils.getRecord(sedonaGeometrySchema,
                                                       getRecordWithUserData("gc",
                                                                             Lists.newArrayList(ImmutableMap.of(AvroConstants.GEOMETRY_OBJECT,
                                                                                                                      ImmutableMap.of(CoordinateSchema.X_COORDINATE,5,
                                                                                                                                      CoordinateSchema.Y_COORDINATE, 5),
                                                                                                                AvroConstants.GEOMETRY_SHAPE,
                                                                                                                GeometryType.POINT.getName()),
                                                                                                ImmutableMap.of(AvroConstants.GEOMETRY_OBJECT,
                                                                                                                ImmutableMap.of(PolygonSchema.EXTERIOR_RING, exRing,
                                                                                                                                PolygonSchema.HOLES,holes),
                                                                                                                AvroConstants.GEOMETRY_SHAPE,
                                                                                                                GeometryType.POLYGON.getName())
                                                                                                ),
                                                                             GeometryType.GEOMETRYCOLLECTION));
        
        
        
    }
    
    
    
    @Test
    public void testPointParquetFormatterFromArray() throws Exception {
    
        Iterable<Point> pointIterable = () -> {
            try {
                return new ParquetFormatMapper<Point>("arr",Lists.newArrayList("i","j"),
                                                      GeometryType.POINT)
                        .call(Lists.newArrayList(arrayRecord).iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        Point p = StreamSupport.stream(pointIterable.spliterator(), false).findFirst().get();
        Assert.equals(p, new GeometryFactory().createPoint(new Coordinate(0,0)));
        Assert.equals(((Map<String,Double>)p.getUserData()).get("i"), 1);
        Assert.equals(((Map<String,Double>)p.getUserData()).get("j"), "Test Dummy");
    }
    
    @Test
    public void testLineStringParquetFormatterFromArray() throws Exception {
        Iterable<LineString> lineStringIterable = () -> {
            try {
                return new ParquetFormatMapper<LineString>("arr", Lists.newArrayList("i","j"),
                                                           GeometryType.LINESTRING).call(Lists.newArrayList(
                        arrayRecord).iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        LineString l = StreamSupport.stream(lineStringIterable.spliterator(), false).findFirst().get();
        Assert.equals(l, new GeometryFactory().createLineString(new Coordinate[]{
                new Coordinate(0,0),
                new Coordinate(10,0),
                new Coordinate(10,10),
                new Coordinate(0,10),
                new Coordinate(0,0)}));
        Assert.equals(((Map<String,Double>)l.getUserData()).get("i"), 1);
        Assert.equals(((Map<String,Double>)l.getUserData()).get("j"), "Test Dummy");
    }
    
    @Test
    public void testPolygonParquetFormatterFromArray() throws Exception {
    
        Iterable<Polygon> iterable = () -> {
            try {
                return new ParquetFormatMapper<Polygon>("arr",Lists.newArrayList("i","j"),GeometryType.POLYGON).call(Lists.newArrayList(
                        arrayRecord).iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        Polygon geometry = StreamSupport.stream(iterable.spliterator(), false).findFirst().get();
        Assert.equals(geometry, new GeometryFactory().createPolygon(new Coordinate[]{
                new Coordinate(0,0),
                new Coordinate(10,0),
                new Coordinate(10,10),
                new Coordinate(0,10),
                new Coordinate(0,0)}));
        Assert.equals(((Map<String,Double>)geometry.getUserData()).get("i"), 1);
        Assert.equals(((Map<String,Double>)geometry.getUserData()).get("j"), "Test Dummy");
    }
    
    @Test
    public void testRectangleParquetFormatterFromArray() throws Exception {
    
        Iterable<Polygon> iterable = () -> {
            try {
                return new ParquetFormatMapper<Polygon>("arr",Lists.newArrayList("i","j"),
                                                        GeometryType.RECTANGLE)
                        .call(Lists.newArrayList(rectArrayRecord).iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        Polygon geometry = StreamSupport.stream(iterable.spliterator(), false).findFirst().get();
        Assert.equals(geometry, new GeometryFactory().createPolygon(new Coordinate[]{
                new Coordinate(10,20),
                new Coordinate(10,40),
                new Coordinate(20,40),
                new Coordinate(20,20),
                new Coordinate(10,20)}));
        Assert.equals(((Map<String,Double>)geometry.getUserData()).get("i"), 1);
        Assert.equals(((Map<String,Double>)geometry.getUserData()).get("j"), "Test Dummy");
        Assert.isTrue(geometry.isRectangle());
    }
    
    @Test
    public void testCircleFromGenericRecord(){
        Iterable<Circle> iterable = () -> {
            try {
                return new ParquetFormatMapper<Circle>("gc",Lists.newArrayList("i","j"))
                        .call(Lists.newArrayList(circleRecord).iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        Circle geometry = StreamSupport.stream(iterable.spliterator(), false).findFirst().get();
        Assert.equals(geometry, new Circle(new GeometryFactory().createPoint(new Coordinate(20,40)),10.0));
        Assert.equals(((Map<String,Double>)geometry.getUserData()).get("i"), 1);
        Assert.equals(((Map<String,Double>)geometry.getUserData()).get("j"), "Test Dummy");
    }
    
    @Test
    public void testPolygonWithoutHolesFromGenericRecord(){
        Iterable<Polygon> iterable = () -> {
            try {
                return new ParquetFormatMapper<Polygon>("gc",Lists.newArrayList("i","j"))
                        .call(Lists.newArrayList(polygonRecordWithoutHoles).iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        Polygon geometry = StreamSupport.stream(iterable.spliterator(), false).findFirst().get();
        Assert.equals(geometry, new GeometryFactory().createPolygon(new Coordinate[]{
                new Coordinate(0,0),
                new Coordinate(10,0),
                new Coordinate(10,10),
                new Coordinate(0,10),
                new Coordinate(0,0)}));
        Assert.equals(((Map<String,Double>)geometry.getUserData()).get("i"), 1);
        Assert.equals(((Map<String,Double>)geometry.getUserData()).get("j"), "Test Dummy");
    }
    
    @Test
    public void testPolygonWithHolesFromGenericRecord(){
        Iterable<Polygon> iterable = () -> {
            try {
                return new ParquetFormatMapper<Polygon>("gc",Lists.newArrayList("i","j"))
                        .call(Lists.newArrayList(polygonRecordWithHoles).iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        GeometryFactory factory = new GeometryFactory();
        Polygon geometry = StreamSupport.stream(iterable.spliterator(), false).findFirst().get();
        Assert.equals(geometry, factory.createPolygon(factory.createLinearRing(new Coordinate[]{
                new Coordinate(0,0),
                new Coordinate(10,0),
                new Coordinate(10,10),
                new Coordinate(0,10),
                new Coordinate(0,0)}),new LinearRing[]{
                        factory.createLinearRing(new Coordinate[]{
                                new Coordinate(0,0),
                                new Coordinate(0,5),
                                new Coordinate(5,5),
                                new Coordinate(0,0)}),
                factory.createLinearRing(new Coordinate[]{
                        new Coordinate(0,0),
                        new Coordinate(5,0),
                        new Coordinate(5,5),
                        new Coordinate(0,0)
        })}));
        Assert.equals(((Map<String,Double>)geometry.getUserData()).get("i"), 1);
        Assert.equals(((Map<String,Double>)geometry.getUserData()).get("j"), "Test Dummy");
    }
    
    @Test
    public void testMultiPolygonFromGenericRecord(){
        Iterable<MultiPolygon> iterable = () -> {
            try {
                return new ParquetFormatMapper<MultiPolygon>("gc",Lists.newArrayList("i","j"))
                        .call(Lists.newArrayList(multiPolygonRecordWithHoles).iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        GeometryFactory factory = new GeometryFactory();
        MultiPolygon geometry = StreamSupport.stream(iterable.spliterator(), false).findFirst().get();
        Assert.equals(geometry, factory.createMultiPolygon(new Polygon[]{
                factory.createPolygon(factory.createLinearRing(new Coordinate[]{
                        new Coordinate(0,0),
                        new Coordinate(10,0),
                        new Coordinate(10,10),
                        new Coordinate(0,10),
                        new Coordinate(0,0)}),new LinearRing[]{
                                factory.createLinearRing(new Coordinate[]{
                                        new Coordinate(0,0),
                                        new Coordinate(0,5),
                                        new Coordinate(5,5),
                                        new Coordinate(0,0)}),
                        factory.createLinearRing(new Coordinate[]{
                                new Coordinate(0,0),
                                new Coordinate(5,0),
                                new Coordinate(5,5),
                                new Coordinate(0,0)
                        })})}));
        Assert.equals(((Map<String,Double>)geometry.getUserData()).get("i"), 1);
        Assert.equals(((Map<String,Double>)geometry.getUserData()).get("j"), "Test Dummy");
    }
    
    @Test
    public void testPointFromGenericRecord(){
        Iterable<Point> pointIterable = () -> {
            try {
                return new ParquetFormatMapper<Point>("gc",Lists.newArrayList("i","j"),
                                                      null)
                        .call(Lists.newArrayList(pointRecord).iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        Point p = StreamSupport.stream(pointIterable.spliterator(), false).findFirst().get();
        Assert.equals(p, new GeometryFactory().createPoint(new Coordinate(5,5)));
        Assert.equals(((Map<String,Double>)p.getUserData()).get("i"), 1);
        Assert.equals(((Map<String,Double>)p.getUserData()).get("j"), "Test Dummy");
    }
    
    @Test
    public void testMultiPointFromGenericRecord(){
        GeometryFactory factory = new GeometryFactory();
        Iterable<MultiPoint> pointIterable = () -> {
            try {
                return new ParquetFormatMapper<MultiPoint>("gc",Lists.newArrayList("i","j"),
                                                      null)
                        .call(Lists.newArrayList(multiPointRecord).iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        MultiPoint p = StreamSupport.stream(pointIterable.spliterator(), false).findFirst().get();
        Assert.equals(p, factory.createMultiPoint(new Point[]{factory.createPoint(new Coordinate(5,5))}));
        Assert.equals(((Map<String,Double>)p.getUserData()).get("i"), 1);
        Assert.equals(((Map<String,Double>)p.getUserData()).get("j"), "Test Dummy");
    }
    
    @Test
    public void testLineStringFromGenericRecord(){
        Iterable<LineString> lineStringIterable = () -> {
            try {
                return new ParquetFormatMapper<LineString>("gc", Lists.newArrayList("i","j"))
                        .call(Lists.newArrayList(lineStringRecord).iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        LineString l = StreamSupport.stream(lineStringIterable.spliterator(), false).findFirst().get();
        Assert.equals(l, new GeometryFactory().createLineString(new Coordinate[]{
                new Coordinate(0,0),
                new Coordinate(10,0),
                new Coordinate(10,10),
                new Coordinate(0,10),
                new Coordinate(0,0)}));
        Assert.equals(((Map<String,Double>)l.getUserData()).get("i"), 1);
        Assert.equals(((Map<String,Double>)l.getUserData()).get("j"), "Test Dummy");
    }
    
    @Test
    public void testMultiLineStringFromGenericRecord(){
        Iterable<MultiLineString> lineStringIterable = () -> {
            try {
                return new ParquetFormatMapper<MultiLineString>("gc", Lists.newArrayList("i","j"))
                        .call(Lists.newArrayList(multiLineStringRecord).iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        GeometryFactory factory = new GeometryFactory();
        MultiLineString l = StreamSupport.stream(lineStringIterable.spliterator(), false).findFirst().get();
        Assert.equals(l,
                      factory.createMultiLineString(new LineString[]{factory.createLineString(new Coordinate[]{new Coordinate(0, 0), new Coordinate(10,
                                                                                                                    0), new Coordinate(
                              10,
                              10), new Coordinate(0, 10), new Coordinate(0, 0)})}));
        Assert.equals(((Map<String,Double>)l.getUserData()).get("i"), 1);
        Assert.equals(((Map<String,Double>)l.getUserData()).get("j"), "Test Dummy");
    }
    
    @Test
    public void testGeometryCollection(){
        Iterable<GeometryCollection> lineStringIterable = () -> {
            try {
                return new ParquetFormatMapper<GeometryCollection>("gc", Lists.newArrayList("i","j"))
                        .call(Lists.newArrayList(geometryCollectionRecord).iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        GeometryFactory factory = new GeometryFactory();
        GeometryCollection l = StreamSupport.stream(lineStringIterable.spliterator(), false).findFirst().get();
        Assert.equals(l,
                      factory.createGeometryCollection(new Geometry[]{
                              factory.createPoint(new Coordinate(5,5)),
                              factory.createPolygon(factory.createLinearRing(new Coordinate[]{
                                      new Coordinate(0,0),
                                      new Coordinate(10,0),
                                      new Coordinate(10,10),
                                      new Coordinate(0,10),
                                      new Coordinate(0,0)}),new LinearRing[]{
                                      factory.createLinearRing(new Coordinate[]{
                                              new Coordinate(0,0),
                                              new Coordinate(0,5),
                                              new Coordinate(5,5),
                                              new Coordinate(0,0)}),
                                      factory.createLinearRing(new Coordinate[]{
                                              new Coordinate(0,0),
                                              new Coordinate(5,0),
                                              new Coordinate(5,5),
                                              new Coordinate(0,0)
                                      })})
                              
                      }));
        Assert.equals(((Map<String,Double>)l.getUserData()).get("i"), 1);
        Assert.equals(((Map<String,Double>)l.getUserData()).get("j"), "Test Dummy");
    }
}
