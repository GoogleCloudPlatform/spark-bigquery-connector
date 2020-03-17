package com.google.cloud.spark.bigquery.converters;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.*;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.spark.sql.types.*;

import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

import org.apache.arrow.vector.types.pojo.Field;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;

/**
 * ArrowSchemaConverter class for accessing values and converting
 * arrow data types to the types supported by big query.
 */
public class ArrowSchemaConverter extends ColumnVector {

  private final ArrowSchemaConverter.ArrowVectorAccessor accessor;
  private ArrowSchemaConverter[] childColumns;

  @Override
  public boolean hasNull() {
    return accessor.getNullCount() > 0;
  }

  @Override
  public int numNulls() {
    return accessor.getNullCount();
  }

  @Override
  public void close() {
    if (childColumns != null) {
      for (int i = 0; i < childColumns.length; i++) {
        childColumns[i].close();
        childColumns[i] = null;
      }
      childColumns = null;
    }
    accessor.close();
  }

  @Override
  public boolean isNullAt(int rowId) {
    return accessor.isNullAt(rowId);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return accessor.getBoolean(rowId);
  }

  @Override
  public byte getByte(int rowId) {
    return accessor.getByte(rowId);
  }

  @Override
  public short getShort(int rowId) {
    return accessor.getShort(rowId);
  }

  @Override
  public int getInt(int rowId) {
    return accessor.getInt(rowId);
  }

  @Override
  public long getLong(int rowId) {
    return accessor.getLong(rowId);
  }

  @Override
  public float getFloat(int rowId) {
    return accessor.getFloat(rowId);
  }

  @Override
  public double getDouble(int rowId) {
    return accessor.getDouble(rowId);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    if (isNullAt(rowId)) return null;
    return accessor.getDecimal(rowId, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    if (isNullAt(rowId)) return null;
    return accessor.getUTF8String(rowId);
  }

  @Override
  public byte[] getBinary(int rowId) {
    if (isNullAt(rowId)) return null;
    return accessor.getBinary(rowId);
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    if (isNullAt(rowId)) return null;
    return accessor.getArray(rowId);
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ArrowSchemaConverter getChild(int ordinal) { return childColumns[ordinal]; }

  private static DataType fromArrowType(ArrowType arrowType)
  {
    switch (arrowType.getTypeID())
    {
      case Int: return DataTypes.LongType;
      case Bool: return DataTypes.BooleanType;
      case FloatingPoint: return DataTypes.DoubleType;
      case Binary: return DataTypes.BinaryType;
      case Utf8: return DataTypes.StringType;
      case Date: return DataTypes.DateType;
      case Time:
      case Timestamp: return DataTypes.TimestampType;
      case Decimal: return DataTypes.createDecimalType();
    }

    throw new UnsupportedOperationException("Unsupported data type " + arrowType.toString());
  }

  private static DataType fromArrowField(Field field)
  {
    if (field.getType().getTypeID() == ArrowTypeID.List)
    {
      Field elementField = field.getChildren().get(0);
      DataType elementType = fromArrowField(elementField);

      return new ArrayType(elementType, elementField.isNullable());
    }

    if (field.getType().getTypeID() == ArrowTypeID.Struct)
    {
      java.util.List<Field> fieldChildren = field.getChildren();
      StructField[] structFields = new StructField[fieldChildren.size()];

      int ind = 0;

      for (Field childField : field.getChildren())
      {
        DataType childType = fromArrowField(childField);
        structFields[ind++] = new StructField(childField.getName(), childType, childField.isNullable(), Metadata.empty());
      }

      return new StructType(structFields);
    }

    return fromArrowType(field.getType());
  }


  public ArrowSchemaConverter(ValueVector vector) {

    super(fromArrowField(vector.getField()));

    if (vector instanceof BitVector) {
      accessor = new ArrowSchemaConverter.BooleanAccessor((BitVector) vector);
    } else if (vector instanceof BigIntVector) {
      accessor = new ArrowSchemaConverter.LongAccessor((BigIntVector) vector);
    } else if (vector instanceof Float8Vector) {
      accessor = new ArrowSchemaConverter.DoubleAccessor((Float8Vector) vector);
    } else if (vector instanceof DecimalVector) {
      accessor = new ArrowSchemaConverter.DecimalAccessor((DecimalVector) vector);
    } else if (vector instanceof VarCharVector) {
      accessor = new ArrowSchemaConverter.StringAccessor((VarCharVector) vector);
    } else if (vector instanceof VarBinaryVector) {
      accessor = new ArrowSchemaConverter.BinaryAccessor((VarBinaryVector) vector);
    } else if (vector instanceof DateDayVector) {
      accessor = new ArrowSchemaConverter.DateAccessor((DateDayVector) vector);
    } else if (vector instanceof TimeMicroVector) {
      accessor = new ArrowSchemaConverter.TimeMicroVectorAccessor((TimeMicroVector) vector);
    } else if (vector instanceof TimeStampMicroVector) {
      accessor = new ArrowSchemaConverter.TimestampMicroVectorAccessor((TimeStampMicroVector) vector);
    } else if (vector instanceof TimeStampMicroTZVector) {
      accessor = new ArrowSchemaConverter.TimestampMicroTZVectorAccessor((TimeStampMicroTZVector) vector);
    } else if (vector instanceof ListVector) {
      ListVector listVector = (ListVector) vector;
      accessor = new ArrowSchemaConverter.ArrayAccessor(listVector);
    } else if (vector instanceof StructVector) {
      StructVector structVector = (StructVector) vector;
      accessor = new ArrowSchemaConverter.StructAccessor(structVector);

      childColumns = new ArrowSchemaConverter[structVector.size()];
      for (int i = 0; i < childColumns.length; ++i) {
        childColumns[i] = new ArrowSchemaConverter(structVector.getVectorById(i));
      }
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private abstract static class ArrowVectorAccessor {

    private final ValueVector vector;

    ArrowVectorAccessor(ValueVector vector) {
      this.vector = vector;
    }

    // TODO: should be final after removing ArrayAccessor workaround
    boolean isNullAt(int rowId) {
      return vector.isNull(rowId);
    }

    final int getNullCount() {
      return vector.getNullCount();
    }

    final void close() {
      vector.close();
    }

    boolean getBoolean(int rowId) {
      throw new UnsupportedOperationException();
    }

    byte getByte(int rowId) {
      throw new UnsupportedOperationException();
    }

    short getShort(int rowId) {
      throw new UnsupportedOperationException();
    }

    int getInt(int rowId) {
      throw new UnsupportedOperationException();
    }

    long getLong(int rowId) {
      throw new UnsupportedOperationException();
    }

    float getFloat(int rowId) {
      throw new UnsupportedOperationException();
    }

    double getDouble(int rowId) {
      throw new UnsupportedOperationException();
    }

    Decimal getDecimal(int rowId, int precision, int scale) {
      throw new UnsupportedOperationException();
    }

    UTF8String getUTF8String(int rowId) {
      throw new UnsupportedOperationException();
    }

    byte[] getBinary(int rowId) {
      throw new UnsupportedOperationException();
    }

    ColumnarArray getArray(int rowId) {
      throw new UnsupportedOperationException();
    }
  }

  private static class BooleanAccessor extends ArrowSchemaConverter.ArrowVectorAccessor {

    private final BitVector accessor;

    BooleanAccessor(BitVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final boolean getBoolean(int rowId) {
      return accessor.get(rowId) == 1;
    }
  }

  private static class LongAccessor extends ArrowSchemaConverter.ArrowVectorAccessor {

    private final BigIntVector accessor;

    LongAccessor(BigIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class DoubleAccessor extends ArrowSchemaConverter.ArrowVectorAccessor {

    private final Float8Vector accessor;

    DoubleAccessor(Float8Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final double getDouble(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class DecimalAccessor extends ArrowSchemaConverter.ArrowVectorAccessor {

    private final DecimalVector accessor;

    DecimalAccessor(DecimalVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final Decimal getDecimal(int rowId, int precision, int scale) {
      if (isNullAt(rowId)) return null;
      return Decimal.apply(accessor.getObject(rowId), precision, scale);
    }
  }

  private static class StringAccessor extends ArrowSchemaConverter.ArrowVectorAccessor {

    private final VarCharVector accessor;
    private final NullableVarCharHolder stringResult = new NullableVarCharHolder();

    StringAccessor(VarCharVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      accessor.get(rowId, stringResult);
      if (stringResult.isSet == 0) {
        return null;
      } else {
        return UTF8String.fromAddress(null,
            stringResult.buffer.memoryAddress() + stringResult.start,
            stringResult.end - stringResult.start);
      }
    }
  }

  private static class BinaryAccessor extends ArrowSchemaConverter.ArrowVectorAccessor {

    private final VarBinaryVector accessor;

    BinaryAccessor(VarBinaryVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final byte[] getBinary(int rowId) {
      return accessor.getObject(rowId);
    }
  }

  private static class DateAccessor extends ArrowSchemaConverter.ArrowVectorAccessor {

    private final DateDayVector accessor;

    DateAccessor(DateDayVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class TimeMicroVectorAccessor extends ArrowSchemaConverter.ArrowVectorAccessor {

    private final TimeMicroVector accessor;

    TimeMicroVectorAccessor(TimeMicroVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }
  }


  private static class TimestampMicroVectorAccessor extends ArrowSchemaConverter.ArrowVectorAccessor {

    private final TimeStampMicroVector accessor;

    TimestampMicroVectorAccessor(TimeStampMicroVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      long epoch = accessor.get(rowId);

      LocalDateTime dateTime = new LocalDateTime(java.util.concurrent.TimeUnit.MICROSECONDS.toMillis(epoch), DateTimeZone.UTC);

      return UTF8String.fromString(dateTime.toString() + epoch % 1000);
    }
  }

  private static class TimestampMicroTZVectorAccessor extends ArrowSchemaConverter.ArrowVectorAccessor {

    private final TimeStampMicroTZVector accessor;

    TimestampMicroTZVectorAccessor(TimeStampMicroTZVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class ArrayAccessor extends ArrowSchemaConverter.ArrowVectorAccessor {

    private final ListVector accessor;
    private final ArrowSchemaConverter arrayData;

    ArrayAccessor(ListVector vector) {
      super(vector);
      this.accessor = vector;
      this.arrayData = new ArrowSchemaConverter(vector.getDataVector());
    }

    @Override
    final boolean isNullAt(int rowId) {
      // TODO: Workaround if vector has all non-null values, see ARROW-1948
      if (accessor.getValueCount() > 0 && accessor.getValidityBuffer().capacity() == 0) {
        return false;
      } else {
        return super.isNullAt(rowId);
      }
    }

    @Override
    final ColumnarArray getArray(int rowId) {
      ArrowBuf offsets = accessor.getOffsetBuffer();
      int index = rowId * ListVector.OFFSET_WIDTH;
      int start = offsets.getInt(index);
      int end = offsets.getInt(index + ListVector.OFFSET_WIDTH);
      return new ColumnarArray(arrayData, start, end - start);
    }
  }

  /**
   * Any call to "get" method will throw UnsupportedOperationException.
   *
   * Access struct values in a ArrowColumnVector doesn't use this accessor. Instead, it uses
   * getStruct() method defined in the parent class. Any call to "get" method in this class is a
   * bug in the code.
   *
   */
  private static class StructAccessor extends ArrowSchemaConverter.ArrowVectorAccessor {

    StructAccessor(StructVector vector) {
      super(vector);
    }
  }
}