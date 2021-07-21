/*
 * Copyright 2018 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.arrow.memory.ArrowBuf;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.*;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID;
import org.apache.spark.sql.types.*;

import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

import org.apache.arrow.vector.types.pojo.Field;

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
    /**
     *  BigQuery does not support Map type but this function needs to be overridden since this
     *  class extends an abstract class
     */
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


  public ArrowSchemaConverter(ValueVector vector, StructField userProvidedField) {

    super(fromArrowField(vector.getField()));

    if (vector instanceof BitVector) {
      accessor = new ArrowSchemaConverter.BooleanAccessor((BitVector) vector);
    } else if (vector instanceof BigIntVector) {
      accessor = new ArrowSchemaConverter.LongAccessor((BigIntVector) vector);
    } else if (vector instanceof Float8Vector) {
      accessor = new ArrowSchemaConverter.DoubleAccessor((Float8Vector) vector);
    } else if (vector instanceof DecimalVector) {
      accessor = new ArrowSchemaConverter.DecimalAccessor((DecimalVector) vector);
    } else if (vector instanceof Decimal256Vector) {
      accessor = new ArrowSchemaConverter.Decimal256Accessor((Decimal256Vector) vector);
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
      accessor = new ArrowSchemaConverter.ArrayAccessor(listVector, userProvidedField);
    } else if (vector instanceof StructVector) {
      StructVector structVector = (StructVector) vector;
      accessor = new ArrowSchemaConverter.StructAccessor(structVector);

      if(userProvidedField != null) {
        List<StructField> structList =
            Arrays
                .stream(((StructType)userProvidedField.dataType()).fields())
                .collect(Collectors.toList());

        childColumns = new ArrowSchemaConverter[structList.size()];

        Map<String, ValueVector> valueVectorMap =
            structVector
                .getChildrenFromFields()
                .stream()
                .collect(Collectors.toMap(ValueVector::getName, valueVector -> valueVector));

        for (int i = 0; i < childColumns.length; ++i) {
          StructField structField = structList.get(i);
          childColumns[i] =
              new ArrowSchemaConverter(valueVectorMap.get(structField.name()), structField);
        }

      } else {
        childColumns = new ArrowSchemaConverter[structVector.size()];
        for (int i = 0; i < childColumns.length; ++i) {
          childColumns[i] = new ArrowSchemaConverter(structVector.getVectorById(i), null);
        }
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
    byte getByte(int rowId) {
      return (byte)getLong(rowId);
    }

    @Override
    short getShort(int rowId) {
      return (short)getLong(rowId);
    }

    @Override
    int getInt(int rowId) {
      return (int)getLong(rowId);
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

  private static class Decimal256Accessor extends ArrowSchemaConverter.ArrowVectorAccessor {

    private final Decimal256Vector accessor;

    Decimal256Accessor(Decimal256Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    UTF8String getUTF8String(int rowId){
      if (isNullAt(rowId)) {
        return null;
      }

      BigDecimal bigDecimal = accessor.getObject(rowId);
      return UTF8String.fromString(bigDecimal.toPlainString());
    }
  }

  private static class StringAccessor extends ArrowSchemaConverter.ArrowVectorAccessor {

    private final VarCharVector accessor;

    StringAccessor(VarCharVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      if (this.accessor.isSet(rowId) == 0) {
        return null;
      } else {
        ArrowBuf offsets = accessor.getOffsetBuffer();
        int index = rowId * VarCharVector.OFFSET_WIDTH;
        int start = offsets.getInt(index);
        int end = offsets.getInt(index + VarCharVector.OFFSET_WIDTH);

        /* Since the result is accessed lazily if the memory address is corrupted we
         * might lose the data. Might be better to include a byte array. Not doing so
         * for performance reasons.
         */
        return UTF8String.fromAddress(/* base = */null,
            accessor.getDataBuffer().memoryAddress() + start,
            end - start);
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

    /**
     * Interpreting Data here as int to keep it consistent with Avro.
     */
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
    private static final int ONE_THOUSAND = 1_000;
    private static final int ONE_MILLION = 1_000_000;
    private static final int ONE_BILLION = 1_000_000_000;

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
      long seconds = epoch / ONE_MILLION;
      int nanoOfSeconds = (int)(epoch % ONE_MILLION) * ONE_THOUSAND;

      // The method LocalDateTime.ofEpochSecond expects
      // seconds as the number of seconds from the epoch of 1970-01-01T00:00:00Z and
      // the nanosecond within the second, from 0 to 999,999,999.
      // For time prior to 1970-01-01, seconds and nanoseconds both could be negative, but
      // negative nanoseconds is not allowed, hence for such cases adding 1_000_000_000 to
      // nanosecond's value and subtracting 1 from the second's value.
      if(nanoOfSeconds < 0) {
        seconds--;
        nanoOfSeconds += ONE_BILLION;
      }

      LocalDateTime dateTime = LocalDateTime.ofEpochSecond(seconds, nanoOfSeconds, ZoneOffset.UTC);
      return UTF8String.fromString(dateTime.toString());
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

    ArrayAccessor(ListVector vector, StructField userProvidedField) {
      super(vector);
      this.accessor = vector;
      StructField structField = null;

      // this is to support Array of StructType/StructVector
      if(userProvidedField != null) {
        ArrayType arrayType = ((ArrayType)userProvidedField.dataType());
        structField =
            new StructField(
                vector.getDataVector().getName(),
                arrayType.elementType(),
                arrayType.containsNull(),
                Metadata.empty());// safe to pass empty metadata as it is not used anywhere
      }

      this.arrayData = new ArrowSchemaConverter(vector.getDataVector(), structField);
    }

    @Override
    final boolean isNullAt(int rowId) {
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
