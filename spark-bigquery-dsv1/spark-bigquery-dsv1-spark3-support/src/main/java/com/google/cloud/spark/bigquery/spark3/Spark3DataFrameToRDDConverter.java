package com.google.cloud.spark.bigquery.spark3;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder.Deserializer;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Spark3DataFrameToRDDConverter implements DataFrameToRDDConverter {

  @Override
  public RDD<Row> convertToRDD(Dataset<Row> data) {
    StructType schema = data.schema();

    List<Attribute> attributes =
        JavaConversions.asJavaCollection(schema.toAttributes()).stream()
            .map(Attribute::toAttribute)
            .collect(Collectors.toList());

    final ExpressionEncoder<Row> expressionEncoder =
        RowEncoder.apply(schema)
            .resolveAndBind(
                JavaConverters.asScalaIteratorConverter(attributes.iterator()).asScala().toSeq(),
                SimpleAnalyzer$.MODULE$);

    final Deserializer<Row> deserializer = expressionEncoder.createDeserializer();
    ClassTag<Row> classTag = ClassTag$.MODULE$.apply(Row.class);

    RDD<Row> rowRdd =
        data.queryExecution()
            .toRdd()
            .mapPartitions(getIteratorMapper(deserializer), false, classTag);

    return rowRdd;
  }

  private AbstractFunction1<Iterator<InternalRow>, Iterator<Row>> getIteratorMapper(
      final Deserializer<Row> deserializer) {

    final AbstractFunction1<InternalRow, Row> internalRowMapper =
        new SerializableAbstractFunction1(
            (Function<InternalRow, Row> & Serializable)
                internalRow -> deserializer.apply(internalRow));

    final AbstractFunction1<Iterator<InternalRow>, Iterator<Row>> iteratorMapper =
        new SerializableAbstractFunction1(
            (Function<Iterator<InternalRow>, Iterator<Row>> & Serializable)
                iterator -> iterator.map(internalRowMapper));

    return iteratorMapper;
  }
}
